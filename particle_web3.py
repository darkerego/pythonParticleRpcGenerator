#!/usr/bin/env python3
"""Trio-compatible Particle Network provider with query and Basic authentication.

Preserves the original project interfaces:
    CustomParticleProvider(chain_id=1, use_websocket=False)
    CustomParticleWsProvider(chain_id=1)
    find_cid(name)
    create_w3(cid=None, chain_name=None, use_ws=False)
    async_is_poa_chain(w3)
    test_needs_middleware_and_inject(w3)

HTTP connections use httpx rather than aiohttp so they work in native Trio.
"""

from __future__ import annotations

import itertools
import json
import logging
import os
import random
from typing import Any, Sequence
from urllib.parse import urlencode

import dotenv
import httpx
import requests
import trio
from urllib3.util.ssl_ import create_urllib3_context
from web3 import AsyncWeb3
from web3.exceptions import ExtraDataLengthError
from web3.middleware import ExtraDataToPOAMiddleware
from web3.providers import AsyncBaseProvider
from web3.types import RPCEndpoint


PARTICLE_SUPPORTED = [
    (1, "ethereum"),
    (43114, "avalanche"),
    (56, "bsc"),
    (137, "polygon"),
    (10, "optimism"),
    (42161, "arbitrum"),
    (42170, "nova"),
    (8453, "base"),
    (534352, "scroll"),
    (324, "zksync"),
    (1101, "polygonzkevm"),
    (1284, "moonbeam"),
    (1285, "moonriver"),
    (1313161554, "aurora"),
    (1030, "confluxespace"),
    (22776, "mapprotocol"),
    (728126428, "tron"),
    (42220, "celo"),
    (25, "cronos"),
    (250, "fantom"),
    (100, "gnosis"),
    (1666600000, "harmony"),
    (128, "heco"),
    (321, "kcc"),
    (8217, "klaytn"),
    (1088, "metis"),
    (42262, "oasisemerald"),
    (66, "okc"),
    (321, "platon"),
    (108, "thundercore"),
]

PARTICLE_HTTP_URL = "https://rpc.particle.network/evm-chain"
PARTICLE_WS_URL = "wss://rpc.particle.network/evm-chain"
RETRYABLE_HTTP_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})

logger = logging.getLogger(__name__)
dotenv.load_dotenv()


class ParticleRPCError(RuntimeError):
    """A request completed with an invalid JSON-RPC response or exhausted retries."""


class ParticleRPCPermanentError(ParticleRPCError):
    """A non-retryable RPC transport failure (e.g., unauthorized)."""


class ParticleRPCRetryableError(ParticleRPCError):
    """A temporary HTTP failure with an optional Retry-After hint."""

    def __init__(self, message: str, retry_after: str | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def _validate_rpc_response(data: Any, expected_id: int) -> dict[str, Any]:
    """Validate the transport envelope, retaining valid JSON-RPC error responses."""
    if not isinstance(data, dict):
        raise ParticleRPCError(
            f"Particle returned {type(data).__name__}, expected a JSON-RPC object"
        )
    if data.get("jsonrpc") != "2.0":
        raise ParticleRPCError(
            f"Particle returned invalid jsonrpc version: {data.get('jsonrpc')!r}"
        )
    if data.get("id") != expected_id:
        raise ParticleRPCError(
            f"Particle response ID mismatch: expected {expected_id!r}, "
            f"received {data.get('id')!r}"
        )
    if "result" not in data and "error" not in data:
        raise ParticleRPCError("Particle JSON-RPC response contains neither result nor error")
    return data


def _short_body(response: httpx.Response, limit: int = 250) -> str:
    return response.text[:limit].replace("\n", " ").replace("\r", " ")


class CustomParticleProvider(AsyncBaseProvider):
    """Particle's HTTP or Trio-WebSocket JSON-RPC transport for AsyncWeb3.

    The HTTP implementation is connection-pooled, concurrency-safe within a Trio
    process, and compatible with the AsyncBaseProvider API in web3.py 7.x.

    The optional WebSocket transport opens a connection for each RPC request to
    keep ownership of trio-websocket's context manager correct. For fast block
    scanning, use the HTTP transport, which reuses connections.
    """

    is_async = True
    has_persistent_connection = False

    def __init__(
        self,
        chain_id: int = 1,
        use_websocket: bool = False,
        *,
        project_id: str | None = None,
        project_server_key: str | None = None,
        timeout: float = 20.0,
        max_attempts: int = 4,
        retry_base: float = 0.5,
        max_connections: int = 32,
        auth_mode: str | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__()
        self.chain_id = int(chain_id)
        self.use_websocket = bool(use_websocket)
        self.auth_mode = (auth_mode or os.getenv("PARTICLE_AUTH_MODE", "query")).strip().lower()
        if self.auth_mode not in ("query", "basic"):
            raise ValueError("PARTICLE_AUTH_MODE must be query or basic")
        self.project_id = project_id or os.getenv("PROJECT_ID")
        self.project_server_key = project_server_key or os.getenv("PROJECT_SERVER_KEY")
        if not self.project_id or not self.project_server_key:
            raise ValueError(
                "Missing Particle credentials: set PROJECT_ID and PROJECT_SERVER_KEY "
                "in the process environment or .env"
            )
        if self.chain_id <= 0:
            raise ValueError(f"Invalid chain ID: {self.chain_id}")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if max_connections < 1:
            raise ValueError("max_connections must be >= 1")
        self.max_attempts = int(max_attempts)
        self.retry_base = max(0.0, float(retry_base))
        self.request_timeout = float(timeout)
        self._ids = itertools.count(1)
        self.headers = {"Content-Type": "application/json"}
        self.auth = (self.project_id, self.project_server_key)
        self._prep_session = requests.Session() if not self.use_websocket else None
        # urllib3 generates the same locally observed JA3-style ClientHello as
        # Requests. This does not guarantee identical Cloudflare classification.
        self._ssl_context = create_urllib3_context() if not self.use_websocket else None
        if self._ssl_context is not None:
            self._ssl_context.load_verify_locations(cafile=requests.certs.where())
        # Match the working web3.HTTPProvider URL exactly in query mode.
        # WebSocket authentication also uses the Particle URL query parameters.
        endpoint = PARTICLE_WS_URL if self.use_websocket else PARTICLE_HTTP_URL
        if self.use_websocket or self.auth_mode == "query":
            self.base_url = endpoint + "?" + urlencode({
                "chainId": self.chain_id,
                "projectUuid": self.project_id,
                "projectKey": self.project_server_key,
            })
        else:
            self.base_url = endpoint
        self.endpoint_uri = self.base_url
        self._owns_client = client is None
        self.client: httpx.AsyncClient | None = None
        if not self.use_websocket:
            self.client = client or httpx.AsyncClient(
                auth=(
                    httpx.BasicAuth(self.project_id, self.project_server_key)
                    if self.auth_mode == "basic"
                    else None
                ),
                verify=self._ssl_context,
                http2=False,
                trust_env=True,
                timeout=httpx.Timeout(self.request_timeout),
                limits=httpx.Limits(
                    max_connections=max_connections,
                    max_keepalive_connections=min(16, max_connections),
                    keepalive_expiry=30.0,
                ),
            )

    def _redact(self, message: str) -> str:
        """Prevent secrets embedded in query URLs from reaching logs/errors."""
        from urllib.parse import quote
        result = str(message)
        for secret in (self.project_server_key, self.project_id):
            if secret:
                result = result.replace(secret, "[REDACTED]")
                result = result.replace(quote(secret, safe=""), "[REDACTED]")
        return result

    async def __ainit__(self) -> None:
        """Backward-compatible hook; connection creation is already complete."""
        return None

    async def _ws_once(self, payload: dict[str, Any]) -> dict[str, Any]:
        from trio_websocket import open_websocket_url

        async with open_websocket_url(self.base_url) as websocket:
            await websocket.send_message(json.dumps(payload))
            message = await websocket.get_message()
        if isinstance(message, bytes):
            message = message.decode("utf-8")
        try:
            return json.loads(message)
        except (ValueError, UnicodeDecodeError) as exc:
            raise ParticleRPCError("Particle WebSocket returned invalid JSON") from exc

    async def _http_once(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.client is None:
            raise RuntimeError("The Particle HTTP client has already been closed")

        # Mirror Requests' JSON spacing, Content-Length, User-Agent, header
        # ordering, and other request-level defaults. No Requests HTTP is made.
        if self.auth_mode == "query":
            prepared = self._prep_session.prepare_request(
                requests.Request("POST", self.base_url, json=payload)
            )
            response = await self.client.post(
                self.base_url,
                content=prepared.body,
                headers=dict(prepared.headers),
            )
        else:
            response = await self.client.post(self.base_url, json=payload)
        if not response.is_success:
            # Avoid response.raise_for_status(): HTTPX exception text includes the
            # entire URL, which in query mode contains the project credentials.
            details = self._redact(_short_body(response))
            ray = response.headers.get("cf-ray")
            challenge = response.headers.get("cf-mitigated")
            context = (
                f"Particle HTTP {response.status_code} "
                f"chain={self.chain_id} method={payload['method']} "
                f"cf-ray={ray!r} cf-mitigated={challenge!r}: {details}"
            )
            if response.status_code in RETRYABLE_HTTP_STATUSES:
                raise ParticleRPCRetryableError(
                    context, response.headers.get("Retry-After")
                )
            raise ParticleRPCPermanentError(context)
        try:
            return response.json()
        except (ValueError, UnicodeDecodeError) as exc:
            raise ParticleRPCError(
                f"Particle returned HTTP {response.status_code} with non-JSON "
                f"content for {payload['method']}: "
                f"{self._redact(_short_body(response))}"
            ) from None

    def _retry_delay(self, attempt: int, retry_after: str | None = None) -> float:
        base = min(15.0, self.retry_base * (2 ** (attempt - 1)))
        if retry_after:
            try:
                base = max(base, min(30.0, float(retry_after)))
            except ValueError:
                pass
        return base + (random.uniform(0.0, 0.15) if base > 0 else 0.0)

    async def _request(self, payload: dict[str, Any]) -> dict[str, Any]:
        method = payload["method"]
        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            retry_after: str | None = None
            try:
                raw = (
                    await self._ws_once(payload)
                    if self.use_websocket
                    else await self._http_once(payload)
                )
                return _validate_rpc_response(raw, payload["id"])
            except ParticleRPCPermanentError:
                raise
            except ParticleRPCRetryableError as exc:
                last_error = exc
                retry_after = exc.retry_after
            except (httpx.TransportError, ParticleRPCError, OSError) as exc:
                # HTTPX's exception descriptions can include URLs; remove any
                # credential strings before including the error in messages.
                last_error = ParticleRPCError(
                    f"{type(exc).__name__}: {self._redact(str(exc))}"
                )
            # Trio cancellation inherits BaseException and isn't swallowed.
            if attempt < self.max_attempts:
                delay = self._retry_delay(attempt, retry_after)
                logger.warning(
                    "Particle RPC chain=%s method=%s attempt=%s/%s failed: %s; "
                    "retrying in %.2fs",
                    self.chain_id,
                    method,
                    attempt,
                    self.max_attempts,
                    last_error,
                    delay,
                )
                await trio.sleep(delay)
        raise ParticleRPCError(
            f"Particle RPC chain={self.chain_id}, method={method} failed after "
            f"{self.max_attempts} attempt(s): {last_error}"
        ) from None

    async def make_request(
        self, method: RPCEndpoint, params: Sequence[Any] | None
    ) -> dict[str, Any]:
        payload = {
            "jsonrpc": "2.0",
            "method": str(method),
            "params": list(params) if params is not None else [],
            "id": next(self._ids),
        }
        # In query mode, match Web3.HTTPProvider's standard JSON-RPC body.
        # With Basic Auth, Particle's documented API selects the chain
        # from the body. WebSocket mode retains the legacy chainId field.
        if self.use_websocket or self.auth_mode == "basic":
            payload["chainId"] = self.chain_id
        return await self._request(payload)

    async def make_batch_request(
        self, requests: list[tuple[RPCEndpoint, Sequence[Any]]]
    ) -> list[dict[str, Any]]:
        """Support Web3 batch APIs via concurrent single RPC calls.

        Particle batch-post support is not assumed. Each call retains its
        chainId, authentication, retry policy, and response validation.
        """
        replies: list[dict[str, Any] | None] = [None] * len(requests)

        async def one(index: int, method: RPCEndpoint, params: Sequence[Any]) -> None:
            replies[index] = await self.make_request(method, params)

        async with trio.open_nursery() as nursery:
            for index, (method, params) in enumerate(requests):
                nursery.start_soon(one, index, method, params)
        return [reply for reply in replies if reply is not None]

    async def is_connected(self, show_traceback: bool = False) -> bool:
        try:
            response = await self.make_request(RPCEndpoint("eth_chainId"), [])
            if "error" in response:
                raise ParticleRPCError(f"Particle eth_chainId error: {response['error']}")
            actual = int(response["result"], 16)
            if actual != self.chain_id:
                raise ParticleRPCError(
                    f"Particle connected to chain {actual}, expected {self.chain_id}"
                )
            return True
        except (Exception,) as exc:
            if show_traceback:
                raise
            logger.warning("Particle connectivity check failed: %s", exc)
            return False

    async def connect_websocket(self) -> None:
        """Compatibility check; WS requests use scoped per-request connections."""
        if not self.use_websocket:
            raise ValueError("connect_websocket requires use_websocket=True")
        await self.is_connected(show_traceback=True)

    async def close(self) -> None:
        if self.client is not None:
            if self._owns_client:
                await self.client.aclose()
            self.client = None
        if self._prep_session is not None:
            self._prep_session.close()
            self._prep_session = None

    async def disconnect(self) -> None:
        await self.close()


class CustomParticleWsProvider(CustomParticleProvider):
    def __init__(self, chain_id: int = 1, **kwargs: Any) -> None:
        super().__init__(chain_id=chain_id, use_websocket=True, **kwargs)


def find_cid(name: str) -> int:
    name = name.strip().lower()
    for chain_id, network in PARTICLE_SUPPORTED:
        if name == network:
            return chain_id
    return 0


async def async_is_poa_chain(w3: AsyncWeb3) -> bool:
    for block_id in (0, "latest"):
        try:
            block = await w3.eth.get_block(block_id, full_transactions=False)
        except ExtraDataLengthError:
            return True
        extra_data = block.get("extraData", b"")
        if isinstance(extra_data, str):
            extra_data = bytes.fromhex(extra_data.removeprefix("0x"))
        if extra_data[:4] in (b"cliq", b"ibft") or len(extra_data) >= 96:
            return True
    return False


async def test_needs_middleware_and_inject(w3: AsyncWeb3) -> AsyncWeb3:
    if await async_is_poa_chain(w3):
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    return w3


async def create_w3(
    cid: int | None = None,
    chain_name: str | None = None,
    use_ws: bool = False,
) -> AsyncWeb3:
    if cid is None:
        if not chain_name:
            raise ValueError("Provide either cid or chain_name")
        cid = find_cid(chain_name)
    if not cid:
        raise ValueError(f"Unrecognized blockchain network {chain_name!r}")
    provider = CustomParticleWsProvider(cid) if use_ws else CustomParticleProvider(cid)
    if use_ws:
        await provider.connect_websocket()
    return AsyncWeb3(provider)


async def main(cid: int) -> None:
    w3 = await create_w3(cid=cid)
    try:
        block_number = await w3.eth.block_number
        gas_price = await w3.eth.gas_price
        print(f"Chain ID: {await w3.eth.chain_id}")
        print(f"Current Block Number: {block_number}")
        print(f"Current Gas Price: {gas_price}")
    finally:
        await w3.provider.close()


if __name__ == "__main__":
    trio.run(main, 1)
