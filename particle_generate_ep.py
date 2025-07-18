#!/usr/bin/env python3
import multiprocessing
import os
import dotenv
import trio
import trio_asyncio
from asyncio.log import logger
from web3 import AsyncWeb3, WebSocketProvider
multiprocessing.freeze_support()
dotenv.load_dotenv()

"""
Example of using an asynchronous web3 (AsyncWeb3) instance with trio, specifically with 
https://particle.network , my personal favorite web3 rpc provider.
TODO: Implement an Asyncio.Queue so that we can keep the `trio_asyncio.open_loop()` open, 
executing coroutines as instructions are received. This would allow us to keep the connection 
open in between requests, which would greatly speed up execution time.
"""

class ParticleConfigEndpoint:
    """
    This works, pass the generate endpoint to your Web3
    set dotenv: PROJECT_ID PROJECT_CLIENT_KEY
    (get api key)[https://dashboard.particle.network]
    """
    def __init__(self, chain_id: int = 1):
        self.project_id = os.getenv('PROJECT_ID')
        self.project_client_key = os.getenv('PROJECT_CLIENT_KEY')
        self.chain_id = chain_id
        self._endpoint = f"wss://rpc.particle.network/evm-chain"
        self.endpoint_uri = self._endpoint + f'?chainId={chain_id}&projectUuid={self.project_id}&projectKey={self.project_client_key}'
        self.request_timeout = 60

@property
def endpoint() -> str:
"""
Configure 
"""
    assert self.chain_id
    assert project id
    assert project_client
    return self.endpoint_uri

class ParticleWebsocketContainer:
    """
    NOTE: This is a P.O.C. and I would appreciate some help making this function like an AyncWeb3 object
    """
     def __init__(self, cid: int):
          self.ep = ParticleConfigEndpoint(cid).endpoint_uri

     async def ws_v2_alternate_init_example_1(self):
          # compatability hack because AsyncWeb3 needs an event_loop and trio does not provide one
          async with trio_asyncio.open_loop():
               w3: AsyncWeb3 = await trio_asyncio.aio_as_trio(AsyncWeb3(WebSocketProvider(self.ep)))
               _cid = (await trio_asyncio.aio_as_trio(w3.eth.chain_id))
               logger.info('Connected to %s ' % _cid)
               ret = await trio_asyncio.aio_as_trio(w3.eth.get_block)('latest', True)
               # we cannot use context managers, so we need to manually disconnect the websocket when we are done.
               await trio_asyncio.aio_as_trio(w3.provider.disconnect())
          return ret









async def main():
     pw3 = ParticleWebsocketContainer(1)
     return await pw3.ws_v2_alternate_init_example_1()

if __name__ == '__main__':
     print(trio.run(main))
