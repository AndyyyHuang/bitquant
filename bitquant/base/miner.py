import copy
import time
import asyncio
import argparse
import threading
import traceback
import json
from abc import ABC, abstractmethod
from functools import partial
from starlette.types import Send

import bittensor as bt
from typing import List, Dict, Tuple, Union, Callable, Awaitable

from bitquant.base.protocol import StreamingPortfolioHistory, PortfolioRecord, MinerEvaluationWindow
from bitquant.base.neuron import BaseNeuron
from bitquant.utils.timeutils import TimeUtils

class QuantMiner(BaseNeuron):
    def __init__(self, config=None):
        # initiate neuron
        super().__init__(config, type(self).__name__)
        self.dendrite = bt.dendrite(wallet=self.wallet)
        self.sync()
        self.serve_axon()

        # if not self.config.blacklist.force_validator_permit:
        #     bt.logging.warning(
        #         "You are allowing non-validators to send requests to your miner. This is a security risk."
        #     )
        # if self.config.blacklist.allow_non_registered:
        #     bt.logging.warning(
        #         "You are allowing non-registered entities to send requests to your miner. This is a security risk."
        #     )

        # Init sync with the network. Updates the metagraph.

        # Serve axon to enable external connections.

        # self.axon = bt.axon(wallet=self.wallet, config=self.config, port=self.config.axon.port)
        # self.axon = bt.axon(
        #     wallet=self.wallet, port=self.config.axon.port
        # )

        # bt.logging.info(f"Attaching forward function to miner axon.")
        # self.axon.attach(
        #     forward_fn=self.forward,
        #     # blacklist_fn=self.blacklist,
        #     # priority_fn=self.priority,
        # )

        self.last_sync_block = self.block - 1000

        # Instantiate miner runners
        self.should_exit: bool = False
        self.is_running: bool = False
        self.thread_ctx = {}
        # self.thread: threading.Thread = None
        self.lock = asyncio.Lock()

        # istantiate miner data
        self.portfolio: List[PortfolioRecord] = []
        '''
        self.request_timestamps: dict = {}
        thread = threading.Thread(target=get_valid_hotkeys, args=(self.config,))
        '''

    # TODO maybe this should be in BaseNeuron?
    def serve_axon(self):
        """Serve axon to enable external connections."""

        bt.logging.info("serving ip to chain...")
        try:
            self.axon = bt.axon(wallet=self.wallet, config=self.config)

            try:
                self.subtensor.serve_axon(
                    netuid=self.config.netuid,
                    axon=self.axon,
                )
                bt.logging.info(
                    f"Running validator {self.axon} on network: {self.config.subtensor.chain_endpoint} with netuid: {self.config.netuid}"
                )
            except Exception as e:
                bt.logging.error(f"Failed to serve Axon with exception: {e}")
                pass

        except Exception as e:
            bt.logging.error(f"Failed to create Axon initialize with exception: {e}")
            pass

    # ===== override BaseNeuron functions =====

    def should_set_weights(self):
        return False

    def save_state(self):
        pass

    # ===== main functions =====

    async def forward(self, synapse: PortfolioRecord):
        validator_ids = self.metagraph.axons

        resp = await self.dendrite(
            axons=[self.metagraph.axons[uid] for uid in validator_ids],
            synapse=synapse,
            deserialize=False,
            streaming=False,
            timeout=float(120),
        )
        bt.logging.debug(f"{resp=}")

        # if response is not successful, retry
        # if resp


    # def forward(self, synapse: PortfolioRecord) -> PortfolioRecord:
    #     bt.logging.debug(f"miner forwarding {synapse}")

    #     # create lazy stream function to stream new portfolio updates within start_time and end_time
    #     # TODO not sure this is right
    #     async def _stream(start_time: int, end_time: int, send: Send):
    #         bt.logging.debug(f"miner entering stream")
    #         t_now = TimeUtils.now_in_ms()
    #         assert t_now >= start_time, f"{self.block=}, {start_time=}"

    #         # initialize an empty portfolio with positions all 0's
    #         if not self.portfolio:
    #             self.portfolio.append(PortfolioRecord({}))

    #         bt.logging.debug(f"preparing portfolio")
    #         while t_now <= end_time:
    #             # select lastest portfolio and send package
    #             portfolio = self.portfolio[-1]
    #             portfolio = json.dumps(portfolio.to_dict()).encode('utf-8')
    #             bt.logging.debug(f"sending {portfolio[-1]=}")
    #             await send(
    #                 {
    #                     "type": "http.response.body",
    #                     "body": portfolio,
    #                     "more_body": True,
    #                 }
    #             )
    #         else:
    #             bt.logging.debug(f"time now exceeded {end_time=}")

    #     portfolio_streamer = partial(_stream, start_time, end_time)
    #     return synapse.create_streaming_response(portfolio_streamer)

    # # TODO update
    # async def forward(self):
    #     """
    #     Validator forward pass. Consists of:
    #     - Generating the query
    #     - Querying the miners
    #     - Getting the responses
    #     - Rewarding the miners
    #     - Updating the scores
    #     """
    #     bt.logging.debug(f"validator forwarding")
    #     try:
    #         now = TimeUtils.now_in_ms()
    #         miner_window = MinerEvaluationWindow(
    #             start_ms=now,
    #             end_ms=now + self.evaluation_window)
    #         synapse = StreamingPortfolioHistory(miner_window=miner_window)

    #         # miner_uids = get_random_uids(self, k=self.config.neuron.sample_size)
    #         miner_uids = [1]
    #         for i in [0,1]:
    #             bt.logging.info("availability:", i, self.check_uid_availability(i, self.config.neuron.vpermit_tao_limit))

    #         # search_query = SearchSynapse(
    #         #     query_string=query_string,
    #         #     size=self.config.neuron.search_result_size,
    #         #     version=get_version(),
    #         # )

    #         # bt.logging.info(
    #         #     f"Sending query: {syn} to miners: {[(uid, self.metagraph.axons[uid] )for uid in miner_uids]}"
    #         # )

    #         # The dendrite client queries the network.
    #         responses = await self.dendrite(
    #             # Send the query to selected miner axons in the network.
    #             axons=[self.metagraph.axons[uid] for uid in miner_uids],
    #             # axons=list(self.metagraph.axons),
    #             synapse=synapse,

    #             # TODO check deserialize
    #             # deserialize=True,
    #             deserialize=False,
    #             streaming=True,

    #             # TODO lol
    #             # set the miner query timeout to be 120 seconds to allow more operations in miner
    #             # timeout=120,
    #             timeout=float(120),
    #         )

    #         for resp in responses:
    #             async for chunk in resp:
    #                 print(chunk)

    #         # Log the results for monitoring purposes.
    #         bt.logging.info(f"Received responses: {responses}")
    #         bt.logging.debug(f"{type(responses)=} {len(responses)=}")

    #         # rewards = self.evaluator.evaluate(
    #         #     search_query.query_string, search_query.size, responses
    #         # )

    #         # bt.logging.info(f"Scored responses: {rewards} for {miner_uids}")
    #         # # Update the scores based on the rewards. You may want to define your own update_scores function for custom behavior.
    #         # self.update_scores(rewards, miner_uids)
    #     except Exception as err:
    #         bt.logging.error("Error during validation", str(err))
    #         bt.logging.debug(print_exception(type(err), err, err.__traceback__))

    async def concurrent_forward(self, portfolio):
        coroutines = [
            self.forward(portfolio) for _ in range(self.config.neuron.num_concurrent_forwards)
        ]
        await asyncio.gather(*coroutines)


    def send(self, portfolio: PortfolioRecord):
        self.sync()
        self.loop.run_until_complete(self.forward(portfolio))


# This is the main function, which runs the miner.
if __name__ == "__main__":
    ...
    # with StreamingTemplateMiner():
    #     while True:
    #         time.sleep(1)
