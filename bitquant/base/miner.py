import copy
import time
import asyncio
import argparse
import threading
import traceback
from abc import ABC, abstractmethod
from functools import partial
from starlette.types import Send

import bittensor as bt
from typing import List, Dict, Tuple, Union, Callable, Awaitable

from bitquant.base.protocol import StreamingTradeHistory
from bitquant.base.neuron import BaseNeuron

class QuantMiner(BaseNeuron):
    def __init__(self, config=None):
        # initiate neuron
        super().__init__(config)

        # warn if allowing incoming requests from anyone.
        if not self.config.blacklist.force_validator_permit:
            bt.logging.warning(
                "You are allowing non-validators to send requests to your miner. This is a security risk."
            )
        if self.config.blacklist.allow_non_registered:
            bt.logging.warning(
                "You are allowing non-registered entities to send requests to your miner. This is a security risk."
            )

        self.axon = bt.axon(wallet=self.wallet, config=self.config)
        # self.axon = bt.axon(
        #     wallet=self.wallet, port=self.config.axon.port
        # )

        bt.logging.info(f"Attaching forward function to miner axon.")
        self.axon.attach(
            forward_fn=self.forward,
            blacklist_fn=self.blacklist,
            priority_fn=self.priority,
        )

        bt.logging.info(f"Axon created: {self.axon}")

        self.last_sync_block = self.block - 1000

        # Instantiate miner runners
        self.should_exit: bool = False
        self.is_running: bool = False
        self.thread_ctx = {}
        # self.thread: threading.Thread = None
        self.lock = asyncio.Lock()

        # istantiate miner data
        self.portfolio = []
        '''
        self.request_timestamps: dict = {}
        thread = threading.Thread(target=get_valid_hotkeys, args=(self.config,))
        '''

    def forward(self, synapse: StreamingTradeHistory) -> StreamingTradeHistory:
        start_time = synapse.miner_window.start
        end_time = synapse.miner_window.end

        # create lazy stream function to stream new portfolio updates within start_time and end_time
        async def _stream(start_time: int, end_time: int, send: Send):
            assert self.block >= start_time, f"{self.block=}, {start_time=}"
            while self.block <= end_time:
                # pull from portfolio and send?
                await send(
                    {
                        "type": "http.response.body",
                        # "body": joined_buffer.encode("utf-8"),
                        "more_body": True,
                    }
                )
                ...

        portfolio_streamer = partial(_stream, start_time, end_time)
        return synapse.create_streaming_response(portfolio_streamer)

    def run_in_background_thread(self):
        """
        Starts the miner's operations in a separate background thread.
        This is useful for non-blocking operations.
        """
        if not self.is_running:
            bt.logging.debug("Starting miner in background thread.")
            self.should_exit = False
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
            self.is_running = True
            bt.logging.debug("Started")

    def stop_run_thread(self):
        """
        Stops the miner's operations that are running in the background thread.
        """
        if self.is_running:
            bt.logging.debug("Stopping miner in background thread.")
            self.should_exit = True
            self.thread.join(5)
            self.is_running = False
            bt.logging.debug("Stopped")

    def run(self):
        """
        Runs the miner logic. This method starts the miner's operations, including
        listening for incoming requests and periodically updating the miner's knowledge
        of the network graph.
        """
        # TODO really should only check once in __init__?
        self.check_registered()

        bt.logging.info(
            f"Serving axon {StreamingTradeHistory} on network: {self.config.subtensor.chain_endpoint} with netuid: {self.config.netuid}"
        )
        self.axon.serve(netuid=self.config.netuid, subtensor=self.subtensor)

        bt.logging.info(
            f"Starting axon server on port: {self.config.axon.port}"
        )
        self.axon.start()

        # run main loop
        bt.logging.info(f"Starting main loop")
        step = 0
        try:
            while not self.should_exit:
                _start_epoch = time.time()

                # --- Wait until next epoch.
                current_block = self.subtensor.get_current_block()
                while (
                    current_block - self.last_epoch_block
                    < self.config.miner.blocks_per_epoch
                ):
                    # --- Wait for next bloc.
                    time.sleep(1)
                    current_block = self.subtensor.get_current_block()

                    # --- Check if we should exit.
                    if self.should_exit:
                        break

                # --- Update the metagraph with the latest network state.
                self.last_epoch_block = self.subtensor.get_current_block()

                metagraph = self.subtensor.metagraph(
                    netuid=self.config.netuid,
                    lite=True,
                    block=self.last_epoch_block,
                )
                log = (
                    f"Step:{step} | "
                    f"Block:{metagraph.block.item()} | "
                    f"Stake:{metagraph.S[self.my_subnet_uid]} | "
                    f"Rank:{metagraph.R[self.my_subnet_uid]} | "
                    f"Trust:{metagraph.T[self.my_subnet_uid]} | "
                    f"Consensus:{metagraph.C[self.my_subnet_uid] } | "
                    f"Incentive:{metagraph.I[self.my_subnet_uid]} | "
                    f"Emission:{metagraph.E[self.my_subnet_uid]}"
                )
                bt.logging.info(log)

                step += 1

        # If someone intentionally stops the miner, it'll safely terminate operations.
        except KeyboardInterrupt:
            self.axon.stop()
            bt.logging.success("Miner killed by keyboard interrupt.")
            exit(0)

        # In case of unforeseen errors, the miner will log the error and continue operations.
        except Exception as e:
            bt.logging.error(traceback.format_exc())



# This is the main function, which runs the miner.
if __name__ == "__main__":
    ...
    # with StreamingTemplateMiner():
    #     while True:
    #         time.sleep(1)
