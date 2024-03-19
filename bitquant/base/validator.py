import copy
import asyncio
import threading
from collections import defaultdict
from pathlib import Path
from traceback import print_exception
from typing import List

import bittensor as bt

from bitquant.base.neuron import BaseNeuron
from bitquant.base.protocol import PortfolioRecord
from bitquant.utils.timeutils import TimeUtils
import torch

class QuantValidator(BaseNeuron):
    def __init__(self, evaluation_window:int, evaluator, config=None):
        self.evaluation_window = evaluation_window
        self.evaluator = evaluator
        # initiate neuron
        super().__init__(config, type(self).__name__)
        self.hotkeys = copy.deepcopy(self.metagraph.hotkeys)
        self.axon = bt.axon(wallet=self.wallet, config=self.config, port=self.config.axon.port)
        self.axon.attach(
            forward_fn=self.forward,
            # blacklist_fn=self.blacklist,
            # priority_fn=self.priority,
        )

        # Set up initial scoring weights for validation
        bt.logging.info("Building validation weights.")
        self.scores = torch.zeros_like(self.metagraph.S, dtype=torch.float32)

        # self.load_state()

        self.miner_portfolio_history = defaultdict(list)


        # Create asyncio event loop to manage async tasks.
        self.loop = asyncio.get_event_loop()

        # Instantiate runners
        self.should_exit: bool = False
        self.is_running: bool = False
        self.thread: threading.Thread = None
        self.lock = asyncio.Lock()



    def resync_metagraph(self):
        """Resyncs the metagraph and updates the hotkeys and moving averages based on the new metagraph."""
        bt.logging.info("resync_metagraph()")

        # Copies state of metagraph before syncing.
        previous_metagraph = copy.deepcopy(self.metagraph)

        # Sync the metagraph.
        super().resync_metagraph()

        # Check if the metagraph axon info has changed.
        if previous_metagraph.axons == self.metagraph.axons:
            return

        bt.logging.info(
            "Metagraph updated, re-syncing hotkeys, dendrite pool and moving averages"
        )
        # Zero out all hotkeys that have been replaced.
        for uid, hotkey in enumerate(self.hotkeys):
            if hotkey != self.metagraph.hotkeys[uid]:
                self.scores[uid] = 0  # hotkey has been replaced

        # Check to see if the metagraph has changed size.
        # If so, we need to add new hotkeys and moving averages.
        if len(self.hotkeys) < len(self.metagraph.hotkeys):
            # Update the size of the moving average scores.
            new_moving_average = torch.zeros((self.metagraph.n)).to(self.device)
            min_len = min(len(self.hotkeys), len(self.scores))
            new_moving_average[:min_len] = self.scores[:min_len]
            self.scores = new_moving_average

        # Update the hotkeys.
        self.hotkeys = copy.deepcopy(self.metagraph.hotkeys)

    # TODO review
    def update_scores(self, rewards: torch.FloatTensor, uids: List[int]):
        """Performs exponential moving average on the scores based on the rewards received from the miners."""

        # Check if rewards contains NaN values.
        if torch.isnan(rewards).any():
            bt.logging.warning(f"NaN values detected in rewards: {rewards}")
            # Replace any NaN values in rewards with 0.
            rewards = torch.nan_to_num(rewards, 0)

        # Compute forward pass rewards, assumes uids are mutually exclusive.
        # shape: [ metagraph.n ]
        scattered_rewards: torch.FloatTensor = self.scores.scatter(
            0, uids.clone().detach().to(self.device), rewards
        ).to(self.device)
        bt.logging.debug(f"Scattered rewards: {rewards}")

        # Update scores with rewards produced by this step.
        # shape: [ metagraph.n ]
        alpha: float = self.config.neuron.moving_average_alpha
        self.scores: torch.FloatTensor = alpha * scattered_rewards + (
            1 - alpha
        ) * self.scores.to(self.device)
        bt.logging.debug(f"Updated moving avg scores: {self.scores}")

    # TODO update path
    def save_state(self):
        """Saves the state of the validator to a file."""
        bt.logging.info("Saving validator state.")

        # Save the state of the validator to file.
        torch.save(
            {
                "step": self.step,
                "scores": self.scores,
                "hotkeys": self.hotkeys,
            },
            self.config.neuron.full_path + "/state.pt",
        )

    # def check_uid_availability(self, uid: int, vpermit_tao_limit: int) -> bool:
    #     # Filter non serving axons.
    #     if not self.metagraph.axons[uid].is_serving:
    #         return False
    #     # Filter validator permit > 1024 stake.
    #     if self.metagraph.validator_permit[uid]:
    #         if self.metagraph.S[uid] > vpermit_tao_limit:
    #             return False
    #     # Available otherwise.
    #     return True

    # ===== main functions =====
    # TODO review
    def run(self):
        while True:
            self.sync()
            # for amount of time, start evaluating self.miner_portfolio_history

    async def forward(self, query: PortfolioRecord) -> PortfolioRecord:
        now_ms = TimeUtils.now_in_ms()
        self.miner_portfolio_history[query.miner_uid].append((now_ms, query.portfolio))
        query.response_code = "200"
        return query

    # ===== state functions ======

    def save_state(self):
        """Saves the state of the validator to a file."""
        bt.logging.info("Saving validator state.")

        # Save the state of the validator to file.
        torch.save(
            {
                "step": self.step,
                "scores": self.scores,
                "hotkeys": self.hotkeys,
            },
            self.config.neuron.full_path + "/state.pt",
        )

    def load_state(self):
        """Loads the state of the validator from a file."""
        bt.logging.info("Loading validator state.")

        if not Path(self.config.neuron.full_path + "/state.pt").exists():
            return False

        # Load the state of the validator from file.
        state = torch.load(self.config.neuron.full_path + "/state.pt")
        self.step = state["step"]
        self.scores = state["scores"]
        self.hotkeys = state["hotkeys"]

        bt.logging.info(f"Loaded state: Step: {self.step}, Scores: {self.scores}, Hotkeys: {self.hotkeys}")

    def print_state(self):
        metagraph = self.metagraph
        self.uid = self.metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)

        log = (
            "Validator | "
            f"Step:{self.step} | "
            f"UID:{self.uid} | "
            f"Block:{metagraph.block.item()} | "
            f"Stake:{metagraph.S[self.uid]} | "
            f"VTrust:{metagraph.Tv[self.uid]} | "
            f"Dividend:{metagraph.D[self.uid]} | "
            f"Emission:{metagraph.E[self.uid]}"
        )
        bt.logging.info(log)