from bitquant.base.neuron import BaseNeuron


class QuantValidator:
    ...








    # TODO this was in neuron before. Should this be in validator?
    def should_set_weights(self) -> bool:
        # don't set weights on initialization or if disabled
        if self.step == 0 or self.config.neuron.disable_set_weights:
            return False

        # check if enough epoch blocks have elapsed since the last epoch.
        return (
            (self.block - self.metagraph.last_update[self.uid])
            > self.config.neuron.epoch_length
            )  # don't set weights if you're a miner
