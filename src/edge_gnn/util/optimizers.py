class WarmupThenPlateau:
    """
    Custom sequential scheduler that handles a LinearLR warmup phase 
    and then seamlessly transitions to ReduceLROnPlateau.
    """
    def __init__(self, warmup_scheduler, plateau_scheduler, warmup_epochs: int):
        self.warmup_scheduler = warmup_scheduler
        self.plateau_scheduler = plateau_scheduler
        self.warmup_epochs = warmup_epochs
        self.current_epoch = 0

    def step(self, metrics=None):
        self.current_epoch += 1
        if self.current_epoch <= self.warmup_epochs:
            self.warmup_scheduler.step()
        else:
            self.plateau_scheduler.step(metrics)