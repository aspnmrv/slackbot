from abc import abstractmethod


class Worker:
    def __init__(self, sender, channel_id):
        self.sender = sender
        self.channel_id = channel_id

    @abstractmethod
    def send(self):
        pass

    @abstractmethod
    def get_data(self, sql: str):
        pass
