import abc


class Case:

    @abc.abstractmethod
    def setup(self):
        return

    @abc.abstractmethod
    def run(self):
        return

    @abc.abstractmethod
    def teardown(self):
        return
