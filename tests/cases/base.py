import abc
import random

from core import Case


class Base:

    def init(self, company):
        self.clearData()
        if company == "raceone":
            self.initRaceOne()
        elif company == "skim":
            self.initSkim()
        else:
            print(company + " not supported as type")

    @abc.abstractmethod
    def initRaceOne(self):
        return

    @abc.abstractmethod
    def initSkim(self):
        return

    @abc.abstractmethod
    def clearData(self):
        return

    # SKIM
    @abc.abstractmethod
    def fetchSKU(self):
        return

    @abc.abstractmethod
    def fetchUsers(self):
        return

    @abc.abstractmethod
    def commentOnImage(self):
        return

    @abc.abstractmethod
    def pairImageSKU(self):
        return

    @abc.abstractmethod
    def fetchAllUserComments(self):
        pass

    @abc.abstractmethod
    def addRowsToSKU(self):
        pass

    # RaceOne
    @abc.abstractmethod
    def follow(self):
        return

    @abc.abstractmethod
    def unfollow(self):
        return

    @abc.abstractmethod
    def unparticipate(self):
        return

    @abc.abstractmethod
    def fetchParticipants(self):
        return

    @abc.abstractmethod
    def fetchParticipants2(self):
        return

    @abc.abstractmethod
    def insertCoords(self):
        return

    @abc.abstractmethod
    def fetchCoords(self):
        return

    @abc.abstractmethod
    def updateCoords(self):
        return

    @abc.abstractmethod
    def removeCoords(self):
        return

    @abc.abstractmethod
    def removeRace(self):
        return

    @abc.abstractmethod
    def duplicateEvent(self):
        return

    @abc.abstractmethod
    def fetchRace(self):
        pass

    @abc.abstractmethod
    def fetchHotRaces(self):
        pass

    # Help methods
    @staticmethod
    def new_rand_int(rands, start, end):
        rand = random.randint(start, end)
        if rands is not None or len(rands) > 0:
            while rand in rands:
                rand = random.randint(start, end)
        return rand

    @staticmethod
    def create_case(name, setup, run, teardown):
        return type(name, (Case, object), {"setup": setup, "run": run, "teardown": teardown})()
