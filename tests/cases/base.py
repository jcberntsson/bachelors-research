import abc
import random

from core import Case


class Base:

    quantities = {
        "raceone": {
            "users": 100,
            "organizers": 10,
            "events": 10,
            "races": 5,
            "race_coordinates": 100,
            "activities": 10,
            "activity_coordinates": 50
        },
        "skim": {
            "users": 100,
            "projects": 10,
            "collaborators": 10,
            "project_images": 100,
            "skus": 20,
            "sku_values": 15,
            "sku_images": 2,
            "image_comments": 5
        }
    }

    current_company = None

    def quantity_of(self, entity):
        if self.current_company is None:
            return 0
        else:
            return self.quantities[self.current_company][entity.lower()]

    def multiply_quantities_with(self, multiplicator):
        for company, props in self.quantities.items():
            for name, value in props.items():
                props[name] *= multiplicator

    def init(self, company):
        company = company.lower()
        self.current_company = company
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
