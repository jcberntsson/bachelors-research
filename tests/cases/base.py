import abc
import random

from core import Case


class Base:
    # Data
    raceone = {'users': [], 'events': [], 'races': [], 'coordinates': [], 'activities': []}

    skim = {'users': [], 'projects': [], 'images': [], 'skus': [], 'comments': []}

    def init(self, company):
        self.clearData()
        if company == "raceone":
            self.initRaceOne()
        elif company == "skim":
            self.initSkim()
        elif company == "reddit":
            self.initReddit()
        else:
            print(company + " not supported as type")

    @abc.abstractmethod
    def initRaceOne(self):
        return

    @abc.abstractmethod
    def initSkim(self):
        return

    @abc.abstractmethod
    def initReddit(self):
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
    def insertMaps(self):
        return

    @abc.abstractmethod
    def removeCoords(self):
        return

    @abc.abstractmethod
    def updateRace(self):
        return

    @abc.abstractmethod
    def removeRace(self):
        return

    @abc.abstractmethod
    def duplicateEvent(self):
        return

    @abc.abstractmethod
    def fetchMapLength(self):
        return

    # Reddit
    @abc.abstractmethod
    def fetchCommentedPosts(self):
        return

    @abc.abstractmethod
    def fetchHotPosts(self):
        return

    @abc.abstractmethod
    def fetchPostLength(self):
        return

    @abc.abstractmethod
    def fetchComments(self):
        return

    @abc.abstractmethod
    def fetchHotPostsInSub(self):
        return

    @abc.abstractmethod
    def createComment(self):
        return

    @abc.abstractmethod
    def upvote(self):
        return

    @abc.abstractmethod
    def fetchUsersAndComments(self):
        return

    @abc.abstractmethod
    def fetchBestFriend(self):
        return

    # Help methods
    @staticmethod
    def new_rand_int(rands, start, end):
        rand = random.randint(start, end)
        while rands is not None and len(rands) > 0 and rand in rands:
            rand = random.randint(start, end)
        return rand

    @staticmethod
    def create_case(name, setup, run, teardown):
        return type(name, (Case, object), {"setup": setup, "run": run, "teardown": teardown})()
