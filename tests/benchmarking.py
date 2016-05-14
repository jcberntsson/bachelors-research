#!/usr/bin/python

# Imports
from sys import argv, stdout
from timeit import timeit
from core import Sheet


class BenchMarker():
    # Declaration of all test cases. Should reflect the google spreadsheet to enable synchronization.
    test_cases = {
        'skim': [
            'fetchSKU',
            'fetchUsers',
            'commentOnImage',
            'pairImageSKU',
            'addRowsToSKU',
            'fetchAllUserComments'
        ],
        'raceone': [
            'follow',
            'unfollow',
            'insertCoords',
            'fetchParticipants',
            'fetchParticipants2',
            'unparticipate',
            'fetchCoords',
            'removeCoords',
            'fetchHotRaces',
            'fetchRace',
            'removeRace'
        ],
        'reference': [
            'tinyGet',
            'smallGet'
        ]
    }

    # Definition of the column system in the Google Spreadsheet
    cols = {"total_time": 2, "peak_time": 3, "avg_time": 4, "load_peak": 5, "load_avg": 6}
    databases = ["mysql", "neo4j", "mongo"]
    iterations = 1000

    def __init__(self, args):
        # Check if argument is provided
        if len(args) < 2:
            print("Too few arguments. A argument for test file is needed.")
            print("Syntax: 'python benchmarking.py <database> <company> <?data multiplier = 1>'")
            print("Example: 'python benchmarking.py mysql skim'")
            exit()

        # Extract arguments
        print("Extracting arguments...")
        database = argv[1] if len(argv) > 1 else "all"
        company = argv[2] if len(argv) > 2 else "all"
        self.multiplier = int(argv[3]) if len(argv) > 3 else 1

        # Test arguments
        is_valid_company = (company == "raceone" or company == "skim" or company == "reference")
        is_valid_database = (database == "neo4j" or database == "mysql" or database == "mongo")
        if not (is_valid_company or company == "all") or not (is_valid_database or database == "all"):
            print("Invalid company " + company + " or invalid database " + database)
            print("Syntax: 'python benchmarking.py <database> <company><data multiplier>'")
            print("Example: 'python benchmarking.py mysql skim'")
            exit()

        for db in self.databases:
            self.test_init(db, "skim")
        #if is_valid_company and is_valid_database:
        #    self.run_tests(database, company)
        #else:
        #    print("Running all tests")
        #    self.run_all_tests()

    def run_all_tests(self):
        for comp, comp_cases in self.test_cases.items():
            for db in self.databases:
                self.run_tests(db, comp)

    @staticmethod
    def test_init(database, company):
        print("Testing init for %s and %s" % (database, company))
        if database == 'neo4j':
            from cases import Neo4j
            database_class = Neo4j()
        elif database == 'mysql':
            from cases import MySQL
            database_class = MySQL()
        elif database == 'mongo':
            from cases import Mongo
            database_class = Mongo()
        else:
            from cases import Neo4j
            database_class = Neo4j()
        database_class.clearData()
        database_class.current_company = company

        if company == "raceone":
            init = database_class.initRaceOne
        elif company == "skim":
            init = database_class.initSkim
        else:
            init = database_class.initReference

        time = 0
        try:
            time = timeit(init, number=1) * 1000  # in ms
        except Exception as ex:
            print("Exception during execution: %s" % ex)
        print("Result for %s and %s is %d ms" % (database, company, time))

    def run_tests(self, database, company):
        print("Running %s %s" % (database, company))
        # Import test class
        print("Initializing database...")
        if database == 'neo4j':
            from cases.neo4j import Neo4j
            database_class = Neo4j()
        elif database == 'mysql':
            from cases.mysql import MySQL
            database_class = MySQL()
        elif database == 'mongo':
            from cases.mongo import Mongo
            database_class = Mongo()
        else:
            from cases.neo4j import Neo4j
            database_class = Neo4j()
        database_class.multiply_quantities_with(self.multiplier)
        database_class.init(company)

        # Configure the google sheet sync
        print("Initializing Google Sheet...")
        sheet = Sheet(database + "-" + company)

        # Run test case
        print("Running tests...")
        for test_case in self.test_cases[company]:
            print("======== Start %s ========" % test_case)
            case_to_test = getattr(database_class, test_case)
            time_array = []
            error = False
            last_percentage = 0
            for i in range(self.iterations):
                percentage = int(i / self.iterations * 100)
                case = case_to_test()
                try:
                    case.setup()
                    time = timeit(case.run, number=1) * 1000  # in ms
                    case.teardown()
                    time_array.append(time)
                except Exception as ex:
                    error = True
                    print("Exception during execution: %s" % ex)
                    break
                if percentage > last_percentage:
                    stdout.write("|")
                    stdout.flush()
                    last_percentage = percentage
            print()

            # Calculate results
            if error:
                total_time = -1
                peak_time = -1
                avg_time = -1
            else:
                total_time = 0
                peak_time = 0
                for time in time_array:
                    total_time += time
                    if time > peak_time:
                        peak_time = time
                avg_time = total_time / len(time_array)

            # Log results
            print("Results (in ms):")
            print("Total: %f" % total_time)
            print("Average: %f" % avg_time)
            print("Peak: %f" % peak_time)

            # Sync to Google Sheets
            if sheet is not None:
                try:
                    print("Syncing to Google Sheets")
                    sheet.update_value(test_case, self.cols["total_time"], total_time)
                    sheet.update_value(test_case, self.cols["peak_time"], peak_time)
                    sheet.update_value(test_case, self.cols["avg_time"], avg_time)
                except Exception as ex:
                    print(ex)
            print("======== End %s ========" % test_case)
        print("Tests are done")


# Run
if __name__ == '__main__':
    bench_marker = BenchMarker(argv)
