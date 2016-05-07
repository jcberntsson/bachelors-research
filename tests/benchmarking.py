#!/usr/bin/python

# Imports
from sys import argv
from timeit import timeit

# Check if argument is provided
if len(argv) < 2:
    print("Too few arguments. A argument for test file is needed.")
    print("Syntax: 'python benchmarking.py <database> <company>'")
    print("Example: 'python benchmarking.py mysql skim'")
    exit()

# Declaration of all test cases. Should reflect the google spreadsheet to enable synchronization.
test_cases = {
    'skim': [
        'pairImageSKU'
    ],
    'raceone': [
        'unfollow',
        'fetchComments',
        'fetchHotPosts'
    ],
    'reddit': []
}

# Definition of the column system in the Google Spreadsheet
cols = {"total_time": 2, "peak_time": 3, "avg_time": 4, "load_peak": 5, "load_avg": 6}

print("Extracting arguments")
# Extract arguments
database = argv[1]
company = argv[2]
iterations = 1000   # Number of runs per test case

# Test arguments
validCompany = (company == "raceone" or company == "skim" or company == "reddit")
if not validCompany:
    print("Invalid company " + company)
    exit()

# Import test class
print("Initializing database")
if database == 'neo4j':
    from cases.neo4j import Neo4j
    testClass = Neo4j()
elif database == 'mysql':
    from cases.mysql import MySQL
    testClass = MySQL()
else:
    from cases.neo4j import Neo4j
    testClass = Neo4j()
#testClass.init(company)
print("Database is done")

# Configure the google sheet sync
print("Initializing Google Sheet")
sheet = None
if validCompany:
    from core.sheet import Sheet
    sheet = Sheet(database + "-" + company)
print("Google Sheet is ready")

# Run
if __name__ == '__main__':
    # Run test case
    print("Running tests...")
    for test_case in test_cases[company]:
        print("======== Start %s ========" % test_case)
        testMethod = getattr(testClass, test_case)
        timeArray = []
        for i in range(iterations):
            case = testMethod()
            case.setup()
            # timeit unreliable?
            time = timeit(case.run, number=1) * 1000    # in ms
            case.teardown()
            timeArray.append(time)

        # TODO: Extract information for CPU load

        # Calculate results
        totalTime = 0
        peakTime = 0
        for time in timeArray:
            totalTime += time
            if time > peakTime:
                peakTime = time
        avgTime = totalTime / len(timeArray)

        # Log results
        print("Results (in ms):")
        print("Total: %f" % totalTime)
        print("Average: %f" % avgTime)
        print("Peak: %f" % peakTime)

        # Sync to Google Sheets
        if sheet is not None:
            print("Syncing to Google Sheets")
            sheet.update_value(test_case, cols["total_time"], totalTime)
            sheet.update_value(test_case, cols["peak_time"], peakTime)
            sheet.update_value(test_case, cols["avg_time"], avgTime)

        print("======== End %s ========" % test_case)

    print("Tests are done")