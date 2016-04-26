import gspread
from gspread import SpreadsheetNotFound
from gspread import WorksheetNotFound


class Sheet:
    sheet = False
    dictionaries = []

    def __init__(self, sheet_name):
        # Acquire access to the google spreadsheet
        from oauth2client.service_account import ServiceAccountCredentials
        scope = ['https://spreadsheets.google.com/feeds', 'https://docs.google.com/feeds']
        credentials = ServiceAccountCredentials.from_json_keyfile_name('resources/Kandidat-877c3960fa23.json', scope)
        gc = gspread.authorize(credentials)
        try:
            self.sheet = gc.open("Test Cases").worksheet(sheet_name)
            self.dictionaries = self.sheet.get_all_records(empty2zero=False, head=1)
        except (SpreadsheetNotFound, WorksheetNotFound) as x:
            print(x)

    @staticmethod
    def format_value(value):
        return str(value).replace(".", ",")

    def find_row_nr(self, row_name):
        for cc in range(len(self.dictionaries)):
            if self.dictionaries[cc]["Name"] == row_name:
                return cc + 2

        return -1

    def update_value(self, row_name, col_nr, value):
        if not self.sheet:
            print("No sheet")
            return

        row_nr = self.find_row_nr("#" + row_name)

        if row_nr >= 0:
            self.sheet.update_cell(row_nr, col_nr, self.format_value(value))
        else:
            print("Can not find row")
