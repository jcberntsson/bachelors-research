import gspread
class Sheet:
	sheet = False
	dictionaries = []

	def __init__(self, sheetName):
		# Acquire access to the google spreadsheet
		from oauth2client.service_account import ServiceAccountCredentials
		scope = ['https://spreadsheets.google.com/feeds', 'https://docs.google.com/feeds']
		credentials = ServiceAccountCredentials.from_json_keyfile_name('Kandidat-877c3960fa23.json', scope)
		gc = gspread.authorize(credentials)
		try:
			self.sheet = gc.open("Test Cases").worksheet(sheetName)
			self.dictionaries = self.sheet.get_all_records(empty2zero=False, head=1)
		except (SpreadsheetNotFound, WorksheetNotFound) as x:
			print(x)
			
	def findRowNr(self, rowName):
		for cc in range(len(self.dictionaries)):
			if self.dictionaries[cc]["Name"] == rowName:
				return cc + 2
				
		return -1
	
	def updateValue(self, rowName, colNr, value):
		if type(self.sheet) == type(True) and not self.sheet:
			print("No sheet")
			return
		
		rownr = self.findRowNr(rowName)
		
		if rownr >= 0:
			self.sheet.update_cell(rownr, colNr, value)
		else:
			print("Can not find row")