import gspread
from google.oauth2.service_account import Credentials
import os

def auth_gss_client(path, scopes):
    credentials = Credentials.from_service_account_file(path, scopes=scopes)
    return gspread.authorize(credentials)

# Connect to Google Sheet
scope = ['https://www.googleapis.com/auth/spreadsheets']
auth_json_path = 'google-enhance-auth.json'

# Authenticate with Google
credentials = auth_gss_client(auth_json_path, scope)

# Open the Google Sheet
google_sheet_key = '1pfDqNe9CDt1dK_9osMIHqQadxOp-4du418I4lLigSuw'
sheet = credentials.open_by_key(google_sheet_key).sheet1


# Read data from Google Sheet
data = sheet.get_all_values()
print(data)

# write data to Google Sheet
sheet.update_cell(1, 1, 'Hello World!')

