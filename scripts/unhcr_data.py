import json
import os

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

from scripts.unhcr_tools.get_page import get_unhcr_data

# Load key as json object
KEY: json = json.loads(os.environ["SHEETS_API"])

WORKBOOK_KEY: str = "1VIaZMH4_myGAwIfeXzfjhiQ6WjFXgt559sThuM3_AaM"
WORKSHEET_KEY: int = 967116072
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CREDENTIALS = ServiceAccountCredentials.from_json_keyfile_dict(
    keyfile_dict=KEY, scopes=SCOPES
)


def _authenticate() -> gspread.client.Client:
    """Authenticate with Google Sheets API"""

    return gspread.authorize(CREDENTIALS)


def _get_workbook(
    authenticated_client: gspread.client.Client, workbook_key: str
) -> gspread.Spreadsheet:
    """Get workbook from Google Sheets API"""

    return authenticated_client.open_by_key(key=workbook_key)


def _get_worksheet(
    workbook: gspread.Spreadsheet, worksheet_key: int
) -> gspread.Worksheet:
    """Get worksheet from Google Sheets API"""

    return workbook.get_worksheet_by_id(id=worksheet_key)


def df2gsheet(df: pd.DataFrame, worksheet_obj: gspread.Worksheet) -> None:
    """Write dataframe to Google Sheets API"""

    columns = [str(col).replace("\n", "").strip() for col in df.columns]
    values = df.fillna("").values.tolist()

    worksheet_obj.update([columns] + values)


def load_hdrc_data() -> None:
    """Load data to google docs"""

    # Get data from UNHCR
    data = get_unhcr_data()

    # Authenticate and load worksheet
    auth = _authenticate()
    wb = _get_workbook(auth, WORKBOOK_KEY)
    sheet = _get_worksheet(wb, WORKSHEET_KEY)

    # Upload data
    df2gsheet(data, sheet)


if __name__ == "__main__":
    load_hdrc_data()
