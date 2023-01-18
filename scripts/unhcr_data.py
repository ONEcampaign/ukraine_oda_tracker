import json
import os

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

from scripts import config
from scripts.unhcr_tools.get_page import get_unhcr_data

# Load key as json object
KEY: json = json.loads(os.environ["SHEETS_API"])

WORKBOOK_KEY: str = "1VIaZMH4_myGAwIfeXzfjhiQ6WjFXgt559sThuM3_AaM"
WORKSHEET_KEY: int = 967116072
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
CREDENTIALS = ServiceAccountCredentials.from_json_keyfile_dict(
    keyfile_dict=KEY, scopes=SCOPES
)
WORKSHEET_KEY_NEW: int = 269028567


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


def load_historic_hcr_data() -> pd.DataFrame:
    """Load a manually downloaded file containing historic HCR numbers for refugees."""

    files = [
        "early_august",
        "early_december",
        "early_january",
        "early_november",
        "early_october",
        "early_september",
        "july",
        "late_december",
        "late_july",
        "late_november",
        "late_october",
        "late_september",
        "mid_august",
        "mid_december",
        "mid_january",
        "mid_november",
        "mid_october",
        "mid_september",
    ]

    dfs = []

    for file in files:
        dfs.append(
            pd.read_csv(
                f"{config.PATHS.data}/{file}_hcr_data.csv",
                parse_dates=["Data Date"],
            )
        )

    return pd.concat(dfs)


def clean_hrc_data(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.filter(
            [
                "iso_code",
                "Country",
                "Data Date",
                "Individual refugees from Ukraine recorded across Europe",
            ],
            axis=1,
        )
        .sort_values(by=["iso_code", "Data Date"])
        .reset_index(drop=True)
    )


def filter_hrc_data_by_month(df: pd.DataFrame) -> pd.DataFrame:
    """Keep one observation per month per country"""

    return df.groupby(
        by=[df.Country, df["Data Date"].dt.month], as_index=False, sort=False
    ).last()


def monthly_difference_by_country(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate the difference in refugees from one month to the next
    for each country"""

    column = "Individual refugees from Ukraine recorded across Europe"

    df["diff"] = df.groupby(by="iso_code")[column].diff().fillna(df[column])

    return df


def add_yearly_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate the share of the amount that should be allocated to each year
    based on the number of months left in the year"""

    df["month"] = df["Data Date"].dt.month

    df["ratio22"] = df.month.apply(lambda x: 1 - ((x - 1) / 12))
    df["ratio23"] = 1 - df.ratio22

    df.loc[df["Data Date"].dt.year > 2022, "ratio24"] = df["ratio23"]
    df.loc[df["Data Date"].dt.year > 2022, "ratio23"] = df["ratio22"]
    df.loc[df["Data Date"].dt.year > 2022, "ratio22"] = 0

    return df.fillna(0).drop(["month"], axis=1)


def clean_hcr_data_download(df=pd.DataFrame) -> None:
    df["Data Date"] = pd.to_datetime(df["Data Date"])
    return df.astype({"Individual refugees from Ukraine recorded across Europe": int})


def hcr_data_pipeline() -> pd.DataFrame:
    """Load and process HCR data"""

    # Get the latest data from the UNHCR website and clean the data types
    new_data = get_unhcr_data().pipe(clean_hcr_data_download)

    # Combine the new and historic data into a list
    data_files = [load_historic_hcr_data(), new_data]

    # Run data through pipeline
    data = (
        pd.concat(data_files, ignore_index=True)
        .pipe(clean_hrc_data)
        .pipe(filter_hrc_data_by_month)
        .pipe(monthly_difference_by_country)
        .pipe(add_yearly_ratios)
    )

    # Change the date format
    data["Data Date"] = data["Data Date"].dt.strftime("%m-%Y")

    return data


def upload_hcr_data() -> None:
    # Get data from UNHCR
    data = hcr_data_pipeline()

    # Authenticate and load worksheet
    auth = _authenticate()
    wb = _get_workbook(auth, WORKBOOK_KEY)
    sheet = _get_worksheet(wb, WORKSHEET_KEY_NEW)

    # Upload data
    df2gsheet(data, sheet)


def load_hrc_data() -> None:
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
    # load_hrc_data()
    upload_hcr_data()
