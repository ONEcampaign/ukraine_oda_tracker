import datetime
from time import sleep

import numpy as np
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains

import country_converter


UNHCR_URL: str = (
    "https://app.powerbi.com/view?r=eyJrIjoiNzkyMjdmN2QtMjdlNy00YT"
    "gyLWI5Y2UtMDMwM2RjZjI4MzY2IiwidCI6ImU1YzM3OTgxLTY2NjQtNDEzNC04"
    "YTBjLTY1NDNkMmFmODBiZSIsImMiOjh9"
)


def _get_driver() -> webdriver.chrome:
    """Get driver for Chrome"""

    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")

    CHROME = ChromeDriverManager().install()

    return webdriver.Chrome(service=Service(CHROME))


def _get_list_of_elements(driver: webdriver.chrome) -> list:
    """Get table elements as a list of strings"""
    # Get page
    driver.get(UNHCR_URL)
    sleep(5)

    # Reload page
    driver.get(UNHCR_URL)
    sleep(15)

    # Get text element
    text_element = driver.find_element(by=By.CLASS_NAME, value="textRun")
    driver.execute_script("arguments[0].scrollIntoView();", text_element)
    sleep(5)

    # Get element containing the data
    parent = driver.find_elements(by=By.CLASS_NAME, value="pivotTableCellWrap")
    sleep(5)

    return [child.text for child in parent]


def _get_neighbouring_df(elements_list: list) -> pd.DataFrame:

    neighbouring = np.array(elements_list[0:48]).reshape(
        int(len(elements_list[0:48]) / 6), 6
    )

    if neighbouring.shape != (8, 6):
        raise ValueError("Shape of neighbouring data is not correct")

    return pd.DataFrame(neighbouring[1:], columns=neighbouring[0:1][0])


def _get_other_df(elements_list: list) -> pd.DataFrame:

    other = np.array(elements_list[54:]).reshape(int(len(elements_list[54:]) / 4), 4)

    if other.shape != (39, 4):
        raise ValueError("Shape of other data is not correct")

    return pd.DataFrame(other[1:-1], columns=other[0:1][0])


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:

    # Clean numbers
    df = df.apply(lambda row: row.str.replace(",", ""), axis=1)

    # Remove ambiguous name
    df["Country"] = df.Country.replace(
        "Serbia and Kosovo: S/RES/1244 (1999)", "Serbia", regex=False
    )
    cols = list(df.columns)

    # Add iso codes
    df["iso_code"] = country_converter.convert(df.Country, to="ISO3", not_found=None)

    # Fix turkey
    df.iso_code = df.iso_code.replace("Türkiye", "TUR", regex=False)

    # Change date format
    try:
        df["Data Date"] = pd.to_datetime(df["Data Date"], format="%m/%d/%Y")

        if df["Data Date"].max() > datetime.datetime.today():
            raise ValueError("Data date is in the future")

        df["Data Date"] = df["Data Date"].dt.strftime("%d %B %Y")

    except ValueError:
        df["Data Date"] = pd.to_datetime(
            df["Data Date"], format="%d/%m/%Y"
        ).dt.strftime("%d %B %Y")

    df = df.filter(["iso_code"] + cols, axis=1)

    return df


def get_unhcr_data() -> pd.DataFrame:
    """Get UNHCR data from the OECD"""

    # Get driver
    driver = _get_driver()

    # Get list of elements
    elements_list = _get_list_of_elements(driver)
    # Clean elements list
    elements_list = [str(item).replace("\n", "").strip() for item in elements_list]

    # Get neighbouring data
    neighbouring_df = _get_neighbouring_df(elements_list)

    # Get other data
    other_df = _get_other_df(elements_list)

    # Combine data
    df = pd.concat([neighbouring_df, other_df], ignore_index=True)

    # Close driver
    driver.quit()

    return df.pipe(_clean_df)


if __name__ == "__main__":
    data = get_unhcr_data()
