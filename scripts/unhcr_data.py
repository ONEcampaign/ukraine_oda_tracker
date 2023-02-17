import pandas as pd

from scripts import config
from scripts.config import PATHS
from scripts.unhcr_tools.get_page import get_unhcr_data


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
                config.PATHS.raw_data / f"{file}_hcr_data.csv",
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

    df["difference"] = df.groupby(by="iso_code")[column].diff().fillna(df[column])

    return df


def add_yearly_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate the share of the amount that should be allocated to each year
    based on the number of months left in the year"""

    df["month"] = df["Data Date"].dt.month

    df["ratio22"] = df.month.apply(lambda x: 1 - ((x - 1) / 12))
    df["ratio23"] = 1 - df.ratio22

    # Correct the july 2022 ratio
    mask = (df["Data Date"].dt.year == 2022) & (df["Data Date"].dt.month == 7)
    df.loc[mask, "ratio22"] = 2 / 3
    df.loc[mask, "ratio23"] = 1 / 3

    df.loc[df["Data Date"].dt.year > 2022, "ratio24"] = df["ratio23"]
    df.loc[df["Data Date"].dt.year > 2022, "ratio23"] = df["ratio22"]
    df.loc[df["Data Date"].dt.year > 2022, "ratio22"] = 0

    return df.fillna(0).drop(["month"], axis=1)


def clean_hcr_data_download(df: pd.DataFrame) -> pd.DataFrame:
    df["Data Date"] = pd.to_datetime(df["Data Date"])
    return df.astype({"Individual refugees from Ukraine recorded across Europe": int})


def read_manual_ukraine_refugee_data() -> pd.DataFrame:
    return (
        pd.read_csv(PATHS.raw_data / "non-eu-refugees.csv", parse_dates=["date"])
        .astype({"value": int})
        .assign(date=lambda d: pd.to_datetime(d.date, format="%B-%y"))
        .rename(
            {
                "date": "Data Date",
                "value": "Individual refugees from Ukraine recorded across Europe",
                "country": "Country",
            },
            axis=1,
        )
    )


def update_ukraine_hcr_data() -> None:
    """Load and process HCR data"""

    # manual data
    manual_data = read_manual_ukraine_refugee_data()

    # Get the latest data from the UNHCR website and clean the data types
    new_data = get_unhcr_data().pipe(clean_hcr_data_download)

    # Combine the new and historic data into a list
    data_files = [load_historic_hcr_data(), new_data]

    # Run data through pipeline
    data = (
        pd.concat(data_files, ignore_index=True)
        .pipe(clean_hrc_data)
        .pipe(filter_hrc_data_by_month)
    )
    data = pd.concat([data, manual_data], ignore_index=True)

    data = data.pipe(monthly_difference_by_country).pipe(add_yearly_ratios)

    # Change the date format
    data["Data Date"] = data["Data Date"].dt.strftime("%m-%Y")

    data.to_csv(PATHS.output / "hcr_data.csv", index=False)
    print("Updated UNHCR recorded refugee data")


if __name__ == "__main__":
    update_ukraine_hcr_data()
