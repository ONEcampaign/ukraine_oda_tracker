import country_converter as coco
import pandas as pd
import pydeflate
from bblocks.import_tools.unzip import read_zipped_csv

from scripts.config import PATHS
from scripts.oda import read_idrc

from bblocks.dataframe_tools.add import add_iso_codes_column

from scripts.unhcr_data import (
    _authenticate,
    _get_workbook,
    WORKBOOK_KEY,
    _get_worksheet,
    WORKSHEET_KEY_NEW,
    df2gsheet,
)

HIGH_LOW = "high"
YEAR_START = 2018
YEAR_END = 2021


def update_unhcr_data(low_or_high: str) -> None:
    """Read historical UNHCR data and save it to a feather file"""
    url = (
        "https://api.unhcr.org/population/v1/"
        "asylum-applications/"
        "?limit=20&dataset=asylum-applications&"
        "displayType=totals&yearFrom=2010&yearTo=2021&"
        "coa_all=true&"
        "columns%5B%5D=procedure_type&"
        "columns%5B%5D=app_type&"
        "columns%5B%5D=app_pc&"
        "columns%5B%5D=app_size&"
        "columns%5B%5D=dec_level&"
        "columns%5B%5D=applied"
        "&download=true"
    )

    corrections = {
        (2018, "GBR"): 1,
        (2018, "KAZ"): 1,
        (2018, "USA"): 1,
        (2019, "FIN"): 1,
        (2019, "GBR"): 1,
        (2020, "GBR"): 1.3,
        (2020, "USA"): 1.5,
        (2020, "CYP"): 1,
        (2021, "GBR"): 1,
    }

    columns = {
        "year": "year",
        "country_of_asylum_iso": "iso_code",
        "application_type": "app_type",
        "applied": "value",
    }

    _replace = {x: "" for x in ["(", ")", "/"]}

    df = (
        read_zipped_csv(url, "asylum-applications.csv")
        .rename(
            columns=lambda x: x.lower()
            .replace(" ", "_")
            .translate(str.maketrans(_replace))
        )
        .rename(columns=columns)
        .assign(
            ratio=lambda d: d.set_index(["year", "iso_code"]).index.map(corrections),
            applied=lambda d: (d.value * d.ratio.fillna(1)).astype(int),
        )
        .filter(columns.values(), axis=1)
    )

    if low_or_high == "high":
        f_ = df.app_type.unique()
    elif low_or_high == "low":
        f_ = ["N"]
    else:
        raise ValueError('low_or_high must be "low" or "high"')

    df = (
        df[df.app_type.isin(f_)]
        .groupby(["year", "iso_code"], as_index=False)
        .sum(numeric_only=True)
    )

    df.to_feather(f"{PATHS.output}/unhcr_data_{low_or_high}.feather")


def read_historical_unhcr_data(low_or_high: str) -> pd.DataFrame:
    """Read the locally saved historical UNHCR data"""
    return pd.read_feather(f"{PATHS.output}/unhcr_data_{low_or_high}.feather")


def filter_dac(df: pd.DataFrame) -> pd.DataFrame:
    """Filter the data to only DAC countries (by ISO3 code)"""
    from oda_data.tools.groupings import donor_groupings

    dac = coco.convert(donor_groupings()["dac_countries"].values(), to="ISO3")

    return df[df.iso_code.isin(dac)]


def read_ukriane_hcr_data() -> pd.DataFrame:
    """Read the locally saved HCR data"""

    return pd.read_csv(f"{PATHS.output}/hcr_data.csv").rename(
        columns={
            "Individual refugees from Ukraine recorded across Europe": "value",
            "Country": "country",
            "Data Date": "date",
        }
    )


def yearly_refugees_spending(
    cost_data: pd.DataFrame, refugee_data: pd.DataFrame
) -> pd.DataFrame:
    """Calculate the yearly spending on refugees"""

    data = refugee_data.merge(cost_data, on=["iso_code"], how="left")

    data = data.assign(
        cost22=lambda d: d["difference"] * d.ratio22 * d.tot_cost_dfl,
        cost23=lambda d: d["difference"] * d.ratio23 * d.tot_cost_dfl,
        cost24=lambda d: d["difference"] * d.ratio24 * d.tot_cost_dfl,
    )

    return (
        data.groupby(["iso_code"], as_index=False)[
            ["difference", "cost22", "cost23", "cost24"]
        ]
        .sum(numeric_only=True)
        .rename({"difference": "total_refugees"}, axis=1)
    )


def yearly_constant_idrc() -> pd.DataFrame:
    """Read the saved IDRC data, format it, and convert it to constant prices"""
    idrc = (
        read_idrc()
        .rename(columns={"idrc": "value"})
        .pipe(add_iso_codes_column, id_column="donor_name", id_type="regex")
    ).drop(columns=["donor_name"])

    return idrc.pipe(
        pydeflate.deflate,
        base_year=2021,
        source="oecd_dac",
        id_column="iso_code",
        id_type="ISO3",
        date_column="year",
        target_col="value",
    )


def per_capita_idrc(
    historical_refugees: pd.DataFrame, reported_idrc_data: pd.DataFrame
) -> pd.DataFrame:
    """Calculate the per capita IDRC spending"""

    # Combine the datasets
    df = reported_idrc_data.merge(
        historical_refugees, on=["iso_code", "year"], suffixes=("_idrc", "_ref")
    )

    # Filter and calculate per capita
    return (
        df.loc[lambda d: d.year.isin(range(YEAR_START, YEAR_END + 1))]
        .groupby(["iso_code"], as_index=False)[["value_idrc", "value_ref"]]
        .sum(numeric_only=True)
        .assign(tot_cost_dfl=lambda d: round(d.value_idrc * 1e6 / d.value_ref, 1))
        .filter(["iso_code", "tot_cost_dfl"], axis=1)
    )


def update_refugee_cost_data() -> None:
    """Calculate the cost estimates per year. This assumes that
    the historical data and ukraine-specific data have been downloaded
    and updated"""

    # Read the historical data
    refugees = read_historical_unhcr_data(HIGH_LOW).pipe(filter_dac)

    # load IDRC data
    idrc = yearly_constant_idrc()

    # Get the per capita numbers
    idrc_per_capita = per_capita_idrc(refugees, idrc)

    # Get the latest Ukraine refugees data
    ukraine_data = read_ukriane_hcr_data().pipe(filter_dac)

    # Calculate the yearly spending on refugees
    summary = yearly_refugees_spending(
        cost_data=idrc_per_capita, refugee_data=ukraine_data
    )

    summary.to_csv(f"{PATHS.output}/ukraine_refugee_cost_estimates.csv", index=False)


def upload_ukraine_refugee_data() -> None:

    # Read the historical data
    refugees = read_historical_unhcr_data(HIGH_LOW).pipe(filter_dac)

    # load IDRC data
    idrc = yearly_constant_idrc()

    # Get the per capita numbers
    idrc_per_capita = per_capita_idrc(refugees, idrc)

    # Get the latest Ukraine refugees data
    ukraine_data = read_ukriane_hcr_data().pipe(filter_dac)

    # Calculate the yearly spending on refugees
    data = (
        yearly_refugees_spending(cost_data=idrc_per_capita, refugee_data=ukraine_data)
        .merge(idrc_per_capita, on=["iso_code"], how="left")
        .assign(donor_name=lambda d: coco.convert(d.iso_code, to="name_short"))
        .filter(
            ["iso_code", "donor_name", "total_refugees", "cost22", "cost23", "cost24"],
            axis=1,
        )
    )

    # Authenticate and load worksheet
    auth = _authenticate()
    wb = _get_workbook(auth, WORKBOOK_KEY)
    sheet = _get_worksheet(wb, WORKSHEET_KEY_NEW)

    # Upload data
    df2gsheet(data, sheet)


if __name__ == "__main__":
    update_refugee_cost_data()
    upload_ukraine_refugee_data()
