import country_converter as coco
import pandas as pd
import pydeflate
from bblocks.import_tools.unzip import read_zipped_csv

from scripts.config import PATHS
from scripts.oda_data import read_idrc

from bblocks.dataframe_tools.add import add_iso_codes_column


HIGH_LOW = "high"
YEAR_START = 2018
YEAR_END = 2021


def get_unhcr_data(low_or_high: str):
    """ """
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

    return (
        df[df.app_type.isin(f_)]
        .groupby(["year", "iso_code"], as_index=False)
        .sum(numeric_only=True)
    )


def filter_dac(df: pd.DataFrame):
    from scripts.create_table import SHEETS

    dac = (
        pd.DataFrame(SHEETS.keys())
        .assign(iso_code=lambda d: coco.convert(d[0], to="ISO3", not_found=None))
        .loc[lambda d: d.iso_code != "European Union"]
    )

    return df[df.iso_code.isin(dac.iso_code)].set_index("iso_code")


def read_hcr_data() -> pd.DataFrame:
    """Read the locally saved HCR data"""

    return pd.read_csv(f"{PATHS.output}/hcr_data.csv").rename(
        columns={
            "Individual refugees from Ukraine recorded across Europe": "value",
            "Country": "country",
            "Data Date": "date",
        }
    )


def get_refugees():
    url = (
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vSqAIxjSZ78fE93CP1K9K0t8rL"
        "M2wi0z_nc60ezrUeDEIOPz-vr01SmmS_5nNnq_uPE0dM26m0V3rQK/pub?"
        "gid=1604958206&single=true&output=csv"
    )
    df = pd.read_csv(url).iloc[:, [1, 3]]
    df.columns = ["iso_code", "refugees"]
    df.refugees = df.refugees.str.replace(",", "").astype(float)

    return df


def yearly_refugees_spending(
    cost_data: pd.DataFrame, refugee_data: pd.DataFrame
) -> pd.DataFrame:

    data = refugee_data.merge(cost_data, on=["iso_code"], how="left")

    data = data.assign(
        cost22=lambda d: d["difference"] * d.ratio22 * d.tot_cost_dfl,
        cost23=lambda d: d["difference"] * d.ratio23 * d.tot_cost_dfl,
        cost24=lambda d: d["difference"] * d.ratio24 * d.tot_cost_dfl,
    )

    return data.groupby(["iso_code"], as_index=False)[
        ["difference", "cost22", "cost23", "cost24"]
    ].sum(numeric_only=True)


def pipeline():
    """Run the full analysis"""

    # load refugees data
    refugees = get_unhcr_data(HIGH_LOW).pipe(filter_dac)

    # load IDRC data
    idrc = (
        read_idrc()
        .rename(columns={"idrc": "value"})
        .pipe(add_iso_codes_column, id_column="donor_name", id_type="regex")
    ).drop(columns=["donor_name"])

    # load and deflate idrc data
    idrc = idrc.pipe(
        pydeflate.deflate,
        base_year=2021,
        source="oecd_dac",
        id_column="iso_code",
        id_type="ISO3",
        date_column="year",
        target_col="value",
    )

    # combine the idrc and refugees data
    df = idrc.merge(refugees, on=["iso_code", "year"], suffixes=("_idrc", "_ref"))

    # Filter and calculate per capita
    df = (
        df.loc[lambda d: d.year.isin(range(YEAR_START, YEAR_END + 1))]
        .groupby(["iso_code"], as_index=False)[["value_idrc", "value_ref"]]
        .sum(numeric_only=True)
        .assign(tot_cost_dfl=lambda d: round(d.value_idrc * 1e6 / d.value_ref, 1))
        .filter(["iso_code", "tot_cost_dfl"], axis=1)
    )

    refugee_data = read_hcr_data().pipe(filter_dac)

    # Calculate estimated yearly costs
    return yearly_refugees_spending(cost_data=df, refugee_data=refugee_data).rename(
        columns={"difference": "refugees"}
    )


if __name__ == "__main__":
    data = pipeline()
    ...
