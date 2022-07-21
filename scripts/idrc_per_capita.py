import country_converter as coco
import pandas as pd
import pydeflate
from bblocks.import_tools.unzip import read_zipped_csv

from scripts.config import PATHS

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

    return df[df.app_type.isin(f_)].groupby(["year", "iso_code"], as_index=False).sum()


def filter_dac(df: pd.DataFrame):
    from scripts.create_table import SHEETS

    dac = (
        pd.DataFrame(SHEETS.keys())
        .assign(iso_code=lambda d: coco.convert(d[0], to="ISO3", not_found=None))
        .loc[lambda d: d.iso_code != "European Union"]
    )

    return df[df.iso_code.isin(dac.iso_code)].set_index("iso_code")


def get_idrc():
    return (
        pd.read_csv(f"{PATHS.data}/total_idrc_current.csv")
        .assign(iso_code=lambda d: coco.convert(d.donor_name, to="ISO3"))
        .filter(["iso_code", "year", "value"], axis=1)
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


def pipeline():
    """Run the full analysis"""

    # load refugees data
    refugees = get_unhcr_data(HIGH_LOW).pipe(filter_dac)

    # load and deflate idrc data
    idrc = get_idrc().pipe(
        pydeflate.deflate,
        base_year=2021,
        source="oecd_dac",
        date_column="year",
        target_col="value",
    )

    df = idrc.merge(refugees, on=["iso_code", "year"], suffixes=["_idrc", "_ref"])

    df = (
        df.loc[lambda d: d.year.isin(range(YEAR_START, YEAR_END))]
        .groupby(["iso_code"], as_index=False)[["value_idrc", "value_ref"]]
        .sum()
        .assign(tot_cost_dfl=lambda d: round(d.value_idrc * 1e6 / d.value_ref, 1))
        .merge(get_refugees(), on=["iso_code"])
        .assign(
            estimated_costs=lambda d: d.tot_cost_dfl * d.refugees,
            country_name=lambda d: coco.convert(d.iso_code, to="short_name"),
        )
    )
    return df


if __name__ == "__main__":
    data = pipeline()
    pass
