import pandas as pd
import pydeflate
from country_converter import country_converter

from scripts import config


def read_idrc() -> pd.DataFrame:
    """read idrc data saved on disk"""
    return pd.read_csv(f"{config.PATHS.scripts}/dac_idrc_constant.csv")


def read_oda() -> pd.DataFrame:
    """read oda data saved on disk"""
    return pd.read_csv(f"{config.PATHS.scripts}/oda_current.csv")


def idrc_flourish():
    return (
        read_idrc()
        .loc[lambda d: d.prices == "constant"]
        .loc[lambda d: d.year >= 2012]
        .filter(["year", "donor", "value"], axis=1)
        .pivot(index="year", columns="donor", values="value")
    )


def idrc_as_share():
    idrc = (
        read_idrc()
        .loc[lambda d: d.year >= 2014]
        .pipe(
            pydeflate.deflate,
            base_year=2021,
            source="oecd_dac",
            id_column="donor",
            id_type="DAC",
            source_col="value",
            target_col="value",
            date_column="year",
        )
        .filter(["year", "donor", "value"], axis=1)
        .rename(columns={"value": "idrc"})
        .assign(iso_code=lambda d: country_converter.convert(d.donor, to="ISO3"))
        .loc[lambda d: d.iso_code != "not found"]
    )

    oda = (
        read_oda()
        .filter(["year", "donor_name", "value"], axis=1)
        .rename(columns={"value": "oda"})
        .pipe(
            pydeflate.deflate,
            base_year=2021,
            source="oecd_dac",
            id_column="donor_name",
            id_type="DAC",
            source_col="oda",
            target_col="oda",
            date_column="year",
        )
        .assign(iso_code=lambda d: country_converter.convert(d.donor_name, to="ISO3"))
    )

    data = (
        idrc.merge(oda, on=["iso_code", "year"], how="left")
        .assign(share=lambda d: round(100 * d.idrc / d.oda, 2))
        .filter(["year", "donor", "share"], axis=1)
        .pivot(index="year", columns="donor", values="share")
    )


if __name__ == "__main__":
    pass
    # idrc_flourish()
