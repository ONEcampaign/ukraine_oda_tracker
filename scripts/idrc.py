import pandas as pd

from scripts import config


def read_idrc() -> pd.DataFrame:
    """read idrc data saved on disk"""
    return pd.read_csv(f"{config.PATHS.scripts}/idrc.csv")


def idrc_flourish():
    return (
        read_idrc()
        .loc[lambda d: d.prices == "constant"]
        .loc[lambda d: d.year>=2012]
        .filter(["year", "donor", "value"], axis=1)
        .pivot(index="year", columns="donor", values="value")
    )


if __name__ == "__main__":

    idrc_flourish()
