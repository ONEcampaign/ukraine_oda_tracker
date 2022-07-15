import pandas as pd
from scripts import config


def read_oda():
    return (
        pd.read_csv(f"{config.PATHS.data}/total_oda_current.csv")
        .filter(["year", "donor_name", "value"], axis=1)
        .rename(columns={"value": "total_oda"})
    )


def read_idrc():
    return (
        pd.read_csv(f"{config.PATHS.data}/total_idrc_current.csv")
        .filter(["year", "donor_name", "value"], axis=1)
        .rename(columns={"value": "idrc"})
    )


def idrc_as_share():

    idrc = read_idrc()
    oda = read_oda()

    return (
        idrc.merge(oda, on=["year", "donor_name"])
        .assign(share=lambda d: round(100 * d.idrc / d.total_oda, 1))
        .filter(["year", "donor_name", "share"], axis=1)
        .rename(columns={"donor_name": "Donor"})
    )


if __name__ == "__main__":
    share = idrc_as_share()
    share.to_csv(config.PATHS.output + "/idrc_share.csv", index=False)
