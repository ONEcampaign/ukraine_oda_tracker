import pandas as pd
import pydeflate

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

    df = (
        idrc.merge(oda, on=["year", "donor_name"])
        .assign(share=lambda d: round(100 * d.idrc / d.total_oda, 5))
        .rename(columns={"donor_name": "Donor"})
    )

    dac = df.groupby(["year"], as_index=False).sum().drop("share", axis=1)
    dac["share"] = round(100 * dac.idrc / dac.total_oda, 5)
    dac["Donor"] = "DAC Countries, Total"

    return pd.concat([dac, df], ignore_index=True)


def idrc_constant_wide():

    idrc = read_idrc()

    idrc_constant = pydeflate.deflate(
        df=idrc.copy(deep=True),
        base_year=2021,
        source="oecd_dac",
        id_column="donor_name",
        id_type="regex",
        date_column="year",
        source_col="idrc",
        target_col="idrc",
    )

    dac_total = (idrc_constant.groupby(["year"], as_index=False)["idrc"].sum()).assign(
        donor_name="DAC Countries, Total"
    )

    idrc_constant = pd.concat(
        [dac_total, idrc_constant], ignore_index=True
    ).sort_values(["idrc"], ascending=False)

    order = idrc_constant.donor_name.unique()

    return (
        idrc_constant.pivot(index="year", columns="donor_name", values="idrc")
        .filter(order, axis=1)
        .reset_index()
    )


if __name__ == "__main__":
    share = idrc_as_share()
    share.to_csv(config.PATHS.output + "/idrc_share.csv", index=False)

    idrc_constant = idrc_constant_wide()
    idrc_constant.to_csv(config.PATHS.output + "/idrc_constant.csv", index=False)
