import pandas as pd
import pydeflate
from country_converter import country_converter

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


def read_gni():
    return (
        pd.read_csv(f"{config.PATHS.data}/gni.csv")
        .filter(["year", "donor_name", "value"], axis=1)
        .rename(columns={"value": "gni"})
    )


def read_idrc_estimates():
    url = (
        "https://docs.google.com/spreadsheets/d/e/2PACX-1vSqAIxjSZ78fE93CP1K9K0t8rL"
        "M2wi0z_nc60ezrUeDEIOPz-vr01SmmS_5nNnq_uPE0dM26m0V3rQK/pub?gid=1887834724"
        "&single=true&output=csv"
    )
    return (
        pd.read_csv(url)
        .assign(
            donor_name=lambda d: country_converter.convert(d.iso_code, to="short_name"),
            year=2022,
        )
        .rename(columns={"estimated_idrc": "idrc"})
        .drop("iso_code", axis=1)
    )


def _pop_groups(list_: list, group_size: int) -> tuple[list, ...]:
    from collections import deque

    list_ = deque(list_)
    groups = []
    while len(list_) > 0:
        if len(list_) >= group_size:
            countries = [list_.popleft() for i in range(group_size)]
        else:
            countries = [list_.popleft() for i in range(len(list_))]

        groups.append(countries)

    return tuple(groups)


def idrc_oda_chart() -> None:

    # Read the different datasets that are needed for the chart
    idrc_est = read_idrc_estimates()
    idrc = pd.concat([read_idrc(), idrc_est], ignore_index=True)
    oda = read_oda()
    gni = read_gni()

    # Assign the 2021 GNI value to 2022
    gni22 = gni.copy(deep=True).loc[lambda d: d.year == 2021].assign(year=2022)
    gni = pd.concat([gni, gni22], ignore_index=True)

    idrc, oda, gni = [
        d.loc[d.year.isin([2012, 2016, 2021, 2022])]
        .sort_values(["year", "donor_name"])
        .reset_index(drop=True)
        for d in [idrc, oda, gni]
    ]

    p1_countries = [
        "Canada",
        "United States",
        "France",
        "Germany",
        "Italy",
        "United Kingdom",
    ]

    other_countries = [c for c in idrc.donor_name.unique() if c not in p1_countries]

    chart_pages = _pop_groups(other_countries, 6)
    chart_pages = [p1_countries] + [*chart_pages]

    def __export_df_page(page: int, page_countries: list):
        _ = (
            idrc.merge(oda, on=["year", "donor_name"], how="outer")
            .merge(gni, on=["year", "donor_name"], how="outer")
            .sort_values(["year", "donor_name"])
            .loc[lambda d: d.donor_name.isin(page_countries)]
            .reset_index(drop=True)
            .assign(
                idrc_gni=lambda d: round(100 * d.idrc / d.gni, 3),
                oda_gni=lambda d: round(100 * d.total_oda / d.gni, 2),
            )
            .rename(
                columns={
                    "donor_name": "Donor",
                    "idrc": "In-Donor Refugee Costs",
                    "total_oda": "Total ODA",
                    "idrc_gni": "IDRC as a share of GNI",
                    "oda_gni": "ODA as a share of GNI",
                    "gni": "GNI",
                }
            )
        ).to_csv(f"{config.PATHS.output}/idrc_oda_chart_{page}.csv", index=False)

    for page_, list_ in enumerate(chart_pages):
        __export_df_page(page=page_, page_countries=list_)


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

    idrc_const = idrc_constant_wide()
    idrc_const.to_csv(config.PATHS.output + "/idrc_constant.csv", index=False)

    idrc_oda_chart()
