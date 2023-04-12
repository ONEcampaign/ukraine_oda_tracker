import pandas as pd
from bblocks.dataframe_tools.add import add_iso_codes_column
from country_converter import country_converter
from oda_data import ODAData, set_data_path, download_dac1
from oda_data.tools.groupings import donor_groupings
from pydeflate import deflate, set_pydeflate_path


from scripts.config import PATHS

# set the data path
set_data_path(PATHS.raw_data)
set_pydeflate_path(PATHS.raw_data)


def __export_df_page(
    page: int,
    page_countries: list,
    idrc: pd.DataFrame,
    oda: pd.DataFrame,
    gni: pd.DataFrame,
) -> None:
    """Helper function to export the individual pages"""
    _ = (
        idrc.merge(oda, on=["year", "donor_name"], how="outer")
        .merge(gni, on=["year", "donor_name"], how="outer")
        .sort_values(["year", "donor_name"])
        .loc[lambda d: d.donor_name.isin(page_countries)]
        .reset_index(drop=True)
        .assign(
            idrc_gni=lambda d: round(100 * d.idrc.fillna(0) / d.gni, 3),
            oda_gni=lambda d: round(100 * d.total_oda.fillna(0) / d.gni, 2),
            year=lambda d: d.year.astype("Int32"),
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
        .filter(
            [
                "year",
                "Donor",
                "In-Donor Refugee Costs",
                "Total ODA",
                "GNI",
                "IDRC as a share of GNI",
                "ODA as a share of GNI",
            ],
            axis=1,
        )
    )
    _.loc[
        lambda d: d.year >= 2022,
        ["Total ODA", "GNI", "ODA as a share of GNI", "IDRC as a share of GNI"],
    ] = pd.NA

    _.to_csv(PATHS.output / f"idrc_oda_chart_{page}.csv", index=False)


def update_oda() -> None:
    """Update the ODA data from the raw_data folder"""
    from oda_data import ODAData

    oda = ODAData(
        years=[2022],
        donors=list(donor_groupings()["dac_countries"]) + [84],
        include_names=True,
    )

    oda.load_indicator(["total_oda_ge"])

    df = oda.get_data()

    df.to_csv(PATHS.output / "latest_oda.csv", index=False)


def update_total_oda_data() -> None:
    from oda_data import ODAData

    oda = ODAData(
        years=range(2010, 2023),
        donors=list(donor_groupings()["dac_countries"]) + [84],
        include_names=True,
    )

    oda.load_indicator(["total_oda_official_definition"])

    df = oda.get_data().filter(["year", "donor_name", "value"], axis=1)

    df.to_csv(PATHS.raw_data / "total_oda_current.csv", index=False)


def read_oda():
    """Read ODA data from raw_data folder. This data contains flows up to 2017 and
    grant equivalents from 2018 onwards. It is in current prices"""
    return (
        pd.read_csv(PATHS.raw_data / "total_oda_current.csv")
        .filter(["year", "donor_name", "value"], axis=1)
        .rename(columns={"value": "total_oda"})
        .assign(
            donor_name=lambda d: country_converter.convert(
                d.donor_name, to="short_name"
            )
        )
    )


def _raw_oda_data(indicator: str) -> pd.DataFrame:
    """Read the data for a specific indicator"""
    dac_donors = donor_groupings()["dac_countries"] | {84: "Lithuania"}

    # Instantiate the ODAData class
    oda = ODAData(years=range(2010, 2024), donors=list(dac_donors), include_names=True)

    # Get the IDRC data
    return (
        oda.load_indicator(indicator)
        .get_data()
        .filter(["year", "donor_name", "value"], axis=1)
    )


def _create_idrc_data() -> None:
    """Create the IDRC data export from DAC1 using oda_data"""

    df = _raw_oda_data(indicator="idrc_flow").rename(columns={"value": "idrc"})

    # Export the data
    df.to_csv(PATHS.raw_data / "total_idrc_current.csv", index=False)


def read_idrc():
    """Read IDRC data from raw_data folder. This data comes from Table 1 from OECD DAC"""
    return pd.read_csv(PATHS.raw_data / "total_idrc_current.csv").assign(
        donor_name=lambda d: country_converter.convert(d.donor_name, to="short_name")
    )


def _create_gni_data() -> None:
    """Create the GNI data export from DAC1 using oda_data"""

    df = _raw_oda_data(indicator="gni").rename(columns={"value": "gni"})

    # Export the data
    df.to_csv(PATHS.raw_data / "gni.csv", index=False)


def read_gni():
    """Read GNI data from raw_data folder. This data comes from Table 1 from OECD DAC"""
    return pd.read_csv(PATHS.raw_data / "gni.csv").assign(
        donor_name=lambda d: country_converter.convert(d.donor_name, to="short_name")
    )


def _pop_groups(list_: list, group_size: int) -> tuple[list, ...]:
    """Split a list into groups of size group_size"""
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


def read_refugee_cost_data() -> pd.DataFrame:
    """Read the saved refugee cost data"""
    return pd.read_csv(PATHS.output / "ukraine_refugee_cost_estimates.csv")


def idrc_oda_chart() -> None:
    """Build the CSVs used by the ODA IDRC chart"""

    # Read the different datasets that are needed for the chart
    idrc_est = (
        read_refugee_cost_data()
        .drop(["total_refugees"], axis=1)
        .rename(columns={"cost22": 2022, "cost23": 2023, "cost24": 2024})
        .melt(id_vars=["iso_code"], var_name="year", value_name="idrc")
        .assign(idrc=lambda d: d.idrc / 1e6)
    )
    idrc_hist = (
        read_idrc()
        .pipe(add_iso_codes_column, id_column="donor_name", id_type="regex")
        .filter(["iso_code", "year", "idrc"], axis=1)
    )

    idrc_latest = idrc_hist.query("year == year.max()").drop("year", axis=1)

    # Add the latest IDRC data to the estimated data
    idrc_est = (
        idrc_est.merge(idrc_latest, on="iso_code", how="left", suffixes=("", "_latest"))
        .assign(
            idrc=lambda d: d.apply(
                lambda x: x.idrc + x.idrc_latest if x.idrc > 1 else 0, axis=1
            )
        )
        .drop("idrc_latest", axis=1)
    ).loc[lambda d: d.year > 2022]

    # Combine the historical and estimated data
    idrc = pd.concat([idrc_hist, idrc_est], ignore_index=True).assign(
        idrc=lambda d: d.idrc.apply(lambda x: x if x > 1 else pd.NA)
    )

    # add the donor names
    idrc = idrc.assign(
        donor_name=lambda d: country_converter.convert(d.iso_code, to="name_short")
    ).drop("iso_code", axis=1)

    # Read the other datasets
    oda = read_oda().assign(
        donor_name=lambda d: country_converter.convert(d.donor_name, to="name_short")
    )
    gni = read_gni().assign(
        donor_name=lambda d: country_converter.convert(d.donor_name, to="name_short")
    )

    # Assign the 2021 GNI value to 2022, 2023 and 2024
    dfs = [
        gni.copy(deep=True).loc[lambda d: d.year == 2022].assign(year=y)
        for y in [2023, 2024]
    ]
    gni = pd.concat([gni, *dfs], ignore_index=True)

    # Filter and sort the dataframes
    idrc, oda, gni = [
        d.astype({"year": "Int32"})
        .loc[d.year.isin([2012, 2016, 2021, 2022, 2023, 2024])]
        .sort_values(["year", "donor_name"])
        .reset_index(drop=True)
        for d in [idrc, oda, gni]
    ]

    # Sort the IDRC data frame in order for the pages to go from the highest spender to lowest
    idrc = idrc.sort_values(["year", "idrc"], ascending=(True, False))

    p1_countries = [
        "Canada",
        "United States",
        "France",
        "Germany",
        "Italy",
        "United Kingdom",
    ]

    # Create the groupings for the chart pages
    other_countries = [c for c in idrc.donor_name.unique() if c not in p1_countries]
    chart_pages = _pop_groups(other_countries, 6)
    chart_pages = [p1_countries] + [*chart_pages]

    for page_, list_ in enumerate(chart_pages):
        __export_df_page(page=page_, page_countries=list_, idrc=idrc, oda=oda, gni=gni)

    print("Exported data for ODA/IDRC charts (pages)")


def idrc_as_share():
    """Build the CSV used by the IDRC as a share of GNI chart"""

    # Read the different datasets that are needed for the chart
    idrc = read_idrc()
    oda = read_oda()

    # Merge the dataframes and create the share column
    df = (
        idrc.merge(oda, on=["year", "donor_name"])
        .assign(share=lambda d: round(100 * d.idrc / d.total_oda, 5))
        .rename(columns={"donor_name": "Donor"})
    )

    dac = (
        df.groupby(["year"], as_index=False)
        .sum(numeric_only=True)
        .drop("share", axis=1)
    )
    dac["share"] = round(100 * dac.idrc / dac.total_oda, 5)
    dac["Donor"] = "DAC Countries, Total"

    data = pd.concat([dac, df], ignore_index=True)

    data.to_csv(PATHS.output / "idrc_share.csv", index=False)
    print("Exported data for IDRC as a share")


def idrc_constant_wide() -> None:
    """Build the CSV used by the IDRC constant prices chart"""

    # Read the different datasets that are needed for the chart
    idrc_est = (
        read_refugee_cost_data()
        .drop(["total_refugees"], axis=1)
        .rename(columns={"cost22": 2022, "cost23": 2023, "cost24": 2024})
        .melt(id_vars=["iso_code"], var_name="year", value_name="idrc")
        .assign(idrc=lambda d: d.idrc / 1e6)
    )
    idrc_hist = (
        read_idrc()
        .pipe(add_iso_codes_column, id_column="donor_name", id_type="regex")
        .filter(["iso_code", "year", "idrc"], axis=1)
    )

    # Deflate to 2021 prices
    idrc_hist = deflate(
        df=idrc_hist.copy(deep=True),
        base_year=2022,
        deflator_source="oecd_dac",
        deflator_method="dac_deflator",
        exchange_source="oecd_dac",
        exchange_method="implied",
        id_column="iso_code",
        id_type="ISO3",
        date_column="year",
        source_column="idrc",
        target_column="idrc",
    )

    idrc_latest = idrc_hist.query("year == year.max()").drop("year", axis=1)

    # Add the latest IDRC data to the estimated data
    idrc_est = (
        idrc_est.merge(idrc_latest, on="iso_code", how="left", suffixes=("", "_latest"))
        .assign(
            idrc=lambda d: d.apply(
                lambda x: x.idrc + x.idrc_latest if x.idrc > 1 else 0, axis=1
            )
        )
        .drop("idrc_latest", axis=1)
    ).loc[lambda d: d.year > 2022]

    # Combine the historical and estimated data
    idrc = pd.concat([idrc_hist, idrc_est], ignore_index=True).assign(
        idrc=lambda d: d.idrc.apply(lambda x: x if x > 0.0001 else pd.NA)
    )

    # add the donor names
    idrc = idrc.assign(
        donor_name=lambda d: country_converter.convert(d.iso_code, to="name_short")
    ).drop("iso_code", axis=1)

    # Calculate dac total
    dac_total = (idrc.groupby(["year"], as_index=False)["idrc"].sum()).assign(
        donor_name="DAC Countries, Total"
    )

    # Merge with the original dataframe
    idrc_constant = (
        pd.concat([dac_total, idrc], ignore_index=True)
        .sort_values(["idrc"], ascending=False)
        .astype({"year": "Int32"})
    )

    order = idrc_constant.donor_name.unique()

    idrc_constant = idrc_constant.sort_values(
        ["donor_name", "year"], ascending=[True, True]
    )

    data = (
        idrc_constant.pivot(index="year", columns="donor_name", values="idrc")
        .filter(order, axis=1)
        .reset_index()
        .loc[lambda d: d.year >= 2012]
    )

    data.to_csv(PATHS.output / "idrc_over_time_constant.csv", index=False)
    print("IDRC over time constant prices CSV created (wide)")


if __name__ == "__main__":
    update_oda()
    _create_idrc_data()
    _create_gni_data()
    idrc_as_share()
    idrc_oda_chart()
    idrc_constant_wide()
    update_total_oda_data()
