import pandas as pd

# --------------------------------------------------------------------------------------
#                               PARAMETERS
# --------------------------------------------------------------------------------------

BASE_URL = lambda url: (
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vSqAIxjSZ78fE93CP1K9K0"
    "t8rLM2wi0z_nc60ezrUeDEIOPz-vr01SmmS_5nNnq_uPE0dM26m0V3rQK/pub?"
    f"gid={url}&single=true&output=csv"
)

SHEETS: dict[str, int] = {
    "Australia": 1503888521,
    "Austria": 1619789998,
    "Belgium": 357504124,
    "Czech Republic": 1143510242,
    "Denmark": 1266971478,
    "Canada": 0,
    "European Union": 1464051755,
    "Finland": 2123695373,
    "France": 1098311333,
    "Germany": 410131162,
    "Greece": 891408109,
    "Hungary": 1163757006,
    "Iceland": 1921544973,
    "Ireland": 1546298383,
    "Italy": 105468234,
    "Luxembourg": 1280324309,
    "Japan": 1024177723,
    "Netherlands": 1597926940,
    "New Zealand": 956169089,
    "Norway": 1522549733,
    "Poland": 1598785617,
    "Portugal": 961044160,
    "South Korea": 2107825452,
    "Spain": 1108322023,
    "Sweden": 82579098,
    "Slovak Republic": 649145236,
    "Slovenia": 1793837304,
    "Switzerland": 2112782746,
    "United States": 1664889518,
    "United Kingdom": 626158716,
}

url = BASE_URL(1503888521)

COLUMNS: list = [
    "Date of Press Release/Source",
    "Text From Source",
    "Text for Popup (ODA)",
    "ODA (LCU)",
    "ODA (USD)",
    "Text for Popup (IDRC)",
    "IDRC (LCU)",
    "IDRC (USD)",
    "Source",
]


def get_data(pages_dict: dict) -> dict[str, pd.DataFrame]:
    """Get the data for all the pages passed through pages dict.
    Store in a dictionary and return"""

    # Empty dictionary to hold the data
    data = {}

    # Loop through countries to download csvs
    for country, page in pages_dict.items():
        _ = BASE_URL(page)
        data[country] = pd.read_csv(
            _,
            usecols=COLUMNS,
            parse_dates=["Date of Press Release/Source"],
            infer_datetime_format=True,
        )

    # Return the dictionary
    return data


def __read_rows(
    df_: pd.DataFrame, date_col: str, amount_col: str, text_col: str
) -> list[tuple]:
    """Read the rows in a given data frame and clean/format"""

    # List to store row contents
    rows = []

    for _, row in df_.iterrows():
        date = row[date_col]
        amount = row[amount_col] if str(row[amount_col]) != "NaT" else None
        text = row[text_col] if str(row[text_col]) != "NaT" else None
        source = row["Source"] if str(row["Source"]) != "NaT" else None
        try:
            date = (
                f"{date.strftime('%-d %B')}: "
                if isinstance(date, pd.Timestamp)
                else None
            )
        except ValueError:
            date = (
                f"{date.strftime('%#d %B')}: "
                if isinstance(date, pd.Timestamp)
                else None
            )

        amount = amount if str(amount) != "nan" else None
        text = text if str(text) != "nan" else None
        source = source if str(source) != "nan" else None
        source = source if source != "" else None
        source = (
            f"<a href='{source}' target='_blank'>{date.split(':')[0]}</a>"
            if not any([source is None, date is None])
            else None
        )

        cell = (amount, date, text, source)

        rows.append(cell)

    return rows


def _write_oda_pledged(data: dict[str, pd.DataFrame]):
    """Write the string for the oda cell"""

    args = {
        "date_col": "Date of Press Release/Source",
        "amount_col": "ODA (USD)",
        "text_col": "Text for Popup (ODA)",
    }

    return {country: __read_rows(df_=data_, **args) for country, data_ in data.items()}


def _write_idrc(data: dict[str, pd.DataFrame]):
    """Write the string for the idrc cell"""

    args = {
        "date_col": "Date of Press Release/Source",
        "amount_col": "IDRC (USD)",
        "text_col": "Text for Popup (IDRC)",
    }

    return {country: __read_rows(df_=data_, **args) for country, data_ in data.items()}


def _write_cell(country_data: list) -> tuple:
    """Write the content with each cell, adding the right syntax for Flourish popups
    to work"""
    amount = 0
    text = ">>"
    source = ""

    for row in country_data:
        amount += float(row[0]) if row[0] is not None else 0
        text += (
            f"<b>{row[1] if row[1] is not None else ''}</b>{row[2]}<br><br>"
            if row[2] is not None
            else ""
        )
        if (row[3] is not None) and (row[0] is not None):
            source += f"{row[3]}<br>"

    if amount == 0:
        amount = ""
    else:
        amount = f"{amount:,.1f}"

    if text == ">>":
        text = ""

    return f"{amount}{text}", source


def build_table(data: dict) -> pd.DataFrame:
    """Build the table for the Flourish visualisation"""

    # Get ODA pledged data
    oda_pledged = _write_oda_pledged(data)

    # Get IDRC data
    idrc = _write_idrc(data)

    # Create an empty data frame
    df = pd.DataFrame()

    # For each donor, create the table data
    for donor in data:

        p = _write_cell(oda_pledged[donor])
        i = _write_cell(idrc[donor])

        source = ""

        for col in [p, i]:
            source += col[1]

        data = {
            "Estimated ODA pledged to Ukraine (USD millions)": p[0],
            "Estimated Ukraine in-donor refugee costs (USD millions)": i[0],
            "Source": source,
        }

        _ = pd.DataFrame([data]).assign(Donor=donor)
        df = pd.concat([df, _], ignore_index=True)

        df = df.filter(["Donor"] + list(data), axis=1)

        df["amount2"] = df[
            "Estimated Ukraine in-donor refugee costs (USD millions)"
        ].apply(
            lambda r: pd.to_numeric(r.split(">>")[0].replace(",", ""), errors="coerce")
        )

    return (
        df.sort_values(
            by=[
                "amount2",
            ],
            ascending=False,
        )
        .drop(columns=["amount2"])
        .loc[
            lambda d: (d["Estimated ODA pledged to Ukraine (USD millions)"] != "")
            | (d["Estimated Ukraine in-donor refugee costs (USD millions)"] != "")
            | (d["Source"] != ""),
        ]
    )


if __name__ == "__main__":
    raw_data = get_data(pages_dict=SHEETS)
    table_data = build_table(raw_data)
