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
    "Denmark": 1266971478,
    "Canada": 0,
    "European Union": 1464051755,
    "Finland": 2123695373,
    "France": 1098311333,
    "Ireland": 1546298383,
    "Italy": 105468234,
    "Japan": 1024177723,
    "Netherlands": 1597926940,
    "New Zealand": 956169089,
    "Norway": 1522549733,
    "Portugal": 961044160,
    "South Korea": 2107825452,
    "Spain": 1108322023,
    "Sweden": 82579098,
    "United States": 626158716,
    "United Kingdom": 1664889518,
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
    "Text for Popup (Diverted)",
    "ODA Diverted (LCU)",
    "ODA Diverted (USD)",
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

    rows = []

    for _, row in df_.iterrows():
        date = row[date_col]
        amount = row[amount_col] if str(row[amount_col]) != "NaT" else None
        text = row[text_col] if str(row[text_col]) != "NaT" else None
        source = row["Source"] if str(row["Source"]) != "NaT" else None

        date = (
            f"{date.strftime('%-d %B')}: " if isinstance(date, pd.Timestamp) else None
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


def _write_oda_diverted(data: dict[str, pd.DataFrame]):
    """Write the string for the oda diverted cell"""

    args = {
        "date_col": "Date of Press Release/Source",
        "amount_col": "ODA Diverted (USD)",
        "text_col": "Text for Popup (Diverted)",
    }

    return {country: __read_rows(df_=data_, **args) for country, data_ in data.items()}


def _write_cell(country_data: list) -> tuple:

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

    if source[-4:] == "<br>":
        source = source[:-4]

    if amount == 0:
        amount = ""

    if text == ">>":
        text = ""

    return f"{amount}{text}", source


def build_table(data: dict) -> pd.DataFrame:

    oda_pledged = _write_oda_pledged(data)
    idrc = _write_idrc(data)
    oda_diverted = _write_oda_diverted(data)

    df = pd.DataFrame()

    for donor in data:

        p = _write_cell(oda_pledged[donor])
        i = _write_cell(idrc[donor])
        d = _write_cell(oda_diverted[donor])

        source = ""

        for col in [p, i, d]:
            source += col[1]

        data = {
            "Cumulative ODA pledged to Ukraine (USD millions)": p[0],
            "Cumulative In-donor Refugee Costs (USD millions)": i[0],
            "Total ODA diverted from current budget (USD millions)": d[0],
            "Source": source,
        }

        _ = pd.DataFrame([data]).assign(Donor=donor)
        df = pd.concat([df, _], ignore_index=True)

        df = df.filter(["Donor"] + list(data), axis=1)

    return df


if __name__ == "__main__":

    raw_data = get_data(pages_dict=SHEETS)
    df = build_table(raw_data)
