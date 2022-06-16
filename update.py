from scripts.create_table import get_data, SHEETS, build_table
from scripts.config import PATHS


if __name__ == "__main__":
    raw_data = get_data(pages_dict=SHEETS)
    df = build_table(raw_data)
    df.to_csv(f"{PATHS.output}/table.csv", index=False)
