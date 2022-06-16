from scripts.create_table import get_data, SHEETS, build_table
from scripts.config import PATHS
from datetime import datetime
from csv import writer


def last_updated():
    """Appends the date of last run to a csv"""

    with open(PATHS.output + r"/updates.csv", "a+", newline="") as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow([datetime.today()])


if __name__ == "__main__":
    raw_data = get_data(pages_dict=SHEETS)
    df = build_table(raw_data)
    df.to_csv(f"{PATHS.output}/table.csv", index=False)
    last_updated()
