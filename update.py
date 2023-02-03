from scripts.create_table import get_data, SHEETS, build_table
from scripts.idrc_per_capita import update_refugee_cost_data, upload_ukraine_refugee_data
from scripts.oda import idrc_as_share, idrc_constant_wide, idrc_oda_chart
from scripts.unhcr_data import update_ukraine_hcr_data
from scripts.config import PATHS
from datetime import datetime
from csv import writer


def last_updated():
    """Appends the date of last run to a csv"""

    with open(f"{PATHS.output}/updates.csv", "a+", newline="") as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow([datetime.today()])


if __name__ == "__main__":
    # Update Ukraine refugees data
    update_ukraine_hcr_data()

    # Update refugee cost calculations
    update_refugee_cost_data()
    # push updates to google sheets
    upload_ukraine_refugee_data()

    # Update IDRC estimates charts
    share = idrc_as_share()
    share.to_csv(PATHS.output + "/idrc_share.csv", index=False)

    # Update IDRC constant chart
    idrc_const = idrc_constant_wide()
    idrc_const.to_csv(PATHS.output + "/idrc_constant.csv", index=False)

    # Update IDRC ODA chart
    idrc_oda_chart()

    # Build table
    raw_data = get_data(pages_dict=SHEETS)
    df = build_table(raw_data)
    df.to_csv(f"{PATHS.output}/table.csv", index=False)

    # Update last updated date
    last_updated()
