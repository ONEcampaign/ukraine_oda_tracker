from csv import writer
from datetime import datetime

from scripts.config import PATHS
from scripts.dt_table import live_dt_table_pipeline
from scripts.idrc_per_capita import export_summary_cost_data, update_refugee_cost_data
from scripts.oda import idrc_as_share, idrc_constant_wide, idrc_oda_chart, update_oda
from scripts.unhcr_data import update_ukraine_hcr_data


def last_updated():
    """Appends the date of last run to a csv"""

    with open(PATHS.output / "updates.csv", "a+", newline="") as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow([datetime.today()])


def update_daily():
    """Charts to update every week"""

    # Update Ukraine refugees data
    update_ukraine_hcr_data()

    # Update IDRC estimates charts
    idrc_as_share()

    # Update IDRC ODA chart
    idrc_oda_chart()

    # Update IDRC constant chart
    idrc_constant_wide()

    # Update donor tracker table
    live_dt_table_pipeline()

    # Export summary cost data
    export_summary_cost_data()


def update_weekly():
    """Charts to update every week"""
    # update historical refugee estimates
    update_refugee_cost_data()

    # update monthly oda
    update_oda()

    # Update last updated date
    last_updated()


if __name__ == "__main__":
    update_daily()
    update_weekly()
