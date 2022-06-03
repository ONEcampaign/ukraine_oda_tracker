from scripts import sdr_tracker, config
from csv import writer
from datetime import datetime


def last_updated():
    """Appends the date of last run to a csv"""

    with open(config.paths.output + r"/updates.csv", "a+", newline="") as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow([datetime.today()])


if __name__ == "__main__":
    # create map template for Africa
    sdr_tracker.create_africa_map_template()

    # create flourish csv
    sdr_tracker.create_sdr_map()

    # append run time
    last_updated()

    print("Successfully updated SDRs Tracker")
