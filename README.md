# **Ukraine ODA Tracker**
This is a joint project from [The ONE Campaign](https://one.org/) and [SEEK's Donor Tracker](https://donortracker.org/).

Russia’s invasion of Ukraine has sparked the biggest war in Europe since WWII.
It has cost thousands of lives, caused millions of people to flee Ukraine, 
and has set off a global food crisis.

Our estimates show that in-donor refugee costs alone amounted to $35bn in 2022 and over this amount again in 2023.
Other humanitarian and financial support committed to Ukraine will further drive up totals.
The world will not be able to  meet the extraordinary needs from these compounding crises without
increasing aid budgets significantly.

## Methodology

### In-donor refugee costs
We estimate average in-donor refugee costs per refugee per year by dividing the total ODA eligible in-donor refugee 
costs reported by a donor country by the reported number of asylum applications over the last four years (2018-2021).
The 2018-2021 timeframe is chosen to reflect recent reporting practises, after the clarifications on reporting
guidelines by the DAC Temporary Working Group on Refugees and Migration. 


To estimate 2022, 2023 and 2024 in-donor refugee costs for Ukrainian refugees, we multiply our estimated average in-donor refugee cost 
for each specific donor by the number of Ukrainian refugees reported to be in that country. We track net arrivals each month to account 
for the varying arrival dates of refugees, such that the 12-months of ODA-eligible costs is spread more accurately across 2022, 2023 and 2024. 
For European countries, data on `individual refugees from Ukraine recorded` comes from
[UNHCR’s Operational Data Portal for Ukrainian Refugees](https://data.unhcr.org/en/situations/ukraine). 
For countries without data via UNHCR, we use the reported number of refugees within a country from official government 
websites or statements. 

For a more detailed methodology, please see [here](https://docs.google.com/document/d/1fLC3RFjmxIlgh7CRCKlGetufJ0oy7Q9u2RqlDupOd5M/edit?usp=sharing). 

### Data sources

#### In-donor refugee costs
Sourced from the [OECD DAC1 table](https://stats.oecd.org/Index.aspx?DataSetCode=Table1).

We deflate these figures into constant 2021 prices for comparability across the four years.

#### Refugees
Sourced from the [UNHCR Refugee Statistics Data Portal](https://www.unhcr.org/data.html). We include applications to 
all authority types and across all stages of application.

We include all asylum application types because donors can report in-donor refugee 
costs up to 12-months as ODA, so only including new applications would risk excluding asylum seekers receiving 
ODA-eligible funding. We include both Persons and Cases, with Cases reported only when the level of disaggregation for
person by person is not available.  

#### Notes
Australia did not report in-donor refugee costs to the OECD over the four-year timeframe. Japan is missing asylum
application data for 2021, reducing the used time period to 2018-2020. Luxembourg uses a two-year average (2019-2020) 
due to missing in-donor refugee costs data.


## Repository Structure and Information

This repository contains data and scripts to reproduce the analysis and create the csv file powering the 
flourish visualization for the tracker. 

Our most recent data is available [here](https://github.com/ONEcampaign/ukraine_oda_tracker/raw/main/output/ukraine_refugee_cost_estimates.xlsx).
This excel contains the number of refugees recorded so far in each donor country,
our estimated average in-donor refugee costs, the net monthly arrivals of refugees, and figures for 2022, 2023 and 2024 costs.

Python (>=3.10) is required and additional packages required are listed under `requirements.txt`.

### Scripts
The `scripts` directory contains the following:
- `conifg.py`: manages working directory and file paths.
- `create_table.py`: creates a csv file for the tracking table (a Flourish visualization).
- `idrc_per_capita.py`: to reproduce the in-donor refugee costs per capita figure for each donor.
- `oda_data.py`: to read, clean and transform the data required to produce the different visualisations.
- `unhcr_data.py`: to scrape the refugee data from UNHCR.


### Raw data
The `raw_data` folder contains data extracted from the OECD DAC databases.


### Output
The `output` folder contains the csv files used to create different Flourish visualisations


## Website and Charts

The ODA tracker can be found at ONE's website: https://www.one.org/international/aid-data/oda-to-ukraine/

And at SEEK's Donor Tracker website: https://donortracker.org/Ukraine-ODA-tracker

