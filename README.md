# **Ukraine ODA Tracker**
This repository contains data and scripts powering ONE's Ukraine ODA tracker)

## Repository Structure and Information

This repository contains data and scripts to create the csv file powering the flourish visualization for the tracker. 
Python (>=3.10) is required and additional packages required are listed under `requirements.txt`. 

The main purpose of the repository is to update the Ukraine ODA tracker with data compiled by the ONE team.

The repository includes the following subfolders:
- `output`: contains the CSV that powers the tracker (`sdr.csv`) and a CSV tracking each update.
- `scripts`: scripts for extracting and transforming data. `imf.py` queries the imf api to extract GDP data.`ukr_tracker.py` creates the final csv that powers the tracker. Additionally, a `config.py` file manages file paths to different folders; `utils.py` contains helper functions for a variety of frequently-used tasks
- `glossaries`: Found inside the `scripts`  folder. `glossaries` contains a json file for Flourish geometries for Africa. Other intermediate files including map templates and WEO data is saved in this folder

## Website and Charts

The ODA tracker can be found here: [link]

## Sources 

- source 1
- source 2