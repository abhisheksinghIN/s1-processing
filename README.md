# Sentinel-1 SLC Burst Processing

Scripts for preparing Sentinel-1 Single Look Complex (SLC) data for the Po Valley soil-moisture study.

# Scripts

scripts/01_s1_aoi_burst_manifestation_selected.ipynb — Notebook for defining the area of interest and preparing a Sentinel-1 burst manifest.

scripts/02_extract_bursts.py — Python script for extracting Sentinel-1 bursts based on the prepared manifest.

# Requirements

Python dependencies and Copernicus Data Space credentials is required, depending on the processing configuration. Keep credentials and local configuration files out of version control.

# Usage

Run the notebook first to prepare the burst manifest, then run the extraction script with the corresponding configuration and input data.

