# GTFS Weekly Transit Data Processor

This Python project processes **GTFS (General Transit Feed Specification) data** to produce a weekly transit summary, including suburb-to-suburb travel times, number of trips, and route coverage across weekdays and weekends. It also maps transit stops to suburb boundaries for spatial context.  

---

## Features

The script will:
1. Fetch GTFS data from the specified URL.
2. Filter services valid for the upcoming week.
3. Compute suburb-to-suburb travel times and frequency.
4. Export the final summary to `output/transit_summary.csv`.

---

## Requirements

Python 3.10+ and the following packages:

```txt
fastparquet==2024.11.0
geopandas==1.1.1
numpy==2.3.4
pandas==2.3.3
pyarrow==22.0.0
pyproj==3.7.2
python-dateutil==2.9.0.post0
python-dotenv==1.1.1
pytz==2025.2
requests==2.32.5
shapely==2.1.2
tqdm==4.67.1
tzdata==2025.2
```

## Install dependencies with
```bash
pip install -r requirements.txt
```
## Environment Variables
This project uses a .env file to configure output locations:
```env
    GTFS_URL=https://gtfsrt.api.translink.com.au/GTFS/SEQ_GTFS.zip
    DATA_DIR= data\gtfs
    SUBURB_DIR= data\suburbs
    OUTPUT_DIR = output
```
## Usage
Run the main script to process GTFS data and generate the transit summary:
```bash
python main.py
```


