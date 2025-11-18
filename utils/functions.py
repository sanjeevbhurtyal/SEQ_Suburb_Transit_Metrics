import pandas as pd
import logging
from dotenv import load_dotenv
import os
import geopandas as gpd
import requests
import zipfile
import io
import shutil
from requests.adapters import HTTPAdapter, Retry

# Load environment variables
load_dotenv()

# Logging setup
logger = logging.getLogger(__name__)

def fetch_gtfs_data_from_web():
    """
    Fetch GTFS data from a web URL and extract into data folder
    """

    logger.info("Fetching GTFS data from web...")

    GTFS_URL = os.getenv('GTFS_URL')
    DATA_DIR = os.getenv('DATA_DIR', 'data/gtfs')
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)
    os.makedirs(DATA_DIR)

    session = requests.Session()
    retries = Retry(
        total=5,                    
        backoff_factor=2,            
        status_forcelist=[500,502,503,504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    try:
        with session.get(GTFS_URL, stream=True, timeout=120) as response:
            response.raise_for_status()

            zip_path = os.path.join(DATA_DIR, "gtfs.zip")
            with open(zip_path, "wb") as f:
                for chunk in response.iter_content():
                    if chunk:
                        f.write(chunk)

        if zipfile.is_zipfile(zip_path):
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(DATA_DIR)
            logger.info("GTFS zip extracted")

        logger.info(f"GTFS data downloaded and extracted to {DATA_DIR}")

        os.remove(zip_path)

    except Exception as e:
        logger.error(f"Error fetching GTFS data from web: {e}")
        raise

def fetch_gtfs_data(table: str) -> pd.DataFrame:
    """
    Fetch GTFS data from local CSV files into a pandas DataFrame.
    """
    try:
        DATA_DIR = os.getenv('DATA_DIR', 'data/gtfs')
        file_path = os.path.join(DATA_DIR, f"{table}.txt")
        df = pd.read_csv(file_path)
        return df

    except Exception as e:
        logger.error(f"Error fetching GTFS data from local files: {e}")
        raise


def fetch_suburb_data_from_web():
    """
    Fetch suburb shapefile data from a web URL and extract into data folder
    """
    try:
        SHAPEFILE_URL = os.getenv('SHAPEFILE_URL')
        SUBURB_DIR = os.getenv('SUBURB_DIR', 'data/suburbs')
        if os.path.exists(SUBURB_DIR):
            shutil.rmtree(SUBURB_DIR)
        os.makedirs(SUBURB_DIR)

        # Download zip file
        response = requests.get(SHAPEFILE_URL, stream=True, timeout=120)
        response.raise_for_status()

        # Extract into a temporary folder first
        temp_extract_dir = os.path.join(SUBURB_DIR, "_temp")
        os.makedirs(temp_extract_dir, exist_ok=True)

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(temp_extract_dir)

        # Find the innermost folder that actually contains the shapefile files
        for root, dirs, files in os.walk(temp_extract_dir):
            if any(f.endswith((".shp", ".dbf", ".shx", ".prj", ".cpg")) for f in files):
                for f in files:
                    src = os.path.join(root, f)
                    dst = os.path.join(SUBURB_DIR, f)
                    shutil.move(src, dst)
                break  # stop after moving from the first folder containing shapefile files

        # Clean up the temp directory
        shutil.rmtree(temp_extract_dir, ignore_errors=True)

        logger.info(f"Suburb shapefile data downloaded and extracted to {SUBURB_DIR}")

    except Exception as e:
        logger.error(f"Error fetching suburb shapefile data from web: {e}")
        raise

def fetch_suburb_data() -> gpd.GeoDataFrame:
    """
    Fetch suburb shapefile data into a GeoDataFrame.
    """
    try:
        SHAPEFILE_PATH = os.getenv('SUBURB_DIR')
        gdf = gpd.read_file(SHAPEFILE_PATH).to_crs(epsg=4326)
        gdf.columns = gdf.columns.str.lower()
        gdf = gdf[['loc_code', 'locality', 'geometry']]
        gdf = gdf.rename(columns={'geometry': 'suburb_geom'})
        gdf = gdf.set_geometry('suburb_geom', crs='EPSG:4326')
        gdf['locality'] = gdf['locality'].fillna('not found')
        return gdf
    except Exception as e:
        logger.error(f"Error fetching suburb data: {e}")
        raise

