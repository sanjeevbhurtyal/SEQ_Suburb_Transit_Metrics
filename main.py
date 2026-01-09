import gc
import numpy as np
from utils.functions import fetch_gtfs_data, fetch_suburb_data, fetch_gtfs_data_from_web
from utils.gtfs_schemas import cast_stops_table, cast_routes_table, cast_trips_table, cast_stop_times_table, cast_calendar_table, cast_calendar_dates_table
import logging
from datetime import datetime, timedelta
import time
import pandas as pd
import geopandas as gpd
import os
from dotenv import load_dotenv
load_dotenv()
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import shutil

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def gtfs_data(table: str) -> pd.DataFrame:
    """
    Fetch GTFS data apply table-specific casting in pandas.
    """
    # Fetch GTFS data
    df = fetch_gtfs_data(table)

    # Map table names to their casting functions
    casting_functions = {
        "stops": cast_stops_table,
        "routes": cast_routes_table,
        "trips": cast_trips_table,
        "calendar": cast_calendar_table,
        "calendar_dates": cast_calendar_dates_table,
        "stop_times": cast_stop_times_table
    }

    # Apply casting function if available, otherwise keep df as is
    df = casting_functions.get(table, lambda x: x)(df)

    # Log number of records
    logger.info(f"Fetched {len(df)} records from the {table} table.")

    return df

def valid_week() -> dict:
    """
    Returns the start and end date of the next week (Monday to Sunday)
    in 'YYYY-MM-DD' format.
    """
    today = datetime.today()
    next_monday = today + timedelta((7 - today.weekday()) % 7)
    next_sunday = next_monday + timedelta(days=6)

    logger.info(f"Next week starts on {next_monday.date()} and ends on {next_sunday.date()}")

    # Return as dict
    return {
        "start_date": next_monday.strftime("%Y-%m-%d"),
        "end_date": next_sunday.strftime("%Y-%m-%d")
    }

def valid_gtfs_data():
    date_range = valid_week()
    start_date = pd.to_datetime(date_range["start_date"])
    end_date = pd.to_datetime(date_range["end_date"])

    calendar_df = gtfs_data("calendar")
    calendar_df["start_date"] = pd.to_datetime(calendar_df["start_date"])
    calendar_df["end_date"] = pd.to_datetime(calendar_df["end_date"])
    calendar_df = calendar_df[
        (calendar_df["start_date"] <= end_date) &
        (calendar_df["end_date"] >= start_date)
    ].drop(columns=["start_date", "end_date"])
    

    # Load trips and keep only valid services
    trips_df = gtfs_data("trips")
    trips_df = trips_df.merge(calendar_df, on="service_id", how="inner")
    trips_df = trips_df.drop(columns=["trip_headsign", "block_id", "shape_id"])
    
    
    # Load routes and join trips
    routes_df = gtfs_data("routes")
    routes_df = routes_df.merge(trips_df, on="route_id", how="inner")
    routes_df = routes_df.drop(columns=["route_long_name", "route_desc", "route_url", "route_color", "route_text_color"])


    # Compute weekly route status
    days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    routes_weekly_status_df = routes_df.groupby("route_short_name")[days].max().reset_index()    
    
    # Compute weekday/weekend flags
    routes_weekly_status_df["weekday"] = (
        routes_weekly_status_df[["monday", "tuesday", "wednesday", "thursday", "friday"]].all(axis=1).astype(int)
    )
    routes_weekly_status_df["weekend"] = (
        routes_weekly_status_df[["saturday", "sunday"]].all(axis=1).astype(int)
    )

    routes_weekly_status_df = routes_weekly_status_df[
                                (routes_weekly_status_df["weekday"] == 1) | (routes_weekly_status_df["weekend"] == 1)
                            ][["route_short_name", "weekday", "weekend"]]

    # Merge weekly status back into routes
    routes_df = routes_df.merge(routes_weekly_status_df, on="route_short_name", how="left")


    # Recompute weekday/weekend based on daily columns
    routes_df["weekday"] = (routes_df[["monday", "tuesday", "wednesday", "thursday", "friday"]].sum(axis=1) > 0).astype(int)
    routes_df["weekend"] = (routes_df[["saturday", "sunday"]].sum(axis=1) > 0).astype(int)
    
    # Join stop_times with routes
    stop_times_df = gtfs_data("stop_times")
    stop_times_df = stop_times_df.merge(routes_df, on="trip_id", how="inner")

    return stop_times_df


def process_route_to_parquet(route, df, output_folder):
    records = []

    try:
        for trip_id, trip_df in df.groupby('trip_id'):
            trip_df = trip_df.sort_values('stop_sequence').reset_index(drop=True)
            trip_df['arrival_time'] = pd.to_timedelta(trip_df['arrival_time'])
            trip_df['departure_time'] = pd.to_timedelta(trip_df['departure_time'])

            for i, from_row in trip_df.iterrows():
                for j in range(i + 1, len(trip_df)):
                    to_row = trip_df.iloc[j]
                    travel_time = max(to_row['arrival_time'] - from_row['departure_time'], pd.Timedelta(0))
                    travel_time_min = round(travel_time.total_seconds() / 60, 2) # Travel time in minutes

                    records.append({
                        'from_stop_id': from_row['stop_id'],
                        'to_stop_id': to_row['stop_id'],
                        'pickup_from': from_row['pickup_type'],
                        'drop_off_to': to_row['drop_off_type'],
                        'trip_id': trip_id,
                        'route_short_name': route,
                        'route_type': from_row['route_type'],
                        'travel_time': travel_time_min,
                        'monday': from_row['monday'],
                        'tuesday': from_row['tuesday'],
                        'wednesday': from_row['wednesday'],
                        'thursday': from_row['thursday'],
                        'friday': from_row['friday'],
                        'saturday': from_row['saturday'],
                        'sunday': from_row['sunday'],
                        'weekday': from_row['weekday'],
                        'weekend': from_row['weekend'],
                    })

        if not records:
            return None  

        result = pd.DataFrame(records)
        os.makedirs(output_folder, exist_ok=True)
        out_path = os.path.join(output_folder, f"{route}.parquet")
        result.to_parquet(out_path, index=False, engine='pyarrow')
        return out_path

    finally:
        del df
        del records
        if 'result' in locals():
            del result
        gc.collect()


def process_route_wrapper(args):
    route, df, output_dir = args
    return process_route_to_parquet(route, df, output_dir)

def stops_with_suburbs():
    suburbs = fetch_suburb_data()

    # Export suburbs to shapefile
    suburbs.rename(columns={'suburb_geom': 'geometry'}).to_file(os.path.join(OUTPUT_DIR, "Locality_Boundaries.shp"))

    stops_df = gtfs_data("stops")[['stop_id', 'stop_lat', 'stop_lon']]
    
    # Convert stops to GeoDataFrame
    stops_gdf = gpd.GeoDataFrame(
        stops_df,
        geometry=gpd.points_from_xy(stops_df.stop_lon, stops_df.stop_lat),
        crs="EPSG:4326"
    )
    # Spatial join with suburbs
    stops_with_suburbs = gpd.sjoin(stops_gdf, suburbs, how="left", predicate='within')[['stop_id', 'loc_code', 'locality']]
    stops_with_suburbs = pd.DataFrame(stops_with_suburbs)

    return stops_with_suburbs


if __name__ == "__main__":

    OUTPUT_DIR =  os.getenv('OUTPUT_DIR')
    if os.path.exists(OUTPUT_DIR):
            shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    fetch_gtfs_data_from_web()

    trip_time_table = valid_gtfs_data()
    grouped_routes = list(trip_time_table.groupby('route_short_name'))
    args_list = [(route, group_df.copy(), OUTPUT_DIR) for route, group_df in grouped_routes]
    
    num_workers = min(cpu_count() - 2, len(args_list))

    with Pool(processes=num_workers, maxtasksperchild=1) as pool:
        parquet_files = list(tqdm(pool.imap_unordered(process_route_wrapper, args_list), total=len(args_list)))

    df_list = [pd.read_parquet(pf) for pf in parquet_files if pf and os.path.exists(pf)]
    df_final = pd.concat(df_list, ignore_index=True)

    stops = stops_with_suburbs()

    df_final = df_final.merge(
        stops,
        left_on='from_stop_id',
        right_on='stop_id',
        how='left'
    ).rename(
        columns={
            'loc_code': 'from_loc_code',
            'locality': 'from_locality'
        }
    ).drop(columns=['stop_id'])
    df_final = df_final.merge(
        stops,
        left_on='to_stop_id',
        right_on='stop_id',
        how='left'
    ).rename(
        columns={
            'loc_code': 'to_loc_code',
            'locality': 'to_locality'
        }
    ).drop(columns=['stop_id'])

    weekday_cols = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']
    weekend_cols = ['saturday', 'sunday']

    group_cols = ['route_type', 'route_short_name', 'from_loc_code', 'to_loc_code','weekday', 'weekend','from_locality', 'to_locality'] + weekday_cols + weekend_cols
    
    df_final = df_final.groupby(
        group_cols
    ).agg(
        number_of_trips=('trip_id', 'nunique'),
        travel_time=('travel_time', 'mean')
    ).reset_index()

    df_final = df_final.groupby(
        ['route_short_name','route_type', 'from_loc_code', 'to_loc_code','from_locality', 'to_locality', 'weekday', 'weekend']
        ).agg(
            number_of_routes=('route_short_name', 'nunique'),
            number_of_trips=('number_of_trips', 'median'),
            travel_time=('travel_time', 'mean')
        ).reset_index()

    df_final = df_final.groupby(
        ['route_type', 'from_loc_code', 'to_loc_code','from_locality', 'to_locality', 'weekday', 'weekend']
        ).agg(
            number_of_routes=('route_short_name', 'nunique'),
            number_of_trips_per_day=('number_of_trips', 'sum'),
            travel_time_min=('travel_time', 'min'),
            travel_time_max=('travel_time', 'max'),
            travel_time_median=('travel_time', 'median')
        ).reset_index()
    
    #Floor number of trips per day
    df_final['number_of_trips_per_day'] = np.floor(df_final['number_of_trips_per_day']).astype(int)

    # Round travel timeto integer
    df_final['travel_time_min'] = df_final['travel_time_min'].round().clip(lower=1).astype(int)
    df_final['travel_time_max'] = df_final['travel_time_max'].round().clip(lower=1).astype(int)
    df_final['travel_time_median'] = df_final['travel_time_median'].round().clip(lower=1).astype(int)

    df_final = df_final[df_final['from_loc_code'] != df_final['to_loc_code']]

    df_final.to_csv(os.path.join(OUTPUT_DIR, "transit_summary.csv"), index=False)

    # Remove temporary parquet files
    for pf in parquet_files:
        if os.path.exists(pf):
            os.remove(pf)
    logging.info("Processing complete.")

