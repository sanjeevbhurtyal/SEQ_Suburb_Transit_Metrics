import pandas as pd

def cast_columns(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """
    Cast columns of a pandas DataFrame according to the schema dict.

    schema: dict of {column_name: data_type}, e.g.
        {"trip_id": "int", "start_date": "date:%Y%m%d"}
    """
    df = df.copy()
    for column, data_type in schema.items():
        if data_type.startswith("date:"):
            fmt = data_type.split(":", 1)[1]
            fmt = fmt.replace("yyyy", "%Y").replace("MM", "%m").replace("dd", "%d")
            df[column] = pd.to_datetime(df[column], format=fmt, errors="coerce")
        else:
            df[column] = df[column].astype(data_type)
    return df


def gtfs_time_to_seconds(series: pd.Series) -> pd.Series:
    """
    Convert a pandas Series of GTFS 'HH:MM:SS' time strings to total seconds.
    
    Parameters:
    - series: pandas Series containing 'HH:MM:SS' strings
    
    Returns:
    - pandas Series of integers representing total seconds
    """
    # Split the time string into hours, minutes, seconds
    parts = series.str.split(":", expand=True).astype(int)
    
    # Convert to seconds
    return parts[0]*3600 + parts[1]*60 + parts[2]


def cast_stops_table(df):
    schema = {
        "stop_id": "string",
        "stop_code": "string",
        "stop_lat": "double",
        "stop_lon": "double",
        "zone_id": "string",
        "location_type": "string",
        "parent_station": "string"
    }
    df = cast_columns(df, schema)
    return df

def cast_stop_times_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert GTFS stop_times table columns to proper types in pandas:
    - Convert 'arrival_time' and 'departure_time' to seconds
    - Cast other columns according to schema
    """
    df = df.copy()

    # Convert GTFS time strings to seconds
    df["arrival_time_secs"] = gtfs_time_to_seconds(df["arrival_time"])
    df["departure_time_secs"] = gtfs_time_to_seconds(df["departure_time"])

    # Define schema for type casting
    schema = {
        "trip_id": "string",
        "stop_id": "string",
        "stop_sequence": "int",
        "pickup_type": "int",
        "drop_off_type": "int"
    }

    # Cast columns according to schema
    df = cast_columns(df, schema)

    return df

def cast_trips_table(df: pd.DataFrame) -> pd.DataFrame:
    schema = {
        "route_id": "string",
        "service_id": "string",
        "trip_id": "string",
        "trip_headsign": "string",
        "direction_id": "int",
        "shape_id": "string"
    }
    df = cast_columns(df, schema)
    return df

def cast_routes_table(df: pd.DataFrame) -> pd.DataFrame:
    schema = {
        "route_id": "string",
        "route_type": "int"
    }
    df = cast_columns(df, schema)
    return df

def cast_shapes_table(df: pd.DataFrame) -> pd.DataFrame:
    schema = {
        "shape_id": "string",
        "shape_pt_lat": "double",
        "shape_pt_lon": "double",
        "shape_pt_sequence": "int"
    }
    df = cast_columns(df, schema)
    return df

def cast_calendar_table(df: pd.DataFrame) -> pd.DataFrame:
    schema = {
        "service_id": "string",
        "monday": "int",
        "tuesday": "int",
        "wednesday": "int",
        "thursday": "int",
        "friday": "int",
        "saturday": "int",
        "sunday": "int",
        "start_date": "date:yyyyMMdd",
        "end_date": "date:yyyyMMdd"
    }
    df = cast_columns(df, schema)
    return df

def cast_calendar_dates_table(df: pd.DataFrame) -> pd.DataFrame:
    schema = {
        "service_id": "string",
        "date": "date:yyyyMMdd",
        "exception_type": "int"
    }
    df = cast_columns(df, schema)
    return df

def cast_stops_with_suburbs(df: pd.DataFrame) -> pd.DataFrame:
    schema = {
        "stop_id": "string",
        "loc_code": "string"
    }
    df = cast_columns(df, schema)
    return df