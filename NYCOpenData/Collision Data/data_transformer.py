# Takes in crash dataframe and outputs a cleaned version for analysis and merging

import datetime as dt
import pandas as pd

from pandas.api.types import is_datetime64_any_dtype

# ---- helpers ---------------------------------------------------------------
def combine_factors_row(row: pd.Series) -> str:
    """
    Normalize + combine + de-dupe contributing-factor columns in a single row.
    - Drops None/NaN/'' and case-insensitive {'unspecified','nan'}
    - Preserves first occurrence order
    - Returns a comma-separated string
    """
    seen, out = set(), []
    for val in row:
        if pd.isna(val):
            continue
        s = str(val).strip()
        if not s or s.lower() in {"unspecified", "nan"}:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return ", ".join(out)

# ---- main ---------------------------------------------------------------

# Transform Crash Dataframe
def transform_crash_data(crash_df: pd.DataFrame) -> pd.DataFrame:
    """
    Intakes a dataframe and returns a transformed dataframe.
    1. Transform crash_date to datetime object and crash_time to time object
    2. Combine contributing factor columns into a single column and drop individual columns
    3. Drop vehicle type columns because we will rely on the vehicles table later
    4. Rename unique_id to crashtable_record_id
    """

    # ensure crash_date is datetime object
    if not is_datetime64_any_dtype(crash_df['crash_date']):
        crash_df['crash_date'] = pd.to_datetime(
            crash_df['crash_date'], errors = 'coerce'
        )
    
    # Ensure crash_time is time object
    is_time_objects = crash_df['crash_time'].dropna().map(
        lambda x: isinstance(x, dt.time)
    ).all()

    # Headers for vehicle contributing factors
    vehicle_contributing_factors_cols = [f"contributing_factor_vehicle_{i}" for i in range(1, 6)]

    # Clean > drop NA > remove dupes > drop old columns
    crash_df["event_collision_factors"] = crash_df[vehicle_contributing_factors_cols].apply(
        combine_factors_row, axis=1
    )

    crash_df.drop(columns=vehicle_contributing_factors_cols, axis=1, inplace=True)

    # Headers for vehicle types
    # Note, we will rely on the vehicle data for vehicle type details
    vehicle_type_factors = [f"vehicle_type_code{i}" for i in range(1, 3)] + [f"vehicle_type_code_{i}" for i in range(3, 6)]
    crash_df.drop(columns=vehicle_type_factors, axis=1, inplace=True)

    # Remove 'location' column as it is redundant with latitude and longitude
    # Note, this also include human_address, not something we need
    # Source also does not seem to populate this consistently
    crash_df.drop(columns=['location'], axis=1, inplace=True)

    # Rename unique_id to crashtable_record_id
    crash_df = crash_df.rename(columns={'unique_id': 'crashtable_record_id'})

    return crash_df

# Transform Vehicle Dataframe
def transform_vehicle_data(crash_vehicle_df: pd.DataFrame) -> pd.DataFrame:
    """ 
    1. Remove crash_date and crash_time columns from vehicle dataframe because they exist in dataframe
    2. Rename unique_id to unique_vehicle_record_id
    """
    columns_to_drop = ['crash_date', 'crash_time']
    crash_vehicle_df.drop(columns=columns_to_drop, axis=1, inplace=True)

    # rename unique_id to unique_vehicle_record_id
    crash_vehicle_df = crash_vehicle_df.rename(columns={
        'unique_id': 'vehicletable_record_id',
        'contributing_factor_1': 'vehicle_contributing_factor_1',
        'contributing_factor_2': 'vehicle_contributing_factor_2'
        })

    return crash_vehicle_df

# Transform Person Dataframe
def transform_person_data(crash_person_df: pd.DataFrame) -> pd.DataFrame:
    """ 
    1. Remove crash_date and crash_time columns from vehicle dataframe because they exist in dataframe
    2. Rename unique_id to unique_person_record_id
    """
    columns_to_drop = ['crash_date', 'crash_time']
    crash_person_df.drop(columns=columns_to_drop, axis=1, inplace=True)

    # Rename crash record id to unique_person_record_id
    crash_person_df = crash_person_df.rename(columns={
        'unique_id': 'persontable_record_id',
        'contributing_factor_1': 'person_contributing_factor_1',
        'contributing_factor_2': 'person_contributing_factor_2'
        })

    return crash_person_df