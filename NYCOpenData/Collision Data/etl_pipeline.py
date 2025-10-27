# Note, there are three kinds of sources of data:
# Crashes, Vehicles, and Persons

import datetime as dt
import logging
import os
import pandas as pd

from other_functions.data_transformer import transform_crash_data, transform_vehicle_data, transform_person_data
from other_functions.scrape_nycopendata_collisions import scrape_nycopendata

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# Data file output directory
DATA_OUTPUT_DIR = "data"
if not os.path.exists(DATA_OUTPUT_DIR):
    os.makedirs(DATA_OUTPUT_DIR)

REQUEST_LIMIT = 1000000

# Headers > Note: The headers are kept constant to prevent future schema changes
# Note, there is no unique_id in crash data. Row id is collision_id
CRASH_HEADERS = [
    'crash_date', 'crash_time', 'borough', 'zip_code', 'latitude', 'longitude',
    'location', 'on_street_name', 'off_street_name', 'cross_street_name',
    'number_of_persons_injured', 'number_of_persons_killed',
    'number_of_pedestrians_injured', 'number_of_pedestrians_killed',
    'number_of_cyclist_injured', 'number_of_cyclist_killed',
    'number_of_motorist_injured', 'number_of_motorist_killed',
    'contributing_factor_vehicle_1', 'contributing_factor_vehicle_2',
    'contributing_factor_vehicle_3', 'contributing_factor_vehicle_4',
    'contributing_factor_vehicle_5', 'collision_id', 'vehicle_type_code1',
    'vehicle_type_code2', 'vehicle_type_code_3', 'vehicle_type_code_4',
    'vehicle_type_code_5'
]

# Note: each row is motor vehicle involved in a crash.
VEHICLES_HEADERS = [
    'unique_id', 'collision_id', 'crash_date', 'crash_time', 'vehicle_id', 'state_registration',
    'vehicle_type', 'vehicle_make', 'vehicle_model', 'vehicle_year', 'travel_direction', 'vehicle_occupants',
    'driver_sex', 'driver_license_status', 'driver_license_jurisdiction', 'pre_crash', 'point_of_impact', 'vehicle_damage',
    'vehicle_damage_2', 'vehicle_damage_3', 'public_property_damage', 'public_property_damage_type', 
    'contributing_factor_1', 'contributing_factor_2'
]

# Note: person (driver, occupant, pedestrian, bicyclist,..) involved in a crash.
PERSONS_HEADERS = [
    'unique_id', 'collision_id', 'crash_date', 'crash_time', 'person_id', 'person_type', 'vehicle_id', 'person_age',
    'ejection', 'emotional_status', 'bodily_injury', 'position_in_vehicle', 'safety_equipment', 'ped_location', 'ped_action',
    'complaint', 'ped_role', 'contributing_factor_1', 'contributing_factor_2', 'person_sex'
]

# Constants
OUTPUT_FILE = {
    'crashes': {
        'headers': CRASH_HEADERS,
        'url': 'https://data.cityofnewyork.us/resource/h9gi-nx95',
        'csv_name': 'collision_crash.csv',
    },
    'vehicles': {
        'headers': VEHICLES_HEADERS,
        'url': 'https://data.cityofnewyork.us/resource/bm4k-52h4',
        'csv_name': 'collision_vehicle.csv',
    },
    'persons': {
        'headers': PERSONS_HEADERS,
        'url': 'https://data.cityofnewyork.us/resource/f55k-p6yu',
        'csv_name': 'collision_person.csv',
    }
}
# Main Function
def main():
    # Create three dataframes for each source type
    crash_data = pd.DataFrame(columns=OUTPUT_FILE['crashes']['headers'])
    vehicle_data = pd.DataFrame(columns=OUTPUT_FILE['vehicles']['headers'])
    person_data = pd.DataFrame(columns=OUTPUT_FILE['persons']['headers'])

    # Map keys to respective dataframes
    dataframes = {
        'crashes': crash_data,
        'vehicles': vehicle_data,
        'persons': person_data,
    }

    # Process extraction for each source type
    for key, value in OUTPUT_FILE.items():
        logging.info(f"🎬 Starting NYCOpenData scrape for {key}...")
        dataframes[key] = scrape_nycopendata(name=key, dataframe=dataframes[key], sourcetype=value['url'], request_limit=REQUEST_LIMIT, data_output_dir=DATA_OUTPUT_DIR)
        logging.info(f"✅ Completed NYCOpenData scrape for {key}.\n")

        match key:
            case 'crashes':
                crash_data_transformed = transform_crash_data(dataframes['crashes'])
                # Uncomment to output transformed data
                crash_data_transformed.to_csv(os.path.join(DATA_OUTPUT_DIR, 'transformed_collision_crash.csv'), index=False)
            case 'vehicles':
                vehicle_data_transformed = transform_vehicle_data(dataframes['vehicles'])
                # Uncomment to output transformed data
                vehicle_data_transformed.to_csv(os.path.join(DATA_OUTPUT_DIR, 'transformed_collision_vehicle.csv'), index=False)
            case 'persons':
                person_data_transformed = transform_person_data(dataframes['persons'])
                # Uncomment to output transformed data
                person_data_transformed.to_csv(os.path.join(DATA_OUTPUT_DIR, 'transformed_collision_person.csv'), index=False)

    # Merge crash → vehicles (1-to-many)
    cv = crash_data_transformed.merge(
        vehicle_data_transformed,
        how='left',
        on='collision_id',
        suffixes=('_crash', '_vehicle'),
        validate='one_to_many'      # guards assumptions
    )

    # Once merged, the 'unique_id' is for vehicle record which we need to merge to person data
    # If this is blank, we need to generate a merge_key_vehicle as "{collision_id}_NULL"    
    cv['merge_key_vehicle'] = cv.apply(
        lambda row: f"{row['collision_id']}_NULL" if pd.isna(row.get('unique_id')) or str(row['unique_id']).strip() == '' 
        else f"{row['collision_id']}_{row['unique_id']}", axis=1
    )
    cv.to_csv(os.path.join(DATA_OUTPUT_DIR, 'crash_vehicle_data_merge1.csv'), index=False)

    final_df = cv.merge(
        person_data_transformed,
        how='left',
        left_on='merge_key_vehicle',
        right_on='merge_key_person',
        suffixes=('', '_person'),
        validate='one_to_many'
    )

    final_df.to_csv(os.path.join(DATA_OUTPUT_DIR, 'final_collision_data.csv'), index=False)

# Main Execution
if __name__ == "__main__":
    logging.info("🎬 Starting NYCOpenData Collision- Crash, Vehicle, and Person ETL Pipeline...")
    main()
    logging.info("✅ Completed NYCOpenData Collision- Crash, Vehicle, and Person ETL Pipeline.")