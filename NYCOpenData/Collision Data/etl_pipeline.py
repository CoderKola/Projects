# Note, there are three kinds of sources of data:
# Crashes, Vehicles, and Persons

import datetime as dt
import logging
import pandas as pd
import requests
import time

from data_transformer import transform_crash_data, transform_vehicle_data, transform_person_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

REQUEST_LIMIT = 1000000
COLLISION_DB = "raw_master_collisions.db"

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
# Function to scrape NYC Open Data
def scrape_nycopendata(name, dataframe, sourcetype):
    index = 0
    offset = REQUEST_LIMIT
    dup_csv = "crash_duplicates.csv" # This is required just to doucment duplicates found in 'crash' if the source is incorrect
    header_written = False  # write CSV header once per run

    while True:
        try:
            logging.info(f"Record Range: {index} to {offset}")
            response = requests.get(f"{sourcetype}.json?$limit={REQUEST_LIMIT}&$offset={offset}")
            if response.status_code != 200:
                logging.error(f"HTTP {response.status_code}")
                return dataframe

            data = pd.DataFrame(response.json())
            if data.empty:
                logging.info("No more data to fetch.")
                return dataframe

            logging.info(f"Fetched {len(data)} records.")

            if name == "crashes":
                # Condition A: duplicates inside this fetched batch
                duplicates_inside_batch = data["collision_id"].duplicated(keep=False)

                # Condition B: ids that already exist in the accumulated dataframe
                existing_ids = set(dataframe["collision_id"].dropna())
                duplicates_against_existing = data["collision_id"].isin(existing_ids)

                # Any row that meets A or B is a duplicate we want to log
                duplicates_any = duplicates_inside_batch | duplicates_against_existing
                count_any = int(duplicates_any.sum())

                if count_any:
                    logging.info(
                        f"Duplicates this batch: {count_any} "
                        f"(within={int(duplicates_inside_batch.sum())}, "
                        f"overlap={int(duplicates_against_existing.sum())})"
                    )
                    # Append those duplicate rows to CSV (header once per run)
                    data.loc[duplicates_any].to_csv(
                        dup_csv, mode="a", index=False, header=not header_written
                    )
                    header_written = True

                # Combine new and existing, then keep the last version per collision_id
                dataframe = (
                    pd.concat([dataframe, data], ignore_index=False)
                      .drop_duplicates(subset=["collision_id"], keep="last")
                      .reindex(columns=dataframe.columns)
                )
            else:
                # Non-crash datasets: just append and align columns
                dataframe = (
                    pd.concat([dataframe, data], ignore_index=False)
                      .reindex(columns=dataframe.columns)
                )

            index = offset
            offset += REQUEST_LIMIT
            time.sleep(0.5)

        except Exception as e:
            logging.error(f"Error fetching data: {e}")
            return dataframe

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
        dataframes[key] = scrape_nycopendata(name=key, dataframe=dataframes[key], sourcetype=value['url'])
        logging.info(f"✅ Completed NYCOpenData scrape for {key}.\n")

        match key:
            case 'crashes':
                crash_data_transformed = transform_crash_data(dataframes['crashes'])
                # Uncomment to output transformed data
                crash_data_transformed.to_csv('transformed_collision_crash.csv', index=False)
            case 'vehicles':
                vehicle_data_transformed = transform_vehicle_data(dataframes['vehicles'])
                # Uncomment to output transformed data
                vehicle_data_transformed.to_csv('transformed_collision_vehicle.csv', index=False)
            case 'persons':
                person_data_transformed = transform_person_data(dataframes['persons'])
                # Uncomment to output transformed data
                person_data_transformed.to_csv('transformed_collision_person.csv', index=False)

    # Normalize key types so matches actually happen
    for df in [crash_data_transformed, vehicle_data_transformed, person_data_transformed]:
        for k in ['collision_id', 'vehicle_id']:
            if k in df.columns:
                df[k] = df[k].astype(str).str.strip()  # or .astype('int64') if numeric everywhere
    

    # Merge crash → vehicles (1-to-many)
    cv = crash_data_transformed.merge(
        vehicle_data_transformed,
        how='left',                 # use 'inner' only if you truly want to drop crashes without vehicles
        on='collision_id',
        suffixes=('_crash', '_vehicle'),
        validate='one_to_many'      # guards assumptions
    )

    # TODO: Getting error that there are many crash -> vehicle in left dataset, need to investigate
    #     raise MergeError(
    #     "Merge keys are not unique in left dataset; not a one-to-many merge"
    # )
    #  Merge (crash+vehicle) → persons (1 vehicle-to-many persons)
    final_df = cv.merge(
        person_data_transformed,
        how='left',                 # use 'inner' if you only want vehicles that have at least one person
        on=['collision_id', 'vehicle_id'],
        suffixes=('', '_person'),
        validate='one_to_many'
    )

    final_df.to_csv('final_collision_data.csv', index=False)

# Main Execution
if __name__ == "__main__":
    main()