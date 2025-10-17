# Note, there are three kinds of sources of data:
# Crashes, Vehicles, and Persons

import datetime as dt
import logging
import pandas as pd
import requests
import time

from data_transformer import transform_crash_data, transform_vehicle_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

REQUEST_LIMIT = 500
COLLISION_DB = "raw_master_collisions.db"

# Headers 
# Note: The headers are kept constant to prevent future schema changes
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

VEHICLES_HEADERS = [
    'unique_id', 'collision_id', 'crash_date', 'crash_time', 'vehicle_id', 'state_registration',
    'vehicle_type', 'vehicle_make', 'vehicle_model', 'vehicle_year', 'travel_direction', 'vehicle_occupants',
    'driver_sex', 'driver_license_status', 'driver_license_jurisdiction', 'pre_crash', 'point_of_impact', 'vehicle_damage',
    'vehicle_damage_2', 'vehicle_damage_3', 'public_property_damage', 'public_property_damage_type', 
    'contributing_factor_1', 'contributing_factor_2'
]

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

# Scrape Function
def scrape_nycopendata(dataframe, sourcetype):
    """
    Scrape data from NYC Open Data.
    """
    index = 0
    offset = 500
    logging.info("Fetching data from NYC Open Data...")

    while True:
        try:
            logging.info(f"""Record Range: {index} to {offset}""")
            response = requests.get(f'{sourcetype}.json?$limit={REQUEST_LIMIT}&$offset={offset}')
            
            if response.status_code == 200:
                logging.info(f"URL: {response.url}")
                data = pd.DataFrame(response.json())

                if not data.empty:
                    logging.info(f"Fetched {len(data)} records.")
                    dataframe = (
                        pd.concat([dataframe, data], ignore_index=True)
                        .drop_duplicates(subset=['collision_id'], keep='last')
                        .reindex(columns=dataframe.columns))
                    index = offset
                    offset += REQUEST_LIMIT
                    time.sleep(0.5)  # To avoid hitting rate limits
                    
                    if offset >= 1001:
                        logging.info(f"Reached temporary limit of 1000 records.")
                        index, offset = 0, 0
                        return dataframe
                
                else:
                    logging.info("No more data to fetch.")
                    index, offset = 0, 500
                    return dataframe

        except Exception as e:
            logging.error(f"Error fetching data: {e}")
            break

# ETL Process to Store Data into DataFrame and CSV
def process_etl_to_df_csv(key, value, dataframe):
    """
    Process ETL for a given key, value and dataframe.
    """
    logging.info(f"------Starting ETL for {key}------")
    dataframe = scrape_nycopendata(dataframe=dataframe, sourcetype=value['url'])
    dataframe.to_csv(value['csv_name'], index=False)
    logging.info(f"Saved {len(dataframe)} records to {value['csv_name']}")
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
        dataframes[key] = process_etl_to_df_csv(key, value, dataframes[key])

    # Transform each set of data
    crash_transformed =  transform_crash_data(dataframes['crashes'])
    vehicle_transformed = transform_vehicle_data(dataframes['vehicles'])
    
    # Print to csv
    crash_transformed.to_csv('test.csv', index=False)
    vehicle_transformed.to_csv('vehicle_test.csv', index=False)

# Main Execution
if __name__ == "__main__":
    main()