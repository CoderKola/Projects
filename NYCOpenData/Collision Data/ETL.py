import logging 
import requests
import pandas as pd
import sqlite3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

# Constants
OUTPUT_FILE = "collision_data.csv"
REQUEST_LIMIT = 500
COLLISION_DB = "collision_data.db"

# Headers 
# Note: The headers are kept constant to prevent future schema changes
HEADERS = [
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


# Scrape Function
def scrape_nycopendata(dataframe):
    """
    Scrape data from NYC Open Data.
    """
    index = 0
    logging.info("Fetching data from NYC Open Data...")

    while True:
        try:
            logging.info(f"""Record Range: {index} to {index + REQUEST_LIMIT}""")
            response = requests.get(f'https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit={REQUEST_LIMIT}&$offset={index + REQUEST_LIMIT}')
            
            if response.status_code == 200:
                logging.info(f"URL: {response.url}")
                data = pd.DataFrame(response.json())

                if not data.empty:
                    logging.info(f"Fetched {len(data)} records.")
                    dataframe = pd.concat([dataframe, data], ignore_index=True).drop_duplicates(subset=['collision_id'])
                    index += REQUEST_LIMIT
                    
                    if len(dataframe) >= 1000:
                        logging.info(f"Reached temporary limit of 1000 records.")
                        return dataframe
                
                else:
                    logging.info("No more data to fetch.")
                    return dataframe

        except Exception as e:
            logging.error(f"Error fetching data: {e}")
            break


# Database Function
def create_database(dataframe, db_name=COLLISION_DB):
    """
    Create SQLite database and store the dataframe.
    """
    try:
        conn = sqlite3.connect(db_name)
        dataframe.to_sql(name='collisions', con=conn, index=False)
        conn.close()
        logging.info(f"Database {db_name} created successfully.")
    except Exception as e:
        logging.error(f"Error creating database: {e}")


# Main Function
def main():
    # Initialize dataframe
    collision_data = pd.DataFrame(columns=HEADERS)

    # Scrape Data, returns a dataframe
    collision_data = scrape_nycopendata(dataframe=collision_data)

    # Save to CSV
    collision_data.to_csv(OUTPUT_FILE, index=False)
    logging.info(f"Data saved to {OUTPUT_FILE}")


# Main Execution
if __name__ == "__main__":
    main()