import pandas as pd
import requests
import sqlite3

# Scopred out headings from NYCOpenData portal
# Heaeders from source: https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data

print("Setting headers for DataFrame...")
collision_data_headers = [
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

# Create empty DataFrame with headers
collision_data = pd.DataFrame(columns=collision_data_headers)
print("Empty DataFrame created with headers.")


# Creating record fetch settings for NYCopenData API
num_of_records = 100 # number of records per API fetch
starting_page = 0
ending_page = 100

while True:
    try:
        print(f"\n--------------------\nCalling URL with 'offset' or records {starting_page} to {ending_page}, with record per call size of {num_of_records}")
        response = requests.get(f'https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit={num_of_records}&$offset={ending_page}')

        if response.status_code == 200:
            print("Response: ", response.status_code)
            data = pd.DataFrame(response.json())

            if not data.empty:
                collision_data = pd.concat([collision_data, data], ignore_index=True)
                print(f"✅ Fetched {len(data)} records, total records so far: {len(collision_data)}")
                starting_page = ending_page
                ending_page += 100

                # COMMENT OUT IF IN PRODUCTION
                if starting_page >= 500:
                    print("Reached Temporary Limit of 500 records.")
                    break

            if data.empty:
                print("No more data to fetch.")
                break

    except Exception as e:
        print("An error occured: ", e)


print("\n--------------------\nData fetching complete.")

# Save to CSV
collision_data.to_csv("collision_data.csv", index=False)
print("Data saved to 'collision_data.csv'.")

# Transform Raw Data - Data Cleaning
print("\n--------------------\nStarting Data Cleaning...")