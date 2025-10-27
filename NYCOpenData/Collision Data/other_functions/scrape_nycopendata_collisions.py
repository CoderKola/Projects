import logging
import os
import pandas as pd
import requests
import time

# Function to scrape NYC Open Data
def scrape_nycopendata(name, dataframe, sourcetype, request_limit, data_output_dir):
    """
    Scrapes NYC Open Data and returns a dataframe.
    Args:
        name: The name of the dataset to scrape.
        dataframe: The dataframe to append the scraped data to.
        sourcetype: The data from the url endpoint.
        request_limit: The number of records to scrape per request.
        data_output_dir: The directory to save the scraped data to.
    """
    DATA_OUTPUT_DIR = data_output_dir
    REQUEST_LIMIT = request_limit   

    index = 0
    offset = 0  # Changed from request_limit to start from beginning (was skipping first batch)
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
                        os.path.join(DATA_OUTPUT_DIR, dup_csv), mode="a", index=False, header=not header_written
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