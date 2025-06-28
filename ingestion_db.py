import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# Create SQLite engine
engine = create_engine('sqlite:///inventory.db')

# Function to ingest DataFrame into DB
def ingest_db(df, table_name, engine):
    '''This function will ingest the DataFrame into a database table'''
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

# Function to load CSVs and ingest
def load_raw_data():
    '''This function will load CSVs as DataFrames and ingest into the DB'''
    start = time.time()

    for file in os.listdir('.'):
        if file.endswith('.csv'):
            df = pd.read_csv(file)
            logging.info(f'Ingesting {file} in db')
            ingest_db(df, file[:-4], engine)

    end = time.time()
    total_time = (end - start) / 60
    logging.info('---------Ingestion complete---------')
    logging.info(f'Total Time Taken: {total_time} minutes')

# Run the script
if __name__ == '__main__':
    load_raw_data()