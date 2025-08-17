import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# Setup logging
logging.basicConfig(
    filename="logs/ingestion_db.log", 
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s", 
    filemode="a"  
)

# Creating SQLAlchemy engine for MySQL
engine = create_engine(f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DB}')

def ingest_db(df, table_name, engine):
    '''Ingest the dataframe into MySQL table'''
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logging.info(f'Data ingested into table {table_name} successfully.')

def load_raw_data():
    '''Load all CSVs in 'data' folder and ingest into MySQL'''
    start = time.time()
    
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join('data', file))
            logging.info(f'Ingesting {file} into DB')
            ingest_db(df, file[:-4], engine)  # removing .csv for table name

    end = time.time()
    total_time = (end - start)/60
    logging.info('--------------Ingestion Complete------------')
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')

if __name__ == '__main__':
    load_raw_data()
