import os
import argparse
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port= params.port
    database_name = params.database_name
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')

    df_iter = pd.read_csv(csv_name, compression = 'gzip', iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name = table_name, con=engine, if_exists="replace")

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True: 

            try:
                t_start = time()
                
                df = next(df_iter)

                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

                df.to_sql(name=table_name, con=engine, if_exists='append')

                t_end = time()

                print('Inserted another chunk! Operation took %.3f second' % (t_end - t_start))

            except StopIteration:
                print("Finished ingesting data into the postgres database")
                break

#Define user, password, host, port, database, table, url
    
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--database_name', help='database name for postgres')
    parser.add_argument('--table_name', help='the table the results are written to')
    parser.add_argument('--url', help='url of the csv file')                  

    args = parser.parse_args()

    main(args)