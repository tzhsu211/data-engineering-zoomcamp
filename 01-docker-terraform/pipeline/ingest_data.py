
import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine
import click



dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}
parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option("--pg-user", default = "root", help="PostgreSQL Username")
@click.option("--pg-password", default = "root", help="PostgreSQL Password")
@click.option("--pg-host", default = "localhost", help= "PostgreSQL Host")
@click.option("--pg-port", default = 5432, help="PostgreSQL Port")
@click.option("--pg-db", default = "ny_taxi", help = "PostgreSQL DB name")
@click.option("--year", default = 2021, help="Year of the data")
@click.option("--month", default = 1, help = "Month of the data")
@click.option("--target-table", default = "yellow_taxi_data", help = "Target Table Name")
@click.option("--chunksize", default = 100000, help = "Data chunksize")

def main(pg_user, pg_password, pg_host, pg_port, pg_db, year, month, chunksize, target_table):
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    url = f"{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz"
    
    db_setting = {
        "pg_user" : pg_user,
        "pg_password" : pg_password,
        "pg_host": pg_host,
        "pg_port": pg_port,
        "pg_db": pg_db
    }
    engine = create_engine(f"postgresql://{db_setting['pg_user']}:{db_setting['pg_password']}@{db_setting['pg_host']}:{db_setting['pg_port']}/{db_setting['pg_db']}") # user:password@host:port/db

    df_iter = pd.read_csv(
        url,
        dtype = dtype,
        parse_dates = parse_dates,
        iterator= True,
        chunksize = chunksize
    )
    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name = target_table,
                con= engine,
                if_exists = "replace"
            )
            first = False
        df_chunk.to_sql(
            name = target_table, 
            con= engine, 
            if_exists = 'append'
            )
        
if __name__ ==  "__main__":
    main()