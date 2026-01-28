import click
import duckdb

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

def main(pg_user, pg_password, pg_host, pg_port, pg_db):
    
    target_table = "green_tripdata_2025_11"
    source_file = 'green_tripdata_2025-11.parquet'
    
    db_setting = {
        "pg_user" : pg_user,
        "pg_password" : pg_password,
        "pg_host": pg_host,
        "pg_port": pg_port,
        "pg_db": pg_db
    }
    # engine = create_engine(f"postgresql://{db_setting['pg_user']}:{db_setting['pg_password']}@{db_setting['pg_host']}:{db_setting['pg_port']}/{db_setting['pg_db']}") # user:password@host:port/db

    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    pg_conn_str = f"host={pg_host} user={pg_user} password={pg_password} port={pg_port} dbname={pg_db}"
    con.execute(f"ATTACH '{pg_conn_str}' AS db (TYPE POSTGRES);")
    
    con.execute(f"CREATE TABLE IF NOT EXISTS db.{target_table} AS SELECT * FROM '{source_file}' LIMIT 0;")
    con.execute(f"INSERT INTO db.{target_table} SELECT * FROM '{source_file}';")
    
        
if __name__ ==  "__main__":
    main()