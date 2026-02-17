from genericpath import exists
from urllib import response
import duckdb
import requests
from pathlib import Path

BASE_URL = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download'

def download_and_convert_files(taxi_type):
    data_dir = Path("data")/taxi_type
    data_dir.mkdir(exist_ok = True, parents = True)
    
    for year in [2019, 2020]:
        for month in range(1, 13):
            parquet_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            parquet_fillepath = data_dir/parquet_filename
            
            if parquet_fillepath.exists():
                print(f"skipping {parquet_filename} (already exists)")
                continue
            
            csv_gz_filename = f"{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
            csv_gz_filepath = data_dir/csv_gz_filename
            
            response = requests.get(f"{BASE_URL}/{taxi_type}/{csv_gz_filename}", stream= True)
            response.raise_for_status()
            
            with open(csv_gz_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size= 8912):
                    f.write(chunk)
                    
            print(f"Converting {csv_gz_filename} to parquet...")
            con = duckdb.connect()
            con.execute(f"""
                        copy (select * from read_csv_auto('{csv_gz_filepath}))
                        to '{parquet_fillepath}' (format parquet)
                        """)
            con.close()
            
            csv_gz_filepath.unlink()
            print(f"complete {parquet_filename}")
            
def update_gitignore():
    gitignore_path = Path(".gitignore")
    content = gitignore_path.read_text() if gitignore_path.exists() else ""
    
    if 'data/' not in content:
        with open(gitignore_path, 'a') as f:
            f.write('\n# Data directory\ndata/\n' if content else '# Data directory\ndata/\n')
            
if __name__ == '__main__':
    update_gitignore()
    
    for taxi_type in ['yellow', 'green']:
        download_and_convert_files(taxi_type)
        
    con = duckdb.connect('taxi_rides_ny.duckdb')
    con.execute("create schema if not exists prod")
    
    for taxi_type in n['yellow', 'green']:
        con.execute(f"""
                    create or replace table prod.{taxi_type}_tripdata as
                    select * from read_parquet('data/{taxi_type}/*.parquet', union_by_name = true)
                    """)
        
        con.close()