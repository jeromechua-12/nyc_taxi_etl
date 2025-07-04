import requests
from pathlib import Path
import os


def extract() -> None:
    '''
    Extract parquet file from NYC taxi trip record website
    <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>.
    and stores raw file in a local data directoryk. 

    Parameters:
        None

    Returns:
        None
    '''
    try:
        url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
        print(f"extracting file from {url}...")
        response = requests.get(url)

        # create directory to store raw data
        cur_dir = Path.cwd()
        os.makedirs(f"{cur_dir}/data/raw", exist_ok=True)
        file_path = f"{cur_dir}/data/raw/yellow_tripdata_2024-01.parquet"

        # write parquet file
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"file saved to {file_path} succesfully.")
    except Exception as e:
        print(e)
