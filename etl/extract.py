import requests
from pathlib import Path
import os


def extract(year: int, month: int) -> None:
    '''
    Extract parquet file from NYC taxi trip record website
    <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>.
    and stores raw file in a local data directoryk. 

    Parameters:
        year (int): year of data to extract.
        month (int): month of data to extract.

    Returns:
        None
    '''
    try:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
        print(f"extracting file from {url}...")
        response = requests.get(url)

        # create directory to store raw data
        cur_dir = Path.cwd()
        os.makedirs(f"{cur_dir}/data/raw", exist_ok=True)
        file_path = f"{cur_dir}/data/raw/yellow_tripdata_{year}-{month:02d}.parquet"

        # write parquet file
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"file saved to {file_path} succesfully.")
    except Exception as e:
        print(e)
