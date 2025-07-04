import requests
from pathlib import Path
import os


def extract_data() -> str | None:
    '''
    Extract parquet file from NYC taxi trip record website
    <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>.
    and returns the path where the file is saved.

    Parameters:
        None

    Returns:
       str: absolute file path. 
    '''
    try:
        url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
        print(f"extracting file from {url}...")
        response = requests.get(url)
        # get root directory
        root_dir = Path(__file__).resolve().parents[1] 
        # ensure data directory exists
        os.makedirs(f"{root_dir}/data", exist_ok=True)
        file_path = f"{root_dir}/data/yellow_tripdata_2024-01.parquet"
        # write parquet file
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"file saved to {file_path} succesfully.")
        return file_path
    except Exception as e:
        print(e)
