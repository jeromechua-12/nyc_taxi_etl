from data_extraction.extract import extract_data
from data_cleaning.clean import clean_data
from data_loading.load import create_table, insert_data


def main():
    create_table()
    file_path = extract_data()
    if not file_path:
        print("No file extracted.")
        return
    df = clean_data(file_path)
    insert_data(df)


if __name__ == "__main__":
    main()
