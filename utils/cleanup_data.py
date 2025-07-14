import os

# Paths and table name
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
JSON_FILE = os.path.join(DATA_DIR, 'raw_events.json')
TABLE_NAME = 'gk_demo.default.events_table'

# Delete the unzipped JSON file if it exists
def delete_unzipped_file():
    if os.path.exists(JSON_FILE):
        print(f"Deleting {JSON_FILE}...")
        os.remove(JSON_FILE)
    else:
        print(f"{JSON_FILE} does not exist. Skipping file deletion.")

# Drop the Unity Catalog table if it exists
def drop_uc_table():
    try:
        spark
    except NameError:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
    print(f"Dropping table {TABLE_NAME} if it exists...")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    print("Table cleanup complete.")

def main():
    delete_unzipped_file()
    drop_uc_table()
    print("Cleanup complete.")

if __name__ == "__main__":
    main() 