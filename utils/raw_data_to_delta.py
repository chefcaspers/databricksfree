import os
import gzip

# Paths (relative to repo root)
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
GZ_FILE = os.path.join(DATA_DIR, 'raw_events.json.gz')
JSON_FILE = os.path.join(DATA_DIR, 'raw_events.json')
TABLE_NAME = 'gk_demo.default.events_table'

# Unzip the file if not already unzipped

def unzip_if_needed():
    if not os.path.exists(JSON_FILE):
        print(f"Unzipping {GZ_FILE} using Python gzip...")
        with gzip.open(GZ_FILE, 'rb') as f_in, open(JSON_FILE, 'wb') as f_out:
            f_out.write(f_in.read())
    else:
        print(f"{JSON_FILE} already exists. Skipping unzip.")

def main():
    unzip_if_needed()
    print(f"Reading JSON from {JSON_FILE}...")
    df = spark.read.json(JSON_FILE)
    columns = [
        "body", "event_id", "event_type", "gk_id",
        "location", "order_id", "sequence", "ts"
    ]
    # Select only the columns if they exist
    df = df.select([c for c in columns if c in df.columns])
    print(f"Writing to Unity Catalog table '{TABLE_NAME}'...")
    df.write.format("delta").mode("overwrite").saveAsTable(TABLE_NAME)
    print("Done.")

if __name__ == "__main__":
    main() 