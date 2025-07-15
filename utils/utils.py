import os
import shutil
from pathlib import Path

# Default configuration
DEFAULT_CATALOG_NAME = 'gk_demo'
DEFAULT_SCHEMA_NAME = 'default'
DEFAULT_VOLUME_NAME = 'raw_data'
DEFAULT_ALL_EVENTS_TABLE_NAME = f'{DEFAULT_CATALOG_NAME}.{DEFAULT_SCHEMA_NAME}.all_events'

# Default data paths
DATA_DIR = Path('/data')
DEFAULT_SOURCE_GZ_FILE = os.path.join(DATA_DIR, 'raw_events.json.gz')
DEFAULT_VOLUME_PATH = f"/Volumes/{DEFAULT_CATALOG_NAME}/{DEFAULT_SCHEMA_NAME}/{DEFAULT_VOLUME_NAME}"
DEFAULT_VOLUME_GZ_FILE = f"{DEFAULT_VOLUME_PATH}/raw_events.json.gz"


def setup_catalog_and_volume(spark, catalog_name=DEFAULT_CATALOG_NAME, schema_name=DEFAULT_SCHEMA_NAME, volume_name=DEFAULT_VOLUME_NAME):
    """
    Create Unity Catalog objects if they do not exist.
    """
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")
    print(f"✅ Unity Catalog objects ready: {catalog_name}.{schema_name}.{volume_name}")


def copy_to_volume(
    source_gz_file=DEFAULT_SOURCE_GZ_FILE,
    volume_gz_file=DEFAULT_VOLUME_GZ_FILE
):
    """
    Copy the gzipped JSON file to the specified volume path if it does not already exist.
    """
    if not os.path.exists(volume_gz_file):
        print(f"Copying {source_gz_file} to volume {volume_gz_file}...")
        shutil.copy2(source_gz_file, volume_gz_file)
        print("✅ Copied to volume")
    else:
        print(f"{volume_gz_file} already exists in volume. Skipping copy.")


def initialize_events_table(
    spark,
    volume_gz_file=DEFAULT_VOLUME_GZ_FILE,
    table_name=DEFAULT_ALL_EVENTS_TABLE_NAME
):
    """
    Read the gzipped JSON file from the volume and write it as a Delta table.
    """
    df = spark.read.json(volume_gz_file)
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"✅ Table {table_name} created")

def drop_gk_demo_catalog(spark, catalog_name=DEFAULT_CATALOG_NAME):
    spark.sql(f"DROP CATALOG IF EXISTS {catalog_name} CASCADE")