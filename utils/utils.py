import os
import shutil
from pathlib import Path

# Default configuration
DEFAULT_CATALOG_NAME = "gk_demo"
DEFAULT_SCHEMA_NAME = "default"
DEFAULT_VOLUME_NAME = "raw_data"
DEFAULT_CHECKPOINTS_NAME = "checkpoints"
DEFAULT_ALL_EVENTS_TABLE_NAME = (
    f"{DEFAULT_CATALOG_NAME}.{DEFAULT_SCHEMA_NAME}.all_events"
)
DEFAULT_ORDER_DELIVERY_TIMES_VIEW_NAME = f"{DEFAULT_CATALOG_NAME}.{DEFAULT_SCHEMA_NAME}.order_delivery_times_per_location_view"

# Default data paths
DATA_DIR = Path("data")
DEFAULT_SOURCE_GZ_FILE = os.path.join(DATA_DIR, "raw_events.json.gz")
DEFAULT_VOLUME_PATH = (
    f"/Volumes/{DEFAULT_CATALOG_NAME}/{DEFAULT_SCHEMA_NAME}/{DEFAULT_VOLUME_NAME}"
)
DEFAULT_VOLUME_GZ_FILE = f"{DEFAULT_VOLUME_PATH}/raw_events.json.gz"


def setup_catalog_and_volume(
    spark,
    catalog_name=DEFAULT_CATALOG_NAME,
    schema_name=DEFAULT_SCHEMA_NAME,
    volume_name=DEFAULT_VOLUME_NAME,
    checkpoints_name=DEFAULT_CHECKPOINTS_NAME
):
    """
    Create Unity Catalog objects if they do not exist.
    """
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_anem}.{schema_name}.{checkpoints_name})
    print(f"✅ Unity Catalog objects ready: {catalog_name}.{schema_name}.{volume_name}")


def copy_raw_data_to_volume(
    source_gz_file=DEFAULT_SOURCE_GZ_FILE, volume_gz_file=DEFAULT_VOLUME_GZ_FILE
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
    table_name=DEFAULT_ALL_EVENTS_TABLE_NAME,
):
    """
    Read the gzipped JSON file from the volume and write it as a Delta table.
    """
    if not spark.catalog.tableExists(table_name):
        df = spark.read.json(volume_gz_file)
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"✅ Table {table_name} created or already exists")


def initialize_order_delivery_times_view(
    spark, table_name=DEFAULT_ORDER_DELIVERY_TIMES_VIEW_NAME
):
    spark.sql(f"""
    CREATE OR REPLACE VIEW {DEFAULT_ORDER_DELIVERY_TIMES_VIEW_NAME} AS
WITH order_times AS (
  SELECT
    order_id,
    location,
    MAX(CASE WHEN event_type = 'order_created' THEN try_to_timestamp(ts) END) AS order_created_time,
    MAX(CASE WHEN event_type = 'delivered' THEN try_to_timestamp(ts) END) AS delivered_time
  FROM
    gk_demo.default.all_events
  GROUP BY
    order_id,
    location
),
total_order_times AS (
  SELECT
    order_id,
    location,
    (UNIX_TIMESTAMP(delivered_time) - UNIX_TIMESTAMP(order_created_time)) / 60 AS total_order_time_minutes
  FROM
    order_times
  WHERE
    order_created_time IS NOT NULL
    AND delivered_time IS NOT NULL
)
SELECT
  location,
  PERCENTILE(total_order_time_minutes, 0.50) AS P50,
  PERCENTILE(total_order_time_minutes, 0.75) AS P75,
  PERCENTILE(total_order_time_minutes, 0.99) AS P99
FROM
  total_order_times
GROUP BY
  location""")


def initialize_dimension_tables(
    spark,
    data_dir=DATA_DIR,
    catalog=DEFAULT_CATALOG_NAME,
    schema=DEFAULT_SCHEMA_NAME
):
    brands_df = spark.read.parquet(str(data_dir / "brands.parquet"))
    menus_df = spark.read.parquet(str(data_dir / "menus.parquet"))
    categories_df = spark.read.parquet(str(data_dir / "categories.parquet"))
    items_df = spark.read.parquet(str(data_dir / "items.parquet"))

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.brand (
        id   BIGINT NOT NULL,
        name STRING NOT NULL,
        CONSTRAINT brand_pk PRIMARY KEY (id)
    )""")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.menu (
        id       BIGINT NOT NULL,
        name     STRING NOT NULL,
        brand_id BIGINT NOT NULL,

        CONSTRAINT menu_pk        PRIMARY KEY (id),
        CONSTRAINT menu_brand_fk  FOREIGN KEY (brand_id)
            REFERENCES {catalog}.{schema}.brand(id)
    )""")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.category (
        id       BIGINT NOT NULL,
        name     STRING NOT NULL,
        menu_id  BIGINT NOT NULL,
        brand_id BIGINT NOT NULL,

        CONSTRAINT category_pk         PRIMARY KEY (id),
        CONSTRAINT category_menu_fk    FOREIGN KEY (menu_id)
            REFERENCES {catalog}.{schema}.menu(id),
        CONSTRAINT category_brand_fk   FOREIGN KEY (brand_id)
            REFERENCES {catalog}.{schema}.brand(id)
    )""")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.item (
        id          BIGINT NOT NULL,
        name        STRING NOT NULL,
        description STRING,
        price       DOUBLE NOT NULL,
        image_data  STRING,

        brand_id    BIGINT NOT NULL,
        menu_id     BIGINT NOT NULL,
        category_id BIGINT NOT NULL,

        CONSTRAINT item_pk            PRIMARY KEY (id),
        CONSTRAINT item_brand_fk      FOREIGN KEY (brand_id)
            REFERENCES {catalog}.{schema}.brand(id),
        CONSTRAINT item_menu_fk       FOREIGN KEY (menu_id)
            REFERENCES {catalog}.{schema}.menu(id),
        CONSTRAINT item_category_fk   FOREIGN KEY (category_id)
            REFERENCES {catalog}.{schema}.category(id)
    )""")

def drop_gk_demo_catalog(spark, catalog_name=DEFAULT_CATALOG_NAME):
    spark.sql(f"DROP CATALOG IF EXISTS {catalog_name} CASCADE")


# USAGE
# From a notebook in the root directory, run:
## from utils.utils import setup_catalog_and_volume, drop_gk_demo_catalog, copy_raw_data_to_volume, initialize_events_table
## drop_gk_demo_catalog(spark)
## setup_catalog_and_volume(spark)
## copy_raw_data_to_volume()
## initialize_events_table(spark)
