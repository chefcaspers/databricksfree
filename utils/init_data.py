import os
import shutil
from pathlib import Path

# Configuration
CATALOG_NAME = 'gk_demo'
SCHEMA_NAME = 'default'
VOLUME_NAME = 'raw_data'

# Create Unity Catalog objects
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_NAME}")
print(f"✅ Unity Catalog objects ready")

VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"
DATA_DIR = Path('../data')

# Copy everything in DATA_DIR to VOLUME_PATH
def copy_data_dir_to_volume():
    for item in DATA_DIR.iterdir():
        dest = Path(VOLUME_PATH) / item.name
        if item.is_dir():
            if not dest.exists():
                shutil.copytree(item, dest)
        else:
            shutil.copy2(item, dest)
    print(f"✅ Copied all contents from {DATA_DIR} to {VOLUME_PATH}")

copy_data_dir_to_volume()