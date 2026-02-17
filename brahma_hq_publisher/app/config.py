import os

# Required
GCS_BUCKET = os.getenv("GCS_BUCKET", "brahma-hq-prod")

# Inputs
MASTER_ROOT = os.getenv("MASTER_ROOT", "03_MasterData/modules")         # contains per-module JSON
STANDARDS_ROOT = os.getenv("STANDARDS_ROOT", "02_Databases/Standards")  # yaml files

# Outputs
RELEASE_ROOT = os.getenv("RELEASE_ROOT", "04_Releases")
ACTIVE_OBJECT = os.getenv("ACTIVE_OBJECT", f"{RELEASE_ROOT}/ACTIVE")

# Release metadata
SCHEMA_VERSION = os.getenv("SCHEMA_VERSION", "1.0.0")
PRODUCT_DB_NAME = os.getenv("PRODUCT_DB_NAME", "brahma_products.sqlite")

# Signing (optional; if you want signed URLs from this service)
SIGN_URL_MINUTES = int(os.getenv("SIGN_URL_MINUTES", "60"))
