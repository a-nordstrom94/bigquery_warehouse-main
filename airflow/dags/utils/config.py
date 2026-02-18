# dags/utils/config.py
import os

# Project Globals
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCS_BUCKET = os.environ.get('GCS_BUCKET')
GCP_LOCATION = os.environ.get('GCP_LOCATION', 'US')
GCP_CONN_ID = "google_cloud_default"

# Dataset Names
BRONZE_DATASET = os.environ.get('BRONZE_DATASET', 'olist_bronze')
SILVER_DATASET = os.environ.get('SILVER_DATASET', 'olist_silver')
GOLD_DATASET = os.environ.get('GOLD_DATASET', 'olist_gold')

# Ingestion Registry
FILE_MAPPING = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocations",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "reviews",
    "olist_orders_dataset.csv": "orders",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_name_translation"
}

# Model Registry
STAGING_VIEWS = [
    'stg_olist__customers',
    'stg_olist__orders',
    'stg_olist__order_items',
    'stg_olist__products',
    'stg_olist__sellers',
    'stg_olist__order_payments',
    'stg_olist__reviews',
    'stg_olist__geolocations',
    'stg_olist__product_category_translation',
]

GOLD_TABLES = [
    'dim_customers',
    'dim_products',
    'dim_sellers',
    'fct_orders',
    'fct_order_items',
    'customer_metrics',
    'product_performance',
    'seller_performance',
]