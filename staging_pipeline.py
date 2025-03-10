from src.config.logging_conf import logger
from src.staging.extract.extract_db import extract_source_db
from src.staging.extract.extract_gsheet import extract_source_gsheet
from src.staging.load.load_db import load_stg_db

if __name__ == '__main__':
    logger.info("===== Staging Pipeline Started =====")

    logger.info("Extracting Source & Load of Data Customer")
    df = extract_source_db("public", "customers")
    load_stg_db(data=df, schema="public", table_name="customers", idx_name="customer_id",
                source="database")
    logger.info("Done Extracting Source & Load of Data Customer")

    logger.info("Extracting Source & Load of Data Employees")
    df = extract_source_db("public", "employees")
    load_stg_db(data=df, schema="public", table_name="employees", idx_name="employee_id",
                source="database")
    logger.info("Done Extracting Source & Load of Data Employees")

    logger.info("Extracting Source & Load of Data Inventory Tracking")
    df = extract_source_db("public", "inventory_tracking")
    load_stg_db(data=df, schema="public", table_name="inventory_tracking", idx_name="tracking_id",
                source="database")
    logger.info("Done Extracting Source & Load of Data Inventory Tracking")

    logger.info("Extracting Source & Load of Data Order Details")
    df = extract_source_db("public", "order_details")
    load_stg_db(data=df, schema="public", table_name="order_details", idx_name="order_detail_id",
                source="database")
    logger.info("Done Extracting Source & Load of Data Order Details")

    logger.info("Extracting Source & Load of Data Orders")
    df = extract_source_db("public", "orders")
    load_stg_db(data=df, schema="public", table_name="orders", idx_name="order_id",
                source="database")
    logger.info("Done Extracting Source & Load of Data Order")

    logger.info("Extracting Source & Load of Data Products")
    df = extract_source_db("public", "products")
    load_stg_db(data=df, schema="public", table_name="products", idx_name="product_id",
                source="database")
    logger.info("Done Extracting Source & Load of Data Products")

    logger.info("Extracting Source & Load of Data Store Branch")
    spreadsheet_id = "1GGAAAID0RYqeL7dg_xGYZ6VS5dSrpxEFXv74J_CRE_w"
    sheet_name = "store_branch"
    df = extract_source_gsheet("store_branch", spreadsheet_id, sheet_name)
    load_stg_db(data=df, schema="public", table_name="store_branch", idx_name="store_id", source="gsheet")
    logger.info("Done Extracting Source & Load of Data Store Branch")
