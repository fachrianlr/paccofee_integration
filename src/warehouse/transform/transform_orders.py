import os
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

from src.common.error.error_etl import handle_etl_error
from src.common.helper.pandas_util import extract_table
from src.common.log.etl_log import insert_etl_log
from src.config.logging_conf import logger

load_dotenv()

DWH_DB_URI = os.environ.get("DWH_DB_URI")
STG_DB_URI = os.environ.get("STG_DB_URI")


def transform_orders(data: pd.DataFrame, table_name: str) -> pd.DataFrame | None:
    """
    This function is used to transform data orders from staging database to the data warehouse.
    """
    log_msg = {}
    step = "warehouse"
    process = "transformation"
    source = "staging"

    try:
        # merge dataframe with other dataframes

        order_details = extract_table(STG_DB_URI, "public", "order_details")
        dim_employees = extract_table(DWH_DB_URI, "public", "dim_employees")
        dim_customers = extract_table(DWH_DB_URI, "public", "dim_customers")
        dim_products = extract_table(DWH_DB_URI, "public", "dim_products")
        dim_date = extract_table(DWH_DB_URI, "public", "dim_date")

        data['order_date'] = pd.to_datetime(data['order_date']).dt.date
        merged_df = \
            data.merge(order_details[
                           ['order_detail_id', 'order_id', 'product_id', 'quantity', 'unit_price', 'subtotal']],
                       left_on='order_id', right_on='order_id', how='inner') \
                .merge(dim_employees[['nk_employee_id', 'sk_employee_id']], left_on='employee_id',
                       right_on='nk_employee_id', how='left') \
                .merge(dim_customers[['nk_customer_id', 'sk_customer_id']], left_on='customer_id',
                       right_on='nk_customer_id', how='left') \
                .merge(dim_products[['nk_product_id', 'sk_product_id']], left_on='product_id',
                       right_on='nk_product_id', how='left') \
                .merge(dim_date[['date_actual', 'date_id']], left_on='order_date',
                       right_on='date_actual', how='left') \
                [['order_detail_id', 'sk_employee_id', 'sk_customer_id', 'sk_product_id', 'date_id', 'total_amount',
                  'quantity', 'unit_price', 'subtotal', 'payment_method', 'order_status', 'created_at']]

        merged_df = merged_df.rename(columns={'date_id': 'order_date', 'order_detail_id': 'nk_order_id'})

        log_msg = {
            "step": step,
            "process": process,
            "source": source,
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "success"
        }

        return merged_df
    except Exception as e:
        log_msg = {
            "step": step,
            "process": process,
            "source": source,
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "failed",
            "error_msg": str(e)
        }
        # Handling error: save data to Object Storage
        try:
            handle_etl_error(data=data, bucket_name='error-paccafe', table_name=table_name, process=process)
        except Exception as e:
            logger.error(e)
        return None
    finally:
        # Save the log message
        logger.info(log_msg)
        insert_etl_log(log_msg)
