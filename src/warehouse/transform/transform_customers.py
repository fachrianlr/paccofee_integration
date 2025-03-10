from datetime import datetime

import pandas as pd

from src.common.error.error_etl import handle_etl_error
from src.common.log.etl_log import insert_etl_log
from src.config.logging_conf import logger


def transform_customers(data: pd.DataFrame, table_name: str) -> pd.DataFrame | None:
    """
    This function is used to transform data customers from staging database to the data warehouse.
    """
    log_msg = {}
    step = "warehouse"
    process = "transformation"
    source = "staging"

    try:
        # rename column category to category_nk
        data = data.rename(columns={'customer_id': 'nk_customer_id'})

        log_msg = {
            "step": step,
            "process": process,
            "source": source,
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "success"
        }

        return data
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
