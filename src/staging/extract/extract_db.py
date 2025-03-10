import os
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from src.common.log.etl_log import read_etl_log, insert_etl_log
from src.config.logging_conf import logger

load_dotenv()

SRC_DB_URI = os.getenv("SRC_DB_URI")


def extract_source_db(schema_name: str, table_name: str) -> pd.DataFrame | None:
    """
    This function is used to extract data from the staging database.
    """
    logger.info(f"Start extract_source_db, Extracting data from {schema_name}.{table_name}")
    log_msg = {}
    step = "staging"
    process = "extraction"
    source = "database"

    try:
        # create connection to database staging
        conn = create_engine(SRC_DB_URI)

        # Get date from previous process
        filter_log = {"step": step,
                      "table_name": table_name,
                      "process": process,
                      "source": source,
                      "status": "success"}
        etl_date = read_etl_log(filter_log)

        # If no previous extraction has been recorded (etl_date is empty), set etl_date to '1111-01-01' indicating the initial load.
        # Otherwise, retrieve data added since the last successful extraction (etl_date).
        if etl_date['max'][0] is None:
            etl_date = '1111-01-01'
        else:
            etl_date = etl_date[max][0]

        logger.info("Last sync date: " + str(etl_date))
        # Constructs a SQL query to select all columns from the specified table_name where created_at is greater than etl_date.
        query = f"SELECT * FROM {schema_name}.{table_name} WHERE created_at > %s::timestamp"

        # Execute the query with pd.read_sql
        df = pd.read_sql(sql=query, con=conn, params=(etl_date,))
        log_msg = {
            "step": step,
            "process": process,
            "source": source,
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "success"
        }
        logger.info(f"Total rows extracted: {len(df)}")
        return df
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
        logger.error(e)
        return None
    finally:
        # Save the log message
        logger.info(log_msg)
        insert_etl_log(log_msg)
