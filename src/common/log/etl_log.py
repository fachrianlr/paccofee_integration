import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from src.config.db_conf import get_query_by_id
from src.config.logging_conf import logger

load_dotenv()

LOG_DB_URI = os.getenv("LOG_DB_URI")
ETL_LOG_PATH = "src/common/sql/etl_log.xml"


def insert_etl_log(log_msg: dict):
    """
    This function is used to save the log message to the database.
    """
    logger.info(f"insert_etl_log : {log_msg}")

    try:
        # create connection to database
        conn = create_engine(LOG_DB_URI)

        # convert dictionary to dataframe
        df_log = pd.DataFrame([log_msg])

        # extract data log
        df_log.to_sql(name="etl_log",  # Your log table
                      con=conn,
                      if_exists="append",
                      index=False)
    except Exception as e:
        logger.error(f"Can't save your log message. Cause: {str(e)}")


def read_etl_log(filter_params: dict):
    """
    This function read_etl_log that reads log information from the etl_log table and extracts the maximum etl_date for a specific process, step, table name, and status.
    """

    logger.info(f"read_etl_log : {filter_params}")
    try:
        # create connection to database
        conn = create_engine(LOG_DB_URI)

        sql_str = get_query_by_id(ETL_LOG_PATH, "getMaxEtlDate")
        # Execute the query with pd.read_sql
        df = pd.read_sql(sql=sql_str, con=conn, params=filter_params)

        # return extracted data
        return df
    except Exception as e:
        logger.error(f"Can't execute your query. Cause: {str(e)}")
