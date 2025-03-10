import os
from datetime import datetime

from dotenv import load_dotenv
from pangres import upsert
from sqlalchemy import create_engine

from src.common.error.error_etl import handle_etl_error
from src.common.log.etl_log import insert_etl_log
from src.config.logging_conf import logger

load_dotenv()

STG_DB_URI = os.getenv("STG_DB_URI")


def load_stg_db(data, schema: str, table_name: str, idx_name: str, source):
    """ This function is used to load data to the stg database. """

    logger.info(f"Start load_stg_db, Load data to {schema}.{table_name}")
    logger.info(f"Total rows to be loaded: {len(data)}")
    log_msg = {}
    step = "staging"
    process = "load"

    try:
        # create connection to database
        conn = create_engine(STG_DB_URI)

        # set data index or primary key
        data = data.set_index(idx_name)

        # Do upsert (Update for existing data and Insert for new data)
        upsert(con=conn,
               df=data,
               table_name=table_name,
               schema=schema,
               if_row_exists="update")

        # create success log message
        log_msg = {
            "step": step,
            "process": process,
            "status": "success",
            "source": source,
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
        }
    except Exception as e:

        # create fail log message
        log_msg = {
            "step": step,
            "process": process,
            "status": "failed",
            "source": source,
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # Current timestamp
            "error_msg": str(e)
        }
        try:
            handle_etl_error(data=data, bucket_name='error-paccafe', table_name=table_name, process='load')
        except Exception as e:
            logger.error(str(e))
    finally:
        logger.info(log_msg)
        insert_etl_log(log_msg)
