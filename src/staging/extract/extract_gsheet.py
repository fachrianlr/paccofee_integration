from datetime import datetime

import pandas as pd

from src.common.gsheet.gsheet import read_google_sheet
from src.common.log.etl_log import insert_etl_log, read_etl_log
from src.config.common import get_abs_path
from src.config.logging_conf import logger

CREDS_PATH = get_abs_path("keys/google-key.json")


def extract_source_gsheet(table_name: str, spreadsheet_id: str, sheet_name: str) -> pd.DataFrame | None:
    """
    This function is used to extract data from a Google Sheet.
    """
    logger.info(
        f"Start extract_source_gsheet, Extracting data from Google Sheet, spreadsheet_id: {spreadsheet_id}, sheet_name: {sheet_name}, credentials_path: {CREDS_PATH}")

    log_msg = {}
    step = "staging"
    process = "load"
    source = "gsheet"
    try:
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
        df = read_google_sheet(spreadsheet_id, sheet_name, CREDS_PATH)
        df["created_at"] = pd.to_datetime(df["created_at"])
        df = df[df["created_at"] > etl_date]

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
        return None
    finally:
        logger.info(log_msg)
        insert_etl_log(log_msg)
