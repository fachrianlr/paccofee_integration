import xml.etree.ElementTree as elementTree

import sqlalchemy
from sqlalchemy import create_engine, text

from src.config.common import get_abs_path
from src.config.logging_conf import logger
from src.config.sentry_conf import SENTRY_LOGGING


def connect_to_db(db_uri):
    try:
        # Create a SQLAlchemy engine
        engine = create_engine(db_uri)
        return engine
    except Exception as e:
        SENTRY_LOGGING.capture_exception(e)
        logger.error(f"Error connecting to the database: {e}")
        raise e


def get_query_by_id(file_path, query_id):
    try:
        abs_file_path = get_abs_path(file_path)

        if not abs_file_path.exists():
            raise FileNotFoundError(f"File not found: {abs_file_path}")

        tree = elementTree.parse(abs_file_path)
        root = tree.getroot()

        query_element = root.find(f".//query[@id='{query_id}']")
        if query_element is not None and query_element.text:
            sql_text = sqlalchemy.text(query_element.text.strip())
            return sql_text
        else:
            raise ValueError(f"Query with ID '{query_id}' not found")

    except Exception as e:
        logger.error(f"Error loading query '{query_id}' from XML: {e}", exc_info=True)
        raise
