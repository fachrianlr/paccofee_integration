from sqlalchemy import create_engine
import pandas as pd

from src.config.logging_conf import logger


def extract_table(db_uri: str, schema:str, table_name: str) -> pd.DataFrame:
    """
    Extracts data from the specified table in the database.

    Parameters:
    - db_uri (str): Database connection URI.
    - table_name (str): Name of the table to extract data from.

    Returns:
    - pd.DataFrame: Extracted data as a Pandas DataFrame.
    """
    try:
        # Create database engine
        engine = create_engine(db_uri)

        # Define query
        query = f"SELECT * FROM {schema}.{table_name}"

        # Use `with` statement to ensure proper resource handling
        with engine.connect() as conn:
            df = pd.read_sql(sql=query, con=conn)

        return df

    except Exception as e:
        logger.error(f"Error extracting data from {table_name}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure
