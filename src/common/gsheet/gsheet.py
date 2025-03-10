import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

from src.config.logging_conf import logger


def read_google_sheet(spreadsheet_id: str, sheet_name: str, credentials_path: str) -> pd.DataFrame:
    """
    Reads data from a Google Sheet with logging for tracing execution.

    :param spreadsheet_id: ID of the Google Spreadsheet
    :param sheet_name: Name of the sheet to read from
    :param credentials_path: Path to the service account JSON credentials
    :return: List of rows (each row is a list of cell values)
    """
    logger.info("Starting to read Google Sheet: %s", spreadsheet_id)

    # Define scope
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    # Authenticate with service account
    logger.info("Authenticating with service account credentials...")
    creds = Credentials.from_service_account_file(credentials_path, scopes=scope)
    client = gspread.authorize(creds)

    # Open the Google Sheet
    logger.info(f"Opening the spreadsheet: {spreadsheet_id}")
    sheet = client.open_by_key(spreadsheet_id).worksheet(sheet_name)

    # Get all data
    logger.info(f"Fetching data from sheet: {sheet_name}")
    data = sheet.get_all_records()

    logger.info(f"Successfully retrieved {len(data)} rows from the sheet")
    df = pd.DataFrame(data)
    return df
