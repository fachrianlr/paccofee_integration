import re

import pandas as pd


def map_columns(source_df: pd.DataFrame, target_df: pd.DataFrame, mapping_id: str, key: str,
                   value: str) -> pd.DataFrame:
    """ Map the date to its corresponding id in the target_df """

    data_mapping = dict(zip(target_df[key], target_df[value]))
    source_df[mapping_id] = source_df[mapping_id].map(data_mapping)
    return source_df

def extract_numeric(price_str):
    """ Extract numeric values from a string """
    return re.sub(r"[^\d.]", "", price_str)
