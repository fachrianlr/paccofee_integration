import os
from pathlib import Path

# Dynamically determine the parent folder (my_project)
PARENT_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))


def get_abs_path(file_path):
    abs_path = os.path.join(PARENT_FOLDER, file_path)
    return Path(abs_path).resolve()
