"""
Data Downloader Module
Handles downloading datasets from URLs or loading from local files.
"""
import sys
import requests
import pandas as pd
import logging
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config import DATA_RAW, DEFAULT_DATASETS
logger = logging.getLogger(__name__)


def download_dataset(url=None, dataset_name=None, save_path=None):
    """
    Download dataset from URL or predefined dataset name
    Args:
        url: Direct URL to CSV file
        dataset_name: Name of predefined dataset ('titanic', 'iris')
        save_path: Custom path to save file
    Returns:
        Path to downloaded file or None if failed
    """
    try:
        #getting the url
        if url is None and dataset_name:
            url = DEFAULT_DATASETS.get(dataset_name)
            if url is None:
                raise ValueError(f"Unknown dataset: {dataset_name}")
        elif url is None:
            url = input("Enter dataset URL: ").strip()
            if not url:
                url = DEFAULT_DATASETS['titanic']

        logger.info(f"Downloading from: {url}")

        response = requests.get(url, timeout=10)
        response.raise_for_status()

        #determining file name for the downloaded file
        if save_path:
            filename = save_path
        else:
            original_name = Path(url.split('/')[-1])
            if original_name.suffix == '.csv':
                filename = DATA_RAW / original_name.name
            else:
                filename = DATA_RAW / "downloaded_dataset.csv"

        with open(filename, "wb") as f:
            f.write(response.content)

        logger.info(f"Downloaded to: {filename}")
        return filename

    except Exception as e:
        logger.error(f"Download failed: {e}")
        return None


def load_downloaded_data(file_path):
    """
    Load downloaded CSV into DataFrame
    Args:
        file_path: Path to CSV file
    Returns:
        None
    """
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded data: {df.shape[0]} rows, {df.shape[1]} columns")
        return df
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        return None

if __name__ == "__main__":
    download_dataset()