"""
Data Cleaner Module
Handles data cleaning operations
"""
import sys
import pandas as pd
import numpy as np
from pathlib import Path
import logging

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
from config import DATA_PROCESSED, DEFAULT_CLEANING

logger = logging.getLogger(__name__)


def clean_dataframe(df, strategy=None):
    """
    Clean DataFrame with configurable strategy
    Args:
        df: Input DataFrame
        strategy: Cleaning strategy dictionary
    Returns:
        Cleaned DataFrame
    """
    if strategy is None:
        strategy = DEFAULT_CLEANING

    df_clean = df.copy()
    initial_shape = df_clean.shape

    logger.info(f"Starting cleaning: {initial_shape}")

    # Remove duplicates
    if strategy.get('remove_duplicates', True):
        before = len(df_clean)
        df_clean = df_clean.drop_duplicates()
        removed = before - len(df_clean)
        if removed > 0:
            logger.info(f"Removed {removed} duplicates")

    # Clean column names
    if strategy.get('strip_columns', True):
        df_clean.columns = df_clean.columns.str.strip().str.replace(' ', '_').str.lower()

    # Handle missing values
    numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
    categorical_cols = df_clean.select_dtypes(exclude=[np.number]).columns

    for col in numeric_cols:
        if df_clean[col].isna().any():
            method = strategy.get('handle_numeric_na', 'median')
            if method == 'mean':
                df_clean[col] = df_clean[col].fillna(df_clean[col].mean())
            elif method == 'median':
                df_clean[col] = df_clean[col].fillna(df_clean[col].median())
            logger.info(f"Filled missing values in {col} with {method}")

    for col in categorical_cols:
        if df_clean[col].isna().any():
            method = strategy.get('handle_categorical_na', 'mode')
            if method == 'mode':
                mode_val = df_clean[col].mode()
                fill_val = mode_val[0] if not mode_val.empty else "Unknown"
                df_clean[col] = df_clean[col].fillna(fill_val)
            logger.info(f"Filled missing values in {col} with {method}")

    logger.info(f"Cleaning complete: {df_clean.shape}")
    return df_clean


def clean_dataset(input_path, output_path=None, strategy=None):
    """
    Complete cleaning pipeline for a CSV file
    Args:
        input_path: Path to CSV file
        output_path: Path to output CSV file
        strategy: Cleaning strategy dictionary
    Returns:
        Path to output CSV file or None if failed
    """
    try:
        df = pd.read_csv(input_path)
        df_clean = clean_dataframe(df, strategy)

        # Save cleaned data
        if output_path is None:
            input_name = Path(input_path).stem
            output_path = DATA_PROCESSED / f"{input_name}_cleaned.csv"

        df_clean.to_csv(output_path, index=False)
        logger.info(f"Saved cleaned data to: {output_path}")

        return output_path

    except Exception as e:
        logger.error(f"Cleaning pipeline failed: {e}")
        return None

if __name__ == "__main__":
    clean_dataset("../data/raw/titanic.csv")