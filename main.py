#!/usr/bin/env python3
"""
Automated Data Pipeline
Runs all the module together to produce results
"""

import logging
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from config import PROJECT_ROOT
from src.data_downloader import download_dataset, load_downloaded_data
from src.data_cleaner import clean_dataset, clean_dataframe
from src.database_handler import dataframe_to_sqlite, list_tables
from src.data_analyzer import analyze_data, generate_insights
from src.plot_generator import generate_all_plots

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PROJECT_ROOT / 'pipeline.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def run_pipeline(dataset_url=None, dataset_name='titanic'):
    """
    Run the complete data pipeline
    Args:
        dataset_url: Direct URL to dataset
        dataset_name: Name of predefined dataset
    """
    logger.info("Starting Automated Data Pipeline")

    try:
        #Download data
        logger.info("Downloading dataset...")
        data_file = download_dataset(url=dataset_url, dataset_name=dataset_name)
        if not data_file:
            logger.error("Failed to download dataset")
            return False

        df_raw = load_downloaded_data(data_file)
        if df_raw is None:
            logger.error("Failed to load dataset")
            return False

        logger.info(f"Downloaded: {df_raw.shape[0]} rows, {df_raw.shape[1]} columns")

        # Clean data
        logger.info("Cleaning data...")
        cleaned_file = clean_dataset(data_file)
        if not cleaned_file:
            logger.error("Failed to clean data")
            return False

        df_clean = load_downloaded_data(cleaned_file)
        logger.info(f"Cleaned data saved to: {cleaned_file}")

        # Store in database
        logger.info("Storing in database...")
        table_name = dataframe_to_sqlite(df_clean, Path(cleaned_file).stem)
        if not table_name:
            logger.error("Failed to store in database")
            return False

        tables = list_tables()
        logger.info(f"Data stored in table: {table_name}")
        logger.info(f"Database tables: {tables}")

        # Analyze data
        logger.info("Analyzing data...")
        analysis_results = analyze_data(df_clean)
        insights = generate_insights(df_clean)

        for insight in insights:
            logger.info(f"    {insight}")

        #Generate plots
        logger.info("Step 5: Generating visualizations...")
        plot_count = generate_all_plots(df_clean)
        logger.info(f"Generated {plot_count} plots")

        # Final summary
        logger.info("Pipeline completed successfully!")
        logger.info(f"""
        PIPELINE SUMMARY:
        • Raw data: {data_file}
        • Cleaned data: {cleaned_file}
        • Database table: {table_name}
        • Analysis reports: outputs/reports/
        • Visualizations: outputs/plots/ ({plot_count} plots)
        • Log file: pipeline.log
        """)

        return True

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        return False


if __name__ == "__main__":
    # Simple command line interface
    import argparse

    parser = argparse.ArgumentParser(description='Run automated data pipeline')
    parser.add_argument('--url', help='Dataset URL', default=None)
    parser.add_argument('--dataset', help='Predefined dataset',
                        choices=['titanic', 'iris'], default='titanic')

    args = parser.parse_args()

    success = run_pipeline(dataset_url=args.url, dataset_name=args.dataset)

    if success:
        print("\n Pipeline completed! Check the outputs:")
        print("   data/ - Raw and processed data")
        print("   outputs/plots/ - Generated visualizations")
        print("   outputs/reports/ - Analysis reports")
        print("   pipeline.log - Detailed execution log")
    else:
        print("\n Pipeline failed. Check pipeline.log for details.")

    sys.exit(0 if success else 1)