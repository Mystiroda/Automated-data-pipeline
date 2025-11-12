"""
Data Analyzer Module
Performs statistical analysis and generates insights from cleaned data.
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import logging

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
from config import OUTPUTS_REPORTS

logger = logging.getLogger(__name__)


def analyze_data(df, analysis_type="basic"):
    """
    Analyze dataset and generate reports
    Args:
        df: DataFrame to analyze
        analysis_type: "basic" or "comprehensive"
    Returns:
        Analysis reports
    """
    analysis_results = {}

    # Basic analysis (always done)
    analysis_results['shape'] = df.shape
    analysis_results['column_types'] = df.dtypes.astype(str).to_dict()
    analysis_results['missing_values'] = df.isnull().sum().to_dict()

    # Numeric analysis
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if not numeric_cols.empty:
        analysis_results['numeric_stats'] = df[numeric_cols].describe().to_dict()
        analysis_results['correlation'] = df[numeric_cols].corr().to_dict()

    # Categorical analysis
    categorical_cols = df.select_dtypes(exclude=[np.number]).columns
    categorical_stats = {}
    for col in categorical_cols:
        if df[col].nunique() < 50:
            categorical_stats[col] = {
                'unique_count': df[col].nunique(),
                'most_common': df[col].value_counts().head().to_dict()
            }
    analysis_results['categorical_stats'] = categorical_stats

    # Save reports
    save_analysis_reports(analysis_results)

    return analysis_results


def save_analysis_reports(results):
    """
    Save analysis results to CSV files
    Args:
        results: Analysis results
    """
    # Numeric summary
    if 'numeric_stats' in results:
        numeric_df = pd.DataFrame(results['numeric_stats'])
        numeric_df.to_csv(OUTPUTS_REPORTS / "numeric_summary.csv")

    # Correlation matrix
    if 'correlation' in results:
        corr_df = pd.DataFrame(results['correlation'])
        corr_df.to_csv(OUTPUTS_REPORTS / "correlation_matrix.csv")

    logger.info(f"Analysis reports saved to {OUTPUTS_REPORTS}")


def generate_insights(df):
    """
    Generate human-readable insights
    Args:
        df: Dataframe
    """
    insights = []

    # Basic info
    insights.append(f"Dataset has {df.shape[0]:,} rows and {df.shape[1]} columns")

    # Missing values
    missing_total = df.isnull().sum().sum()
    if missing_total > 0:
        insights.append(f"Found {missing_total} missing values")

    # Numeric columns insights
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        insights.append(f"Numeric columns: {', '.join(numeric_cols)}")

    return insights