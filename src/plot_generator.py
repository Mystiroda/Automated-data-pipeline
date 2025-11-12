"""
Plot Generator Module
Generates visualizations for data analysis.
"""
import sys
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import logging

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
from config import OUTPUTS_PLOTS

logger = logging.getLogger(__name__)


def setup_plotting():
    """Configure matplotlib for consistent plotting"""
    plt.style.use('seaborn-v0_8')
    sns.set_palette("husl")
    plt.rcParams['figure.figsize'] = (10, 6)


def create_distribution_plots(df, max_columns=8):
    """Create distribution plots for numeric columns"""
    setup_plotting()
    numeric_columns = df.select_dtypes(include=['number']).columns

    # Limit number of plots
    columns_to_plot = numeric_columns[:max_columns]

    for column in columns_to_plot:
        try:
            plt.figure()
            if df[column].nunique() > 20:
                df[column].hist(bins=30, alpha=0.7, edgecolor='black')
                plt.title(f'Distribution of {column}')
                plt.ylabel('Frequency')
            else:
                df[column].value_counts().sort_index().plot(kind='bar', alpha=0.7)
                plt.title(f'Value Counts of {column}')
                plt.ylabel('Count')

            plt.xlabel(column)
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig(OUTPUTS_PLOTS / f"{column}_distribution.png", dpi=150, bbox_inches='tight')
            plt.close()

        except Exception as e:
            logger.warning(f"Could not plot {column}: {e}")


def create_correlation_heatmap(df):
    """Create correlation heatmap for numeric columns"""
    numeric_columns = df.select_dtypes(include=['number']).columns

    if len(numeric_columns) < 2:
        logger.info("Not enough numeric columns for correlation heatmap")
        return

    try:
        plt.figure(figsize=(10, 8))
        corr_matrix = df[numeric_columns].corr()

        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0,
                    fmt='.2f', square=True, linewidths=0.5)
        plt.title('Correlation Heatmap')
        plt.tight_layout()
        plt.savefig(OUTPUTS_PLOTS / "correlation_heatmap.png", dpi=150, bbox_inches='tight')
        plt.close()

    except Exception as e:
        logger.error(f"Failed to create correlation heatmap: {e}")


def create_categorical_plots(df, max_categories=10):
    """Create plots for categorical columns"""
    categorical_columns = df.select_dtypes(exclude=['number']).columns

    for column in categorical_columns:
        if df[column].nunique() <= max_categories:
            try:
                plt.figure()
                df[column].value_counts().plot(kind='bar', alpha=0.7)
                plt.title(f'Distribution of {column}')
                plt.ylabel('Count')
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.savefig(OUTPUTS_PLOTS / f"{column}_distribution.png", dpi=150, bbox_inches='tight')
                plt.close()
            except Exception as e:
                logger.warning(f"Could not plot categorical {column}: {e}")


def generate_all_plots(df):
    """Generate comprehensive set of plots"""
    logger.info("Generating visualizations...")

    OUTPUTS_PLOTS.mkdir(parents=True, exist_ok=True)

    create_distribution_plots(df)
    create_correlation_heatmap(df)
    create_categorical_plots(df)

    # Count generated plots
    plot_count = len(list(OUTPUTS_PLOTS.glob("*.png")))
    logger.info(f"Generated {plot_count} plots in {OUTPUTS_PLOTS}")

    return plot_count