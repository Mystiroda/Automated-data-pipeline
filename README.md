# Automated Data Pipeline

A comprehensive data processing pipeline that automatically downloads, cleans, analyzes, and visualizes datasets with zero configuration required.

![Python](https://img.shields.io/badge/python-3.8%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-production--ready-brightgreen)

##  Features

- **Automatic Data Download** - From URLs or predefined datasets
- **Smart Data Cleaning** - Handles missing values, duplicates, and data quality
- **SQL Database Storage** - Automatic SQLite integration with query capabilities
- **Comprehensive Analysis** - Statistical summaries, correlations, and insights
- **Automated Visualization** - Generates plots and charts automatically
- **Detailed Reporting** - Execution logs and analysis reports
- **Zero Configuration** - Works out of the box
- **Modular Design** - Easy to extend and customize

## Quick Start

### Prerequisites
- Python 3.8 or higher
- pip (Python package manager)

### Installation & Run

```bash
# 1. Clone and setup
git clone https://github.com/Mystiroda/automated-data-pipeline.git
cd automated-data-pipeline
pip install -r requirements.txt

# 2. Run the pipeline (uses Titanic dataset by default)
python main.py

# 3. Verify results
python verify_database.py
```

### Try Different Datasets
```bash
# Use Iris dataset
python main.py --dataset iris

# Use your own data
python main.py --url "https://example.com/your-data.csv"
```

### Project Structure
```bash
automated-data-pipeline/
├── main.py                   # Main pipeline file
├── config.py                 # Configuration and paths
├── requirements.txt          # Python dependencies
├── verify_database.py        # Database verification
├── README.md                 # This file
├── QUICKSTART.md             # Quick start guide
├── PROJECT_SUMMARY.md        # Detailed project overview
├── PIPELINE_FLOW.txt         # Execution flow
├── Data/
│   ├── processed             #data after cleaning
│   └── raw                   #raw data
├── Outputs
│   ├── plots                 #Generated plots
│   └── reports               #Generated reports
├── Src/
│   ├── __init__.py           # Initialization file
│   ├── data_downloader.py    # Data downloading utilities
│   ├── data_cleaner.py       # Data cleaning functions
│   ├── database_handler.py   # Database operations
│   ├── data_analyzer.py      # Analysis functions
│   └── plot_generator.py     # Visualization functions
├──tests
    └── 📄 test_pipeline.py   # Test suite
```

### Supported Datasets

##### Built-in Datasets
- Titanic - Passenger survival data (--dataset titanic)
- Iris - Flower measurement data (--dataset iris)

##### Custom Datasets
- Any CSV file via URL (--url "https://...")

### Customization

Modify Cleaning Strategies

Edit `config.py`:
```python
DEFAULT_CLEANING = {
    'remove_duplicates': True,
    'strip_columns': True,
    'handle_numeric_na': 'median',  # Options: 'mean', 'median', 'zero'
    'handle_categorical_na': 'mode' # Options: 'mode', 'unknown'
}
```

### Add New Datasets
Edit `config.py`:
```python
DEFAULT_DATASETS = {
    'titanic': 'https://...titanic.csv',
    'iris': 'https://...iris.csv',
    'your_dataset': 'https://...your-data.csv'  # Add your own
}
```
### Expected Outputs
| Output Type | Location           | Description                            |
| ----------- | ------------------ |----------------------------------------|
| Raw Data    | `data/raw/`        | Original downloaded dataset            |
| Cleaned Data | `data/processed/`  | Cleaned CSV after preprocessing        |
| Visualizations | `outputs/plots/`   | Automatically generated plots          |
| Reports     | `outputs/reports/` | Statistical summaries and correlations |
| Database   | `database.sqlite`  | SQLite storage of final dataset        |
| Logs      | `pipeline.log`     | Full execution logs                    |

### Future Enhancements
- Configurable YAML-based pipelines
- Auto notebook report generation
- Machine Learning model integration
- Web dashboard (Flask + Plotly)

### License
This project is licensed under the MIT License - see the LICENSE file for details.

### Acknowledgments

Built with [Pandas](https://pandas.pydata.org/), [Matplotlib](https://matplotlib.org/), [Seaborn](https://seaborn.pydata.org/), and [SQLite](https://www.sqlite.org/). Test datasets from various public sources. Inspired by real-world data engineering pipelines

