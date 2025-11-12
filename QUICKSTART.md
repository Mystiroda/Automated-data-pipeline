# Quick Start Guide

Quick guide to set up and run the pipeline

## Prerequisites

- Python 3.8 or higher
- pip (Python package manager)
- Internet connection

## Step-by-Step Setup

### 1. Clone and Setup
```bash
# Clone the repository
git clone https://github.com/Mystiroda/automated-data-pipeline.git
cd automated-data-pipeline

# Install dependencies
pip install -r requirements.txt
```
### 2. Run the Pipeline 
```bash
# Run with default Titanic dataset
python main.py

# Or run with Iris dataset
python main.py --dataset iris

# Or use your own data
python main.py --url "https://your-data.com/data.csv"
```
### 3. Check Results
```bash
# Verify everything worked
python verify_database.py

# View generated files
ls outputs/plots/
ls outputs/reports/
```