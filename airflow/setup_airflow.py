#!/usr/bin/env python3
"""
Simple Airflow setup script for MODIS Flood Data Processing
"""
import os
import sys
from pathlib import Path

# Set Airflow environment variables
AIRFLOW_HOME = Path(__file__).parent
os.environ['AIRFLOW_HOME'] = str(AIRFLOW_HOME)
os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = str(AIRFLOW_HOME / 'dags')
os.environ['AIRFLOW__CORE__PLUGINS_FOLDER'] = str(AIRFLOW_HOME / 'plugins')
os.environ['AIRFLOW__CORE__LOGGING_LEVEL'] = 'INFO'
os.environ['AIRFLOW__CORE__EXECUTOR'] = 'LocalExecutor'
os.environ['AIRFLOW__CORE__LOAD_EXAMPLES'] = 'False'

# Add src directory to Python path
src_path = AIRFLOW_HOME.parent / 'src'
sys.path.insert(0, str(src_path))

print(f"Airflow home set to: {AIRFLOW_HOME}")
print(f"DAGs folder: {AIRFLOW_HOME / 'dags'}")
print(f"Plugins folder: {AIRFLOW_HOME / 'plugins'}")
print(f"Source path added: {src_path}")

