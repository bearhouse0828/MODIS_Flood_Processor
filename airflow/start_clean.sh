#!/bin/bash

echo "=== Starting Clean Airflow Environment ==="

# Set environment variables
export AIRFLOW_HOME=$(pwd)
export AIRFLOW__CORE__DAGS_FOLDER=$AIRFLOW_HOME/dags
export AIRFLOW__CORE__PLUGINS_FOLDER=$AIRFLOW_HOME/plugins
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False

echo "AIRFLOW_HOME: $AIRFLOW_HOME"
echo "Disable example DAGs: $AIRFLOW__CORE__LOAD_EXAMPLES"

# Stop existing processes
echo "Stopping existing Airflow processes..."
pkill -f airflow 2>/dev/null || true
sleep 2

# Start Airflow standalone
echo "Starting Airflow standalone..."
airflow standalone 2>&1 | tee /tmp/airflow_clean.log &

# Wait for startup
echo "Waiting for Airflow to start..."
sleep 20

# Check DAGs
echo "Checking flood DAGs..."
airflow dags list | grep -E "(flood|modis)" || echo "No flood DAGs found"

echo ""
echo "=== Startup Complete ==="
echo "Access: http://localhost:8080"
echo "Username: admin"
echo "Password: $(cat simple_auth_manager_passwords.json.generated 2>/dev/null | grep -o '\"[^\"]*\"' | tail -1 | tr -d '\"' || echo 'Please check startup logs')"

