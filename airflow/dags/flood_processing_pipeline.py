import sys
import os
from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Add src directory to Python path
if '__file__' in globals():
    # When running as a script
    src_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '..', 'src')
else:
    # When running in interactive mode or Airflow
    src_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '..', 'src')
sys.path.insert(0, src_path)


# ========== Define Task Functions ==========
def download_flood_data(**context):
    from downloader import MODISFloodDownloader
    from config import Config
    from datetime import datetime, timedelta
    
    # Calculate date two months ago
    three_days_ago = datetime.now() - timedelta(days=3)
    
    # Initialize downloader with config
    config = Config()
    downloader = MODISFloodDownloader(config)
    
    # Download data for three days ago
    files = downloader.download_tiles(three_days_ago)
    
    # Debug: Check download results
    print(f"DEBUG: Download completed for {three_days_ago.strftime('%Y-%m-%d')}")
    print(f"DEBUG: Downloaded files count: {len(files) if files else 'None'}")
    print(f"DEBUG: Files type: {type(files)}")
    
    if files is None:
        raise ValueError("Download task failed - no files downloaded")
    
    # Store files in a temporary file to avoid XCom size limits
    import tempfile
    import json
    from pathlib import Path
    
    # Create temp file for file list
    if '__file__' in globals():
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    else:
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    temp_dir = Path(project_root) / "data" / "temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    files_list_path = temp_dir / f"files_{three_days_ago.strftime('%Y%m%d')}.json"
    
    with open(files_list_path, 'w') as f:
        json.dump(files, f)
    
    print(f"DEBUG: Files list saved to: {files_list_path}")
    
    return {
        "files_list_path": str(files_list_path), 
        "file_count": len(files),
        "download_date": three_days_ago.strftime("%Y-%m-%d")
    }


def merge_flood_data(**context):
    from processor import MODISFloodProcessor
    from exporter import FloodExporter
    from config import Config
    from datetime import datetime, timedelta
    ti = context["ti"]
    
    # Get file list path from XCom
    xcom_data = ti.xcom_pull(task_ids="download_data")
    files_list_path = xcom_data.get("files_list_path")
    file_count = xcom_data.get("file_count", 0)
    
    print(f"DEBUG: Files list path: {files_list_path}")
    print(f"DEBUG: Expected file count: {file_count}")
    
    if not files_list_path:
        raise ValueError("No files list path received from download task")
    
    # Read files list from temporary file
    import json
    from pathlib import Path
    
    files_list_file = Path(files_list_path)
    if not files_list_file.exists():
        raise ValueError(f"Files list file not found: {files_list_path}")
    
    with open(files_list_file, 'r') as f:
        files = json.load(f)
    
    print(f"DEBUG: Loaded {len(files)} files from {files_list_path}")
    print(f"DEBUG: Files type: {type(files)}")
    
    if files is None:
        raise ValueError("No files received from download task")
    
    if not isinstance(files, list):
        raise ValueError(f"Expected list of files, got {type(files)}")
    
    if len(files) == 0:
        raise ValueError("No files to process - download task returned empty list")
    
    # Initialize processor and exporter with config
    config = Config()
    processor = MODISFloodProcessor(config)
    exporter = FloodExporter(config)
    
    # Process tiles (merge and resample)
    result = processor.process_tiles(files)
    if result is None:
        raise ValueError("Data processing failed")
    
    output_data, output_bounds = result
    
    # Export to NetCDF - use same date as download task
    three_days_ago = datetime.now() - timedelta(days=3)
    output_path = config.get_flood_output_path(three_days_ago)
    
    print(f"DEBUG: Exporting to NetCDF file: {output_path}")
    
    success = exporter.export_flood_netcdf(
        output_data,
        output_bounds,
        three_days_ago,
        output_path,
        config.GRID_RES
    )
    
    if not success:
        raise ValueError("Data export failed")
    
    print(f"DEBUG: NetCDF export successful: {output_path}")
    print(f"DEBUG: File exists: {Path(output_path).exists()}")
    
    return {"merged_file": str(output_path)}


def generate_visualizations(**context):
    # Set matplotlib backend for Airflow environment
    import matplotlib
    matplotlib.use('Agg')
    
    from plotter import FloodPlotter
    from config import Config
    ti = context["ti"]
    
    # Debug: Check XCom data from merge task
    xcom_data = ti.xcom_pull(task_ids="merge_data")
    print(f"DEBUG: Merge task XCom data: {xcom_data}")
    
    # Get merged file path from XCom data
    if isinstance(xcom_data, dict):
        merged_file = xcom_data.get("merged_file")
    else:
        # Fallback: try with key parameter
        merged_file = ti.xcom_pull(task_ids="merge_data", key="merged_file")
    
    print(f"DEBUG: Merged file path: {merged_file}")
    print(f"DEBUG: Merged file type: {type(merged_file)}")
    
    if merged_file is None:
        raise ValueError("No merged file received from merge task")
    
    # Check if file exists
    from pathlib import Path
    merged_file_path = Path(merged_file)
    if not merged_file_path.exists():
        raise ValueError(f"Merged file does not exist: {merged_file}")
    
    print(f"DEBUG: Merged file exists: {merged_file_path.exists()}")
    print(f"DEBUG: File size: {merged_file_path.stat().st_size if merged_file_path.exists() else 'N/A'} bytes")
    
    # Initialize plotter with config
    config = Config()
    plotter = FloodPlotter(config)
    
    # Generate all flood visualizations
    output_dir = config.FLOOD_DIRS['output'] / 'plots'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"DEBUG: Starting visualization for file: {merged_file}")
    print(f"DEBUG: Output directory: {output_dir}")
    
    plotter.plot_all_regions(merged_file, output_dir)
    
    return {"visualization_output": str(output_dir)}


def cleanup_temp_files(**context):
    from config import Config
    from pathlib import Path
    import os
    import glob
    
    config = Config()
    
    # Clean up temporary files
    if '__file__' in globals():
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    else:
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    temp_dir = Path(project_root) / "data" / "temp"
    if temp_dir.exists():
        # Remove temporary JSON files
        json_files = list(temp_dir.glob("*.json"))
        for json_file in json_files:
            try:
                json_file.unlink()
                print(f"DEBUG: Removed temporary file: {json_file}")
            except Exception as e:
                print(f"DEBUG: Error removing {json_file}: {e}")
    
    # Clean up old raw data files (optional - keep recent ones)
    raw_dir = config.FLOOD_DIRS['raw']
    if raw_dir.exists():
        # Remove files older than 7 days
        import time
        current_time = time.time()
        seven_days_ago = current_time - (7 * 24 * 60 * 60)
        
        for file_path in raw_dir.rglob("*.tif"):
            if file_path.stat().st_mtime < seven_days_ago:
                try:
                    file_path.unlink()
                    print(f"DEBUG: Removed old file: {file_path}")
                except Exception as e:
                    print(f"DEBUG: Error removing {file_path}: {e}")
    
    print("DEBUG: Cleanup completed")
    return {"status": "cleanup_done"}


# ========== Define DAG ==========
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="modis_flood_processing_pipeline",
    description="Complete MODIS Flood Data Processing Pipeline",
    schedule="0 6 * * *",  # Every day at 6 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["flood", "modis", "processing", "pipeline"],
) as dag:

    start_task = EmptyOperator(task_id="start_processing")

    download_task = PythonOperator(
        task_id="download_data",
        python_callable=download_flood_data,
    )

    merge_task = PythonOperator(
        task_id="merge_data",
        python_callable=merge_flood_data,
    )

    visualization_task = PythonOperator(
        task_id="generate_visualizations",
        python_callable=generate_visualizations,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_temp_files",
        python_callable=cleanup_temp_files,
    )

    end_task = EmptyOperator(task_id="end_processing")

    # Define task dependencies
    start_task >> download_task >> merge_task >> visualization_task >> cleanup_task >> end_task
