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


# ========== S3 Upload Task ==========
def upload_visualizations_to_s3(**context):
    """
    Upload PNG figures generated by visualization step to S3 bucket 'floodmap'.
    Path convention: s3://floodmap/{region}/{date}/{name}.png
    - region: parsed from filename 'flood_{region}_{date}.png'; use 'full' for full map
    - date:   YYYYMMDD (taken from download task XCom)
    - name:   original filename
    """
    import os
    from pathlib import Path
    import re
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    ti = context["ti"]

    # Output directory from visualization
    vis_xcom = ti.xcom_pull(task_ids="generate_visualizations")
    output_dir = vis_xcom.get("visualization_output") if isinstance(vis_xcom, dict) else vis_xcom

    if not output_dir:
        raise ValueError("Visualization output directory not found in XCom")

    # Date from download task
    dl_xcom = ti.xcom_pull(task_ids="download_data")
    date_str = None
    if isinstance(dl_xcom, dict):
        # Prefer YYYY-MM-DD then convert
        iso_date = dl_xcom.get("download_date")
        if iso_date:
            # Convert YYYY-MM-DD -> YYYYMMDD
            date_str = iso_date.replace("-", "")

    if not date_str:
        # Fallback: try to infer from filenames
        date_str = ""

    output_path = Path(output_dir)
    if not output_path.exists():
        raise ValueError(f"Visualization output directory does not exist: {output_path}")

    # Collect PNG files, optionally filter by date suffix if available
    png_files = list(output_path.glob("*.png"))
    if date_str:
        png_files = [p for p in png_files if p.stem.endswith(date_str)]

    if len(png_files) == 0:
        print(f"DEBUG: No PNG files found to upload in {output_path}")
        return {"uploaded": 0}

    s3 = boto3.client("s3")
    bucket = "floodmap"

    uploaded = 0
    # Filename patterns: flood_{region}_{YYYYMMDD}.png or flood_full_{YYYYMMDD}.png
    pattern = re.compile(r"^flood_(?P<region>.+?)_(?P<date>\d{8})$")

    for png in png_files:
        name = png.name
        stem = png.stem
        region = "unknown"
        date_comp = date_str if date_str else None

        m = pattern.match(stem)
        if m:
            region = m.group("region")
            date_comp = m.group("date")
        elif stem.startswith("flood_full_"):
            region = "full"
            tail = stem[len("flood_full_"):]
            if len(tail) == 8 and tail.isdigit():
                date_comp = tail

        if not date_comp:
            print(f"DEBUG: Skip file without parsable date: {name}")
            continue

        # Convert YYYYMMDD to YYYY-MM-DD for S3 key
        if len(date_comp) == 8 and date_comp.isdigit():
            iso_date = f"{date_comp[0:4]}-{date_comp[4:6]}-{date_comp[6:8]}"
        else:
            iso_date = date_comp

        key = f"{region}/{iso_date}/{name}"
        try:
            s3.upload_file(str(png), bucket, key, ExtraArgs={"ContentType": "image/png"})
            uploaded += 1
            print(f"DEBUG: Uploaded to s3://{bucket}/{key}")
        except (BotoCoreError, ClientError) as e:
            print(f"DEBUG: Failed to upload {name} -> s3://{bucket}/{key}: {e}")

    return {"uploaded": uploaded}


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

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_visualizations_to_s3,
    )

    def update_availability_manifest(**context):
        """
        Scan s3://floodmap and build availability.json structure:
        {
          "regions": {
            "<region>": ["YYYYMMDD", ...]
          }
        }
        The manifest is uploaded to s3://floodmap/availability.json
        """
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
        import json
        from collections import defaultdict

        s3 = boto3.client("s3")
        bucket = "floodmap"

        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket)

        regions_to_dates = defaultdict(set)

        # Expect keys like region/date/name.png
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # skip manifest itself
                if key == "availability.json":
                    continue
                parts = key.split("/")
                if len(parts) >= 3 and parts[2].endswith(".png"):
                    region, date_part = parts[0], parts[1]
                    # Handle both YYYYMMDD and YYYY-MM-DD formats
                    if len(date_part) == 8 and date_part.isdigit():
                        # Old format: YYYYMMDD -> YYYY-MM-DD
                        iso_date = f"{date_part[0:4]}-{date_part[4:6]}-{date_part[6:8]}"
                        regions_to_dates[region].add(iso_date)
                    elif len(date_part) == 10 and date_part.count("-") == 2:
                        # New format: YYYY-MM-DD (already correct)
                        regions_to_dates[region].add(date_part)

        manifest = {
            "regions": {region: sorted(list(dates)) for region, dates in regions_to_dates.items()}
        }

        body = json.dumps(manifest, indent=2).encode("utf-8")
        try:
            s3.put_object(
                Bucket=bucket, 
                Key="availability.json", 
                Body=body, 
                ContentType="application/json",
                CacheControl="no-cache, no-store"
            )
            print("DEBUG: Uploaded availability.json with no-cache headers")
        except (BotoCoreError, ClientError) as e:
            print(f"DEBUG: Failed to upload availability.json: {e}")

        return {"regions": list(manifest.get("regions", {}).keys())}

    manifest_task = PythonOperator(
        task_id="update_availability_manifest",
        python_callable=update_availability_manifest,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_temp_files",
        python_callable=cleanup_temp_files,
    )

    end_task = EmptyOperator(task_id="end_processing")

    # Define task dependencies
    start_task >> download_task >> merge_task >> visualization_task >> upload_task >> manifest_task >> cleanup_task >> end_task
