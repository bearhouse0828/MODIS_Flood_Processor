# MODIS Flood Data Processing - Airflow Pipeline

This directory contains the Airflow implementation of the MODIS flood data processing pipeline, split into three main modules:

## Pipeline Architecture

```
Download → Merge → Visualization
    ↓         ↓         ↓
  Raw Data  Processed  Maps
           NetCDF
```

### 1. Download Module (`flood_download_task.py`)
- **Purpose**: Download MODIS MCDWD_L3_F2_NRT flood data from NASA servers
- **Input**: Date parameter
- **Output**: Raw tile files (.tif format)
- **Schedule**: Daily at 6:00 AM

### 2. Merge Module (`flood_merge_task.py`)
- **Purpose**: Process and merge downloaded tiles into a single NetCDF file
- **Input**: Raw tile files from download module
- **Output**: Processed NetCDF file
- **Process**: Tile mosaicking, resampling, data export

### 3. Visualization Module (`flood_visualization_task.py`)
- **Purpose**: Generate flood monitoring maps and visualizations
- **Input**: Processed NetCDF file from merge module
- **Output**: PNG image files for different regions
- **Regions**: Full region, Mekong River, and multiple sub-regions

## Main Pipeline (`flood_processing_pipeline.py`)

The main DAG orchestrates the complete workflow:
1. **start_processing** - Dummy start task
2. **download_data** - Download MODIS tiles
3. **merge_data** - Process and export data
4. **generate_visualizations** - Create maps
5. **cleanup_temp_files** - Clean up temporary files
6. **end_processing** - Dummy end task

## Setup and Usage

### 1. Initialize Airflow
```bash
cd airflow
./start_airflow_v3.sh
```

### 2. Start Airflow Services

**Option A: Standalone Mode (Recommended for testing)**
```bash
airflow standalone
```

**Option B: Separate Components**
```bash
# Start API server (in one terminal)
airflow api-server --port 8080

# Start scheduler (in another terminal)
airflow scheduler
```

### 3. Access Airflow UI
Open your browser and go to: http://localhost:8080
- Username: admin
- Password: admin

### 4. Run the Pipeline
1. Go to the DAGs page in the Airflow UI
2. Find "modis_flood_processing_pipeline"
3. Toggle it ON to enable scheduling
4. Click "Trigger DAG" to run manually

## Individual Module DAGs

You can also run individual modules separately:

- **flood_download**: Download only
- **flood_merge**: Merge only (requires upstream data)
- **flood_visualization**: Visualization only (requires processed data)

## Configuration

The pipeline uses the existing configuration from `src/config.py`:
- **Coverage Area**: Asia-Oceania region (35°E-155°E, 40°S-56°N)
- **Grid Resolution**: ~0.002083 degrees
- **Data Source**: NASA MODIS MCDWD_L3_F2_NRT
- **Output Format**: NetCDF (WGS84 projection)

## Dependencies

All required Python packages are installed via the main `requirements.txt` file in the project root.

## File Structure

```
airflow/
├── dags/                          # DAG files
│   ├── flood_processing_pipeline.py  # Main pipeline
│   ├── flood_download_task.py        # Download module
│   ├── flood_merge_task.py           # Merge module
│   └── flood_visualization_task.py   # Visualization module
├── plugins/                       # Airflow plugins (empty)
├── logs/                         # Airflow logs
├── config/                       # Airflow configuration
├── setup_airflow.py              # Environment setup
├── start_airflow.sh              # Initialization script
└── README.md                     # This file
```

## Monitoring and Logs

- **Airflow UI**: Monitor task status and logs
- **Log Files**: Located in `airflow/logs/`
- **Task Logs**: Available in the Airflow UI for each task

## Troubleshooting

1. **Import Errors**: Ensure the `src/` directory is in the Python path
2. **Data Not Found**: Check if MODIS data is available for the specified date
3. **Permission Issues**: Ensure proper file permissions for data directories
4. **Memory Issues**: Large datasets may require increased memory allocation

## Next Steps

1. Test the pipeline with a recent date
2. Monitor performance and optimize if needed
3. Add error handling and retry logic
4. Implement data quality checks
5. Add notification systems for failures
