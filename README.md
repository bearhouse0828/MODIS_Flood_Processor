# MODIS Flood Data Processor

This is a specialized system for processing MODIS flood data, separated from the original VIIRS project.

## Features

- **Data Download**: Download MODIS MCDWD_L3_F2_NRT flood data from NASA servers
- **Data Processing**: Mosaic and process multiple tile files
- **Data Export**: Support NetCDF and GeoTIFF format export
- **Data Visualization**: Generate flood monitoring maps for multiple regions
- **Batch Processing**: Support single date and date range processing

## Project Structure

```
flood_processor/
├── src/
│   ├── config.py          # Configuration file
│   ├── downloader.py      # Data downloader
│   ├── processor.py       # Data processor
│   ├── exporter.py        # Data exporter
│   ├── plotter.py         # Data plotter
│   └── main.py           # Main program entry point
├── data/
│   ├── raw/              # Raw data directory
│   └── output/           # Output data directory
├── logs/                 # Log files directory
├── requirements.txt      # Dependencies list
└── README.md            # Project documentation
```

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### 1. Process Latest Data (Yesterday)

```bash
python src/main.py --latest
```

### 2. Process Specified Date

```bash
python src/main.py --date 20250101
```

### 3. Process Date Range

```bash
python src/main.py --start-date 20250101 --end-date 20250107
```

### 4. Check Data Availability

```bash
python src/main.py --check 20250101
```

### 5. List Available Data Dates

```bash
python src/main.py --list-available --days-back 7
```

## Output Files

After processing, the system generates the following files in the `data/output/` directory:

- **NetCDF Files**: `MODIS_Flood_YYYYMMDD.nc` - Standardized flood data
- **Image Files**: In `plots/` subdirectory
  - `flood_full_YYYYMMDD.png` - Full region map
  - `flood_mekong_YYYYMMDD.png` - Mekong River region map
  - `flood_[RegionName]_YYYYMMDD.png` - Sub-region maps

## Configuration

Main configuration parameters in `src/config.py`:

- **AOI Range**: Asia-Oceania region (35°E-155°E, 40°S-56°N)
- **Grid Resolution**: Approximately 0.002083 degrees (10/4800)
- **Data Source**: NASA MODIS MCDWD_L3_F2_NRT
- **Output Format**: NetCDF (WGS84 projection)

## Data Description

MODIS flood data contains the following values:
- 0: No water
- 1: Surface water (normal water bodies)
- 2: Recurring flood (not populated in beta release)
- 3: Unusual flood
- 255: Insufficient data

## Logging

All processing steps are logged in corresponding log files in the `logs/` directory.

## Notes

1. Ensure sufficient disk space for storing raw data and output files
2. Stable network connection required to access NASA data servers
3. For processing large amounts of data, consider using high-performance computing environments
4. Regularly clean old data to save storage space

## Testing

Run the test script to verify system functionality:

```bash
python test_flood_processor.py
```

## Technical Support

For issues, please check log files or contact the development team.