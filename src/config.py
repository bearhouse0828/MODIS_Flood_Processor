# config.py
import os
import sys
from pathlib import Path

class Config:
    """Configuration class: Contains all flood project configuration parameters"""
    
    # Project root directory
    ROOT_DIR = Path(__file__).parent.parent
    
    # Base data directory
    DATA_DIR = ROOT_DIR / 'data'
    TEMP_DIR = DATA_DIR / 'temp'  # Temporary file directory
    
    # Flood data directory structure
    FLOOD_DIRS = {
        'raw': DATA_DIR / 'flood' / 'raw',
        'output': DATA_DIR / 'flood' / 'output'
    }
    
    # NASA MODIS data server configuration
    MODIS_PRODUCT_ID = "MCDWD_L3_F2_NRT"
    MODIS_BASE_URL = f"https://nrt3.modaps.eosdis.nasa.gov/archive/allData/61/{MODIS_PRODUCT_ID}"
    MODIS_BACKUP_URL = f"https://nrt4.modaps.eosdis.nasa.gov/archive/allData/61/{MODIS_PRODUCT_ID}"
    
    # AOI range configuration (U.S.)
    AOI = {
        "lon_min": -125.0,  # Westernmost point (Pacific coast)
        "lon_max": -66.9,   # Easternmost point (Atlantic coast)
        "lat_min": 24.5,    # Southernmost point (Florida Keys)
        "lat_max": 49.5     # Northernmost point (Northern border)
    }

    # Target grid parameters
    GRID_NORTH = AOI["lat_max"]  # Northern boundary
    GRID_SOUTH = AOI["lat_min"]  # Southern boundary
    GRID_EAST = AOI["lon_max"]   # Eastern boundary
    GRID_WEST = AOI["lon_min"]   # Western boundary
    
    # Grid resolution for flood data
    GRID_RES = 10/4800  # Resolution of flood data (approximately 0.002083 degrees)
    
    # Projection parameters
    PROJECTION = "EPSG:4326"  # WGS84
    MAX_MAPPING_DISTANCE = 0.1  # Maximum mapping distance (degrees)
    
    # Data retention policy (days)
    RAW_DATA_RETENTION_DAYS = 7    # Raw data retention time
    OUTPUT_RETENTION_DAYS = 30     # Output data retention time
    
    # Processing parameters
    MERGE_METHOD = 'max'  # Data merging method: 'max', 'mean', 'weighted_mean'
    
    # Logging configuration
    LOG_LEVEL = "INFO"
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    LOG_DIR = ROOT_DIR / "logs"
    
    # Log files for each module
    LOG_FILES = {
        'downloader': LOG_DIR / "downloader.log",
        'processor': LOG_DIR / "processor.log",
        'exporter': LOG_DIR / "exporter.log",
        'plotter': LOG_DIR / "plotter.log",
        'main': LOG_DIR / "main.log"
    }
    
    # Valid MODIS tiles for U.S. (h05-h12, v03-v07)
    VALID_TILES = [(h, v) for h in range(5, 10) for v in range(3, 5)]
    
    # Default scheduling parameter: how many days ago to process
    DAYS_AGO_DEFAULT = 3

    @classmethod
    def get_target_date(cls, days_ago=None):
        """Return target date as now() - days_ago.

        If days_ago is None, read from env FLOOD_DAYS_AGO; fallback to DAYS_AGO_DEFAULT.
        """
        from datetime import datetime, timedelta
        try:
            effective_days = (
                int(days_ago)
                if days_ago is not None
                else int(os.getenv("FLOOD_DAYS_AGO", cls.DAYS_AGO_DEFAULT))
            )
        except Exception:
            effective_days = cls.DAYS_AGO_DEFAULT
        return datetime.now() - timedelta(days=effective_days)
    
    @classmethod
    def setup_logging(cls, module_name: str = 'main'):
        """Setup logger"""
        import logging
        
        # Ensure log directory exists
        cls.LOG_DIR.mkdir(parents=True, exist_ok=True)
        
        # Create logger
        logger = logging.getLogger(module_name)
        logger.setLevel(getattr(logging, cls.LOG_LEVEL))
        
        # Clear existing handlers
        logger.handlers.clear()
        
        # Create formatter
        formatter = logging.Formatter(cls.LOG_FORMAT)
        
        # Add file handler
        if module_name in cls.LOG_FILES:
            fh = logging.FileHandler(cls.LOG_FILES[module_name])
            fh.setFormatter(formatter)
            logger.addHandler(fh)
        
        # Add console handler (output to stdout instead of stderr)
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        
        return logger
    
    @classmethod
    def get_flood_output_path(cls, date, region='full'):
        """Get flood data output path"""
        from datetime import datetime
        
        if isinstance(date, str):
            date = datetime.strptime(date, '%Y%m%d')
        
        year = date.year
        doy = date.timetuple().tm_yday
        
        # Create output directory
        output_dir = cls.FLOOD_DIRS['output'] / f"{year}/{doy:03d}"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename
        if region == 'full':
            filename = f"MODIS_Flood_{date.strftime('%Y%m%d')}.nc"
        else:
            filename = f"MODIS_Flood_{region}_{date.strftime('%Y%m%d')}.nc"
        
        return output_dir / filename
