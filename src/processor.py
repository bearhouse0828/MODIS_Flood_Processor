import logging
import numpy as np
import rasterio
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import os
from scipy.ndimage import zoom
import xarray as xr
from rasterio.enums import Resampling
from rasterio import warp
import gc

from config import Config


class MODISFloodProcessor:
    """Process downloaded MODIS flood tiles"""
    
    def __init__(self, config: Config):
        self.config = config
        self.setup_logging()
        
        # MODIS Flood data resolution and tile size (degrees)
        self.PIXEL_SIZE = config.GRID_RES  # degrees per pixel
        self.TILE_SIZE = 10.0      # degrees per tile
        
    def setup_logging(self):
        """Setup logger"""
        self.logger = self.config.setup_logging('flood_processor')
    
    def _parse_tile_filename(self, filename: str) -> Tuple[int, int]:
        """
        Parse tile information from filename
        Example: MCDWD_L3_F1C_NRT.A2024366.h20v03.061.tif -> (20, 3)
        """
        try:
            # Use regex to extract h and v values
            import re
            pattern = r'h(\d{2})v(\d{2})'
            match = re.search(pattern, filename)
            if match:
                h = int(match.group(1))
                v = int(match.group(2))
                return (h, v)
            else:
                self.logger.error(f"Unable to parse tile information from filename: {filename}")
                return None
        except Exception as e:
            self.logger.error(f"Error parsing filename {filename}: {e}")
            return None
    
    def get_tile_files(self, date: str) -> List[Path]:
        """
        Get all tile files for specified date
        
        Args:
            date: Date string in YYYYMMDD format or datetime object
        
        Returns:
            List of tile file paths
        """
        try:
            # Ensure date is string format
            if isinstance(date, datetime):
                date = date.strftime('%Y%m%d')
            elif not isinstance(date, str):
                raise ValueError(f"Unsupported date format: {type(date)}")
            
            # Extract year and day of year from date
            year = date[:4]
            doy = datetime.strptime(date, '%Y%m%d').strftime('%j')
            
            # Build data directory path
            data_dir = self.config.FLOOD_DIRS['raw'] / year / doy
            
            # Check if directory exists
            if not data_dir.exists():
                self.logger.error(f"Data directory does not exist: {data_dir}")
                return []
            
            # Get all .tif files
            tile_files = list(data_dir.glob("*.tif"))
            
            if not tile_files:
                self.logger.warning(f"No tile files found: {data_dir}")
                return []
            
            self.logger.info(f"Found {len(tile_files)} tile files")
            return tile_files
            
        except Exception as e:
            self.logger.error(f"Error getting tile files: {e}")
            return []

    def calculate_mosaic_bounds(self, tile_ids: List[str]) -> Tuple[int, int, int, int]:
        """
        Calculate mosaic boundary range
        
        Args:
            tile_ids: MODIS tile ID list (format like "h21v03")
            
        Returns:
            (min_h, max_h, min_v, max_v)
        """
        h_values = []
        v_values = []
        
        for tile_id in tile_ids:
            # Ensure tile_id format is standardized
            if len(tile_id) == 6:  # h22v8 format
                h = int(tile_id[1:3])
                v = int(tile_id[4:])
            else:  # h22v08 format
                h = int(tile_id[1:3])
                v = int(tile_id[4:6])
                
            h_values.append(h)
            v_values.append(v)
        
        return min(h_values), max(h_values), min(v_values), max(v_values)

    def create_mosaic_array(self, min_h: int, max_h: int, min_v: int, max_v: int, 
                           tile_size: int = 4800) -> np.ndarray:
        """
        Create empty array for mosaic
        
        Args:
            min_h, max_h: Minimum and maximum horizontal tile numbers
            min_v, max_v: Minimum and maximum vertical tile numbers
            tile_size: Pixel size of each tile
            
        Returns:
            Empty array for mosaic, initialized to 255 (insufficient data)
        """
        width = (max_h - min_h + 1) * tile_size
        height = (max_v - min_v + 1) * tile_size
        
        return np.full((height, width), 255, dtype=np.uint8)  # Initialize to 255

    def place_tile(self, mosaic: np.ndarray, tile_data: np.ndarray, tile_id: str, 
                   min_h: int, min_v: int, tile_size: int = 4800) -> None:
        """
        Place single tile in correct position in mosaic
        
        Args:
            mosaic: Mosaic array
            tile_data: Tile data
            tile_id: Tile ID
            min_h: Minimum horizontal tile number
            min_v: Minimum vertical tile number
            tile_size: Tile size
        """
        # Extract h and v from tile_id
        h = int(tile_id[1:3])
        v = int(tile_id[4:6]) if len(tile_id) > 6 else int(tile_id[4:])
        
        # Calculate offset
        col_offset = (h - min_h) * tile_size
        row_offset = (v - min_v) * tile_size
        
        # Place tile
        mosaic[row_offset:row_offset+tile_size, 
               col_offset:col_offset+tile_size] = tile_data

    def read_tile_data(self, tile_path: str) -> np.ndarray:
        """
        Read single tile data
        
        Args:
            tile_path: Tile file path
        
        Returns:
            Tile data array
        """
        try:
            with rasterio.open(tile_path) as src:
                data = src.read(1)  # Read first band
                return data
                
        except Exception as e:
            self.logger.error(f"Error reading tile data {tile_path}: {e}")
            raise

    def process_tiles(self, tile_paths: List[str]) -> Tuple[np.ndarray, Dict]:
        """
        Process and mosaic all tiles
        
        Args:
            tile_paths: List of tile file paths
            
        Returns:
            (Processed data array, boundary information dictionary)
        """
        try:
            # 1. First mosaic the data
            # Extract tile IDs from filenames
            tile_ids = []
            for path in tile_paths:
                filename = os.path.basename(path)
                parts = filename.split('.')
                for part in parts:
                    if part.startswith('h') and 'v' in part:
                        tile_ids.append(part)
                        break
            
            # Calculate mosaic boundaries
            min_h, max_h, min_v, max_v = self.calculate_mosaic_bounds(tile_ids)
            
            # Create mosaic array
            mosaic = self.create_mosaic_array(min_h, max_h, min_v, max_v)
            
            # Place each tile
            for tile_path in tile_paths:
                filename = os.path.basename(tile_path)
                tile_id = next(part for part in filename.split('.') if part.startswith('h') and 'v' in part)
                tile_data = self.read_tile_data(tile_path)
                self.place_tile(mosaic, tile_data, tile_id, min_h, min_v)
            
            # 2. Calculate actual longitude-latitude range of mosaicked data
            mosaic_bounds = {
                'left': min_h * 10 - 180,
                'right': (max_h + 1) * 10 - 180,
                'bottom': 90 - (max_v + 1) * 10,
                'top': 90 - min_v * 10
            }
            
            # 3. Resample to predefined output range
            output_bounds = {
                'left': self.config.GRID_WEST,
                'right': self.config.GRID_EAST,
                'bottom': self.config.GRID_SOUTH,
                'top': self.config.GRID_NORTH
            }
            
            # Use flood grid resolution
            grid_res = self.PIXEL_SIZE
            
            # Calculate output array size
            width = int((output_bounds['right'] - output_bounds['left']) / grid_res)
            height = int((output_bounds['top'] - output_bounds['bottom']) / grid_res)
            
            # Create source data affine transform
            src_transform = rasterio.transform.from_bounds(
                mosaic_bounds['left'], mosaic_bounds['bottom'],
                mosaic_bounds['right'], mosaic_bounds['top'],
                mosaic.shape[1], mosaic.shape[0]
            )
            
            # Create target data affine transform
            dst_transform = rasterio.transform.from_bounds(
                output_bounds['left'], output_bounds['bottom'],
                output_bounds['right'], output_bounds['top'],
                width, height
            )
            
            # Use rasterio for resampling
            resampled = np.full((height, width), 255, dtype=np.uint8)  # Initialize to 255
            warp.reproject(
                source=mosaic,
                destination=resampled,
                src_transform=src_transform,
                src_crs='EPSG:4326',
                dst_transform=dst_transform,
                dst_crs='EPSG:4326',
                resampling=Resampling.mode,  # Use mode as aggregation method
            )
            
            return resampled, output_bounds
            
        except Exception as e:
            self.logger.error(f"Error processing tile files: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise

    def process_flood_data(self, date):
        """
        Process flood data for specified date
        
        Args:
            date: Single date, can be datetime object or YYYYMMDD format string
            
        Returns:
            (Processed data array, boundary information dictionary) or None
        """
        try:
            self.logger.info(f"Starting processing date: {date}, type: {type(date)}")
            
            # If datetime object, convert to string
            if isinstance(date, datetime):
                date_str = date.strftime('%Y%m%d')
            elif isinstance(date, str):
                date_str = date
            else:
                raise ValueError(f"Unsupported date format: {type(date)}")
            
            self.logger.info(f"Processing date string: {date_str}")
            
            # Get all tile files for this date
            tile_files = self.get_tile_files(date_str)
            if not tile_files:
                self.logger.warning(f"No tile files found for date {date_str}")
                return None
            
            # Process tile files
            self.logger.info(f"Starting to process {len(tile_files)} tile files")
            result = self.process_tiles([str(f) for f in tile_files])
            return result
            
        except Exception as e:
            self.logger.error(f"Error processing flood data: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise


def main():
    """Main function for running processing task independently"""
    from datetime import datetime, timedelta
    
    config = Config()
    processor = MODISFloodProcessor(config)
    
    # Process yesterday's data as example
    yesterday = datetime.now() - timedelta(days=1)
    
    print(f"Starting to process MODIS flood data for {yesterday.strftime('%Y-%m-%d')}...")
    result = processor.process_flood_data(yesterday)
    
    if result:
        data, bounds = result
        print(f"Processing completed: data shape {data.shape}, bounds {bounds}")
    else:
        print("Processing failed: no data found")


if __name__ == "__main__":
    main()