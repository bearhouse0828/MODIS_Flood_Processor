import numpy as np
import logging
from pathlib import Path
import xarray as xr
from datetime import datetime
from typing import Dict, Optional, Union, Tuple
import os
import netCDF4 as nc
import gc

from config import Config


class FloodExporter:
    """Flood data exporter: supports NetCDF format"""
    
    def __init__(self, config: Config):
        """
        Initialize exporter
        
        Args:
            config: Configuration object containing output parameters
        """
        self.config = config
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logger"""
        self.logger = self.config.setup_logging('flood_exporter')
    
    def export_flood_netcdf(self, data: np.ndarray, bounds: Dict, date: Union[datetime, str], 
                           output_path: Union[str, Path], pixel_size: float) -> bool:
        """
        Export MODIS Flood data as NetCDF format
        
        Args:
            data: Flood data array
            bounds: Boundary information dictionary {'left', 'right', 'bottom', 'top'}
            date: Date object or string
            output_path: Output file path
            pixel_size: Pixel size (degrees)
            
        Returns:
            Whether export was successful
        """
        try:
            # Ensure output directory exists
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Handle date
            if isinstance(date, str):
                date_obj = datetime.strptime(date, '%Y%m%d')
            else:
                date_obj = date
            
            # Calculate target size
            lat_size = int((bounds['top'] - bounds['bottom']) / pixel_size)
            lon_size = int((bounds['right'] - bounds['left']) / pixel_size)
            
            # Create coordinate arrays
            lats = np.linspace(bounds['top'], bounds['bottom'], lat_size)
            lons = np.linspace(bounds['left'], bounds['right'], lon_size)
            
            # Create dataset
            ds = xr.Dataset(
                {
                    "flood": (["latitude", "longitude"], data),
                },
                coords={
                    "latitude": ("latitude", lats),
                    "longitude": ("longitude", lons)
                },
                attrs={
                    "description": "MODIS Flood Data",
                    "created": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "resolution": f"{pixel_size:.6f} degrees",
                    "projection": "Geographic (WGS84)",
                    "source": "NASA MODIS MCDWD_L3_F2_NRT",
                    "processing_date": date_obj.strftime("%Y-%m-%d")
                }
            )
            
            # Add flood variable attributes
            ds.flood.attrs.update({
                "long_name": "MODIS Flood Water",
                "units": "1",
                "_FillValue": 255,
                "valid_range": [0, 4],
                "flag_values": [0, 1, 2, 3, 255],
                "flag_meanings": "no_water surface_water recurring_flood flood_unusual insufficient_data",
                "flag_descriptions": (
                    "0=No water; "
                    "1=Surface water (matching expected water); "
                    "2=Recurring flood (not populated in beta release); "
                    "3=Flood (unusual); "
                    "255=Insufficient data"
                )
            })
            
            # Add coordinate attributes
            ds.latitude.attrs.update({
                "long_name": "Latitude",
                "units": "degrees_north",
                "standard_name": "latitude"
            })
            
            ds.longitude.attrs.update({
                "long_name": "Longitude", 
                "units": "degrees_east",
                "standard_name": "longitude"
            })
            
            # Save file
            ds.to_netcdf(output_path)
            self.logger.info(f"NetCDF file saved: {output_path}")
            
            # Clean up memory
            del ds
            gc.collect()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error exporting NetCDF file: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False
    
    def export_flood_geotiff(self, data: np.ndarray, bounds: Dict, date: Union[datetime, str], 
                            output_path: Union[str, Path], pixel_size: float) -> bool:
        """
        Export MODIS Flood data as GeoTIFF format
        
        Args:
            data: Flood data array
            bounds: Boundary information dictionary {'left', 'right', 'bottom', 'top'}
            date: Date object or string
            output_path: Output file path
            pixel_size: Pixel size (degrees)
            
        Returns:
            Whether export was successful
        """
        try:
            import rasterio
            from rasterio.transform import from_bounds
            
            # Ensure output directory exists
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Handle date
            if isinstance(date, str):
                date_obj = datetime.strptime(date, '%Y%m%d')
            else:
                date_obj = date
            
            # Create affine transform
            transform = from_bounds(
                bounds['left'], bounds['bottom'],
                bounds['right'], bounds['top'],
                data.shape[1], data.shape[0]
            )
            
            # Create GeoTIFF file
            with rasterio.open(
                output_path,
                'w',
                driver='GTiff',
                height=data.shape[0],
                width=data.shape[1],
                count=1,
                dtype=data.dtype,
                crs='EPSG:4326',
                transform=transform,
                compress='lzw'
            ) as dst:
                dst.write(data, 1)
                
                # Add band attributes
                dst.set_band_description(1, "MODIS Flood Water")
                
                # Add metadata
                dst.update_tags(
                    description="MODIS Flood Data",
                    source="NASA MODIS MCDWD_L3_F2_NRT",
                    processing_date=date_obj.strftime("%Y-%m-%d"),
                    resolution=f"{pixel_size:.6f} degrees",
                    projection="Geographic (WGS84)"
                )
            
            self.logger.info(f"GeoTIFF file saved: {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error exporting GeoTIFF file: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False
    
    def export_flood_data(self, data: np.ndarray, bounds: Dict, date: Union[datetime, str], 
                         output_dir: Union[str, Path], pixel_size: float, 
                         formats: list = ['netcdf', 'geotiff']) -> Dict[str, bool]:
        """
        Export flood data in multiple formats
        
        Args:
            data: Flood data array
            bounds: Boundary information dictionary
            date: Date object or string
            output_dir: Output directory
            pixel_size: Pixel size
            formats: List of formats to export
            
        Returns:
            Dictionary of export results for each format
        """
        # Handle date
        if isinstance(date, str):
            date_str = date
            date_obj = datetime.strptime(date, '%Y%m%d')
        else:
            date_obj = date
            date_str = date_obj.strftime('%Y%m%d')
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        results = {}
        
        # Export NetCDF format
        if 'netcdf' in formats:
            nc_path = output_dir / f"MODIS_Flood_{date_str}.nc"
            results['netcdf'] = self.export_flood_netcdf(data, bounds, date, nc_path, pixel_size)
        
        # Export GeoTIFF format
        if 'geotiff' in formats:
            tif_path = output_dir / f"MODIS_Flood_{date_str}.tif"
            results['geotiff'] = self.export_flood_geotiff(data, bounds, date, tif_path, pixel_size)
        
        return results


def main():
    """Main function for running export task independently"""
    from datetime import datetime
    import numpy as np
    
    config = Config()
    exporter = FloodExporter(config)
    
    # Create sample data
    data = np.random.randint(0, 4, (100, 100), dtype=np.uint8)
    bounds = {
        'left': 100.0,
        'right': 120.0,
        'bottom': 20.0,
        'top': 40.0
    }
    date = datetime.now()
    output_dir = config.FLOOD_DIRS['output'] / 'test'
    
    print("Starting to export test data...")
    results = exporter.export_flood_data(data, bounds, date, output_dir, config.GRID_RES)
    
    for format_name, success in results.items():
        print(f"{format_name.upper()}: {'Success' if success else 'Failed'}")


if __name__ == "__main__":
    main()