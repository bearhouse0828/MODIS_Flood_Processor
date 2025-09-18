import requests
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Optional
from config import Config


class MODISFloodDownloader:
    """Download MODIS Flood data from NASA servers"""
    
    def __init__(self, config: Config):
        self.config = config
        self.setup_logging()
        
        # NASA MODIS data server configuration
        self.product_id = config.MODIS_PRODUCT_ID
        self.base_url = config.MODIS_BASE_URL
        self.backup_url = config.MODIS_BACKUP_URL
        
        # Use valid tile list from configuration
        self.valid_tiles = config.VALID_TILES
        
        # Create output directory
        self.output_dir = Path(config.FLOOD_DIRS['raw'])
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def setup_logging(self):
        """Setup logger"""
        self.logger = self.config.setup_logging('flood_downloader')
    
    def download_tiles(self, date: datetime) -> List[str]:
        """
        Download MODIS flood tiles for specified date
        
        Args:
            date: Target date
            
        Returns:
            List of downloaded file paths
        """
        year = date.year
        doy = date.timetuple().tm_yday
        
        # Create download directory
        out_dir = self.output_dir / f"{year}/{doy:03d}"
        out_dir.mkdir(parents=True, exist_ok=True)
        
        downloaded_files = []
        
        self.logger.info(f"Starting download of MODIS flood data for {date.strftime('%Y-%m-%d')}")
        self.logger.info(f"Target directory: {out_dir}")
        self.logger.info(f"Need to download {len(self.valid_tiles)} tiles")
        
        for h, v in self.valid_tiles:
            tile_id = f"h{h:02d}v{v:02d}"
            filename = f"{self.product_id}.A{year}{doy:03d}.{tile_id}.061.tif"
            url = f"{self.base_url}/{year}/{doy:03d}/{filename}"
            
            # Download file
            out_file = out_dir / filename
            if not out_file.exists():
                try:
                    self.logger.debug(f"Downloading: {filename}")
                    response = requests.get(url, timeout=30)
                    if response.status_code == 200:
                        out_file.write_bytes(response.content)
                        downloaded_files.append(str(out_file))
                        self.logger.debug(f"Successfully downloaded: {filename}")
                    else:
                        # If main URL fails, try backup URL
                        backup_url = f"{self.backup_url}/{year}/{doy:03d}/{filename}"
                        self.logger.warning(f"Main URL failed ({response.status_code}), trying backup URL: {filename}")
                        response = requests.get(backup_url, timeout=30)
                        if response.status_code == 200:
                            out_file.write_bytes(response.content)
                            downloaded_files.append(str(out_file))
                            self.logger.debug(f"Backup URL successfully downloaded: {filename}")
                        else:
                            self.logger.warning(f"Download failed {filename}: HTTP {response.status_code}")
                except Exception as e:
                    self.logger.error(f"Error downloading {filename}: {e}")
            else:
                downloaded_files.append(str(out_file))
                self.logger.debug(f"File already exists, skipping: {filename}")
        
        self.logger.info(f"Download completed: {len(downloaded_files)}/{len(self.valid_tiles)} files")
        return downloaded_files
    
    def download_date_range(self, start_date: datetime, end_date: datetime) -> List[str]:
        """
        Download all flood data for specified date range
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            List of all downloaded file paths
        """
        all_files = []
        current_date = start_date
        
        while current_date <= end_date:
            files = self.download_tiles(current_date)
            all_files.extend(files)
            current_date = current_date.replace(day=current_date.day + 1)
        
        self.logger.info(f"Date range download completed: {len(all_files)} files")
        return all_files
    
    def check_data_availability(self, date: datetime) -> bool:
        """
        Check if data is available for specified date
        
        Args:
            date: Target date
            
        Returns:
            Whether data is available
        """
        year = date.year
        doy = date.timetuple().tm_yday
        
        # Check if first tile is available
        h, v = self.valid_tiles[0]
        tile_id = f"h{h:02d}v{v:02d}"
        filename = f"{self.product_id}.A{year}{doy:03d}.{tile_id}.061.tif"
        url = f"{self.base_url}/{year}/{doy:03d}/{filename}"
        
        try:
            response = requests.head(url, timeout=10)
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Error checking data availability: {e}")
            return False


def main():
    """Main function for running download task independently"""
    from datetime import datetime, timedelta
    
    config = Config()
    downloader = MODISFloodDownloader(config)
    
    # Download yesterday's data as example
    yesterday = datetime.now() - timedelta(days=1)
    
    print(f"Starting download of MODIS flood data for {yesterday.strftime('%Y-%m-%d')}...")
    files = downloader.download_tiles(yesterday)
    print(f"Download completed: {len(files)} files")


if __name__ == "__main__":
    main()