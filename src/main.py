import logging
from pathlib import Path
from datetime import datetime, timedelta
import argparse
from typing import List, Optional, Tuple
import sys
import os

from config import Config
from downloader import MODISFloodDownloader
from processor import MODISFloodProcessor
from exporter import FloodExporter
from plotter import FloodPlotter


class FloodProcessor:
    """Main flood data processor class"""
    
    def __init__(self, config: Config = None):
        """Initialize processor"""
        self.config = config or Config()
        self.setup_logging()
        
        # Initialize all modules
        self.downloader = MODISFloodDownloader(self.config)
        self.processor = MODISFloodProcessor(self.config)
        self.exporter = FloodExporter(self.config)
        self.plotter = FloodPlotter(self.config)
        
    def setup_logging(self):
        """Setup logger"""
        self.logger = self.config.setup_logging('flood_main')
    
    def process_single_date(self, date: datetime) -> bool:
        """
        Process flood data for single date
        
        Args:
            date: Target date
            
        Returns:
            Whether processing was successful
        """
        try:
            self.logger.info(f"Starting to process flood data: {date.strftime('%Y-%m-%d')}")
            
            # 1. Download data
            self.logger.info("Step 1: Downloading MODIS flood data...")
            tile_files = self.downloader.download_tiles(date)
            
            if not tile_files:
                self.logger.warning(f"No flood data found for {date.strftime('%Y-%m-%d')}")
                return False
            
            self.logger.info(f"Successfully downloaded {len(tile_files)} tile files")
            
            # 2. Process data
            self.logger.info("Step 2: Processing tile data...")
            result = self.processor.process_tiles(tile_files)
            
            if result is None:
                self.logger.error("Data processing failed")
                return False
            
            output_data, output_bounds = result
            self.logger.info(f"Data processing completed: shape {output_data.shape}")
            
            # 3. Export data
            self.logger.info("Step 3: Exporting data...")
            output_path = self.config.get_flood_output_path(date)
            
            success = self.exporter.export_flood_netcdf(
                output_data,
                output_bounds,
                date,
                output_path,
                self.config.GRID_RES
            )
            
            if not success:
                self.logger.error("Data export failed")
                return False
            
            self.logger.info(f"Data exported to: {output_path}")
            
            # 4. Generate images
            self.logger.info("Step 4: Generating images...")
            plot_dir = output_path.parent / 'plots'
            plot_dir.mkdir(exist_ok=True)
            
            # Generate images for all regions
            self.plotter.plot_all_regions(output_path, plot_dir)
            
            self.logger.info(f"Images saved to: {plot_dir}")
            self.logger.info(f"Flood data processing completed: {date.strftime('%Y-%m-%d')}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing flood data: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False
    
    def process_date_range(self, start_date: datetime, end_date: datetime) -> Tuple[int, int]:
        """
        Process flood data for date range
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            (Number of successfully processed days, Total number of days)
        """
        current_date = start_date
        success_count = 0
        total_count = 0
        
        while current_date <= end_date:
            total_count += 1
            if self.process_single_date(current_date):
                success_count += 1
            current_date += timedelta(days=1)
        
        self.logger.info(f"Date range processing completed: {success_count}/{total_count} days successful")
        return success_count, total_count
    
    def process_latest_data(self, days_back: int = 1) -> bool:
        """
        Process latest flood data
        
        Args:
            days_back: Number of days back, default is 1 (yesterday)
            
        Returns:
            Whether processing was successful
        """
        target_date = datetime.now() - timedelta(days=days_back)
        return self.process_single_date(target_date)
    
    def check_data_availability(self, date: datetime) -> bool:
        """
        Check if data is available for specified date
        
        Args:
            date: Target date
            
        Returns:
            Whether data is available
        """
        return self.downloader.check_data_availability(date)
    
    def list_available_dates(self, start_date: datetime, end_date: datetime) -> List[datetime]:
        """
        List available data dates in specified range
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            List of available dates
        """
        available_dates = []
        current_date = start_date
        
        while current_date <= end_date:
            if self.check_data_availability(current_date):
                available_dates.append(current_date)
            current_date += timedelta(days=1)
        
        return available_dates


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='MODIS Flood Data Processor')
    parser.add_argument('--date', type=str, help='Process specified date (YYYYMMDD format)')
    parser.add_argument('--start-date', type=str, help='Start date (YYYYMMDD format)')
    parser.add_argument('--end-date', type=str, help='End date (YYYYMMDD format)')
    parser.add_argument('--latest', action='store_true', help='Process latest data (yesterday)')
    parser.add_argument('--check', type=str, help='Check data availability for specified date (YYYYMMDD format)')
    parser.add_argument('--list-available', action='store_true', help='List available data dates')
    parser.add_argument('--days-back', type=int, default=7, help='Number of days back when listing available data')
    
    args = parser.parse_args()
    
    # Initialize processor
    config = Config()
    processor = FloodProcessor(config)
    
    try:
        if args.check:
            # Check data availability
            date = datetime.strptime(args.check, '%Y%m%d')
            available = processor.check_data_availability(date)
            print(f"Data availability for {args.check}: {'Yes' if available else 'No'}")
            
        elif args.list_available:
            # List available data
            end_date = datetime.now() - timedelta(days=1)
            start_date = end_date - timedelta(days=args.days_back)
            available_dates = processor.list_available_dates(start_date, end_date)
            
            print(f"Available data dates in the last {args.days_back} days:")
            for date in available_dates:
                print(f"  {date.strftime('%Y-%m-%d')}")
            
        elif args.date:
            # Process specified date
            date = datetime.strptime(args.date, '%Y%m%d')
            success = processor.process_single_date(date)
            print(f"Processing result: {'Success' if success else 'Failed'}")
            
        elif args.start_date and args.end_date:
            # Process date range
            start_date = datetime.strptime(args.start_date, '%Y%m%d')
            end_date = datetime.strptime(args.end_date, '%Y%m%d')
            success_count, total_count = processor.process_date_range(start_date, end_date)
            print(f"Processing result: {success_count}/{total_count} days successful")
            
        elif args.latest:
            # Process latest data
            success = processor.process_latest_data()
            print(f"Processing result: {'Success' if success else 'Failed'}")
            
        else:
            # Default: process latest data
            print("No parameters specified, processing yesterday's data...")
            success = processor.process_latest_data()
            print(f"Processing result: {'Success' if success else 'Failed'}")
            
    except Exception as e:
        print(f"Program execution error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()