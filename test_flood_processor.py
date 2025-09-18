#!/usr/bin/env python3
"""
MODIS Flood Data Processor Test Script
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from config import Config
from downloader import MODISFloodDownloader
from processor import MODISFloodProcessor
from exporter import FloodExporter
from plotter import FloodPlotter


def test_config():
    """Test configuration module"""
    print("Testing configuration module...")
    try:
        config = Config()
        print(f"✓ Configuration loaded successfully")
        print(f"  - Project root directory: {config.ROOT_DIR}")
        print(f"  - Data directory: {config.DATA_DIR}")
        print(f"  - Number of valid tiles: {len(config.VALID_TILES)}")
        return True
    except Exception as e:
        print(f"✗ Configuration loading failed: {e}")
        return False


def test_downloader():
    """Test downloader module"""
    print("\nTesting downloader module...")
    try:
        config = Config()
        downloader = MODISFloodDownloader(config)
        print(f"✓ Downloader initialized successfully")
        
        # Test data availability check
        yesterday = datetime.now() - timedelta(days=1)
        available = downloader.check_data_availability(yesterday)
        print(f"  - Yesterday's data availability: {'Yes' if available else 'No'}")
        return True
    except Exception as e:
        print(f"✗ Downloader test failed: {e}")
        return False


def test_processor():
    """Test processor module"""
    print("\nTesting processor module...")
    try:
        config = Config()
        processor = MODISFloodProcessor(config)
        print(f"✓ Processor initialized successfully")
        
        # Test tile file retrieval
        yesterday = datetime.now() - timedelta(days=1)
        tile_files = processor.get_tile_files(yesterday.strftime('%Y%m%d'))
        print(f"  - Number of tile files found: {len(tile_files)}")
        return True
    except Exception as e:
        print(f"✗ Processor test failed: {e}")
        return False


def test_exporter():
    """Test exporter module"""
    print("\nTesting exporter module...")
    try:
        config = Config()
        exporter = FloodExporter(config)
        print(f"✓ Exporter initialized successfully")
        return True
    except Exception as e:
        print(f"✗ Exporter test failed: {e}")
        return False


def test_plotter():
    """Test plotter module"""
    print("\nTesting plotter module...")
    try:
        config = Config()
        plotter = FloodPlotter(config)
        print(f"✓ Plotter initialized successfully")
        print(f"  - Color mapping: {len(plotter.colors)} colors")
        return True
    except Exception as e:
        print(f"✗ Plotter test failed: {e}")
        return False


def test_integration():
    """Test module integration"""
    print("\nTesting module integration...")
    try:
        from main import FloodProcessor
        processor = FloodProcessor()
        print(f"✓ Main processor initialized successfully")
        return True
    except Exception as e:
        print(f"✗ Integration test failed: {e}")
        return False


def main():
    """Main test function"""
    print("MODIS Flood Data Processor Test Started")
    print("=" * 50)
    
    tests = [
        test_config,
        test_downloader,
        test_processor,
        test_exporter,
        test_plotter,
        test_integration
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print("\n" + "=" * 50)
    print(f"Test completed: {passed}/{total} passed")
    
    if passed == total:
        print("✓ All tests passed! System is ready to use.")
        return 0
    else:
        print("✗ Some tests failed, please check error messages.")
        return 1


if __name__ == "__main__":
    sys.exit(main())