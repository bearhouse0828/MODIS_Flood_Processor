import os
# Set matplotlib backend before importing pyplot
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for Airflow

import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
from pathlib import Path
import numpy as np
from matplotlib.colors import ListedColormap, BoundaryNorm
from mpl_toolkits.axes_grid1.anchored_artists import AnchoredSizeBar
from datetime import datetime
import logging
from typing import Union, Optional

from config import Config


class FloodPlotter:
    """Flood data plotter"""
    
    def __init__(self, config: Config):
        self.config = config
        self.setup_logging()
        
        # Custom color mapping
        self.colors = [
            '#d6b4fc',  # Purple - No water
            '#0000FF',  # Blue - Water
            '#FFA500',  # Orange - Recurring flood
            'red',      # Red - Flood (unusual)
            'gray'      # Gray - Insufficient data
        ]
        self.flood_cmap = ListedColormap(self.colors)
        self.bounds = [-0.5, 0.5, 1.5, 2.5, 3.5, 255.5]  # Boundary values offset by 0.5 to align labels with color centers
        self.norm = BoundaryNorm(self.bounds, len(self.colors))
        
        # Define legend labels
        self.legend_labels = {
            0: 'No water',
            1: 'Water',
            2: 'Recurring flood',
            3: 'Flood (unusual)',
            255: 'Insufficient data'
        }
    
    def setup_logging(self):
        """Setup logger"""
        self.logger = self.config.setup_logging('flood_plotter')
    
    def plot_flood_data(self, nc_file: Union[str, Path], output_path: Optional[Union[str, Path]] = None):
        """Plot flood data map"""
        try:
            # Read data
            ds = xr.open_dataset(nc_file)
            
            # Sample data (every 10th point)
            flood_data = ds.flood.isel(
                latitude=slice(None, None, 10),
                longitude=slice(None, None, 10)
            )
            
            # Extract date from filename (YYYYMMDD)
            date_str = os.path.basename(nc_file).split('_')[-1].split('.')[0]
            date_obj = datetime.strptime(date_str, '%Y%m%d')
            formatted_date = date_obj.strftime('%B %d, %Y')
            
            # Create figure, adjust size and margins
            fig = plt.figure(figsize=(12, 10))
            plt.subplots_adjust(left=0.1, right=0.9, top=0.95, bottom=0.15)
            ax = plt.axes(projection=ccrs.PlateCarree())
            
            # Set map extent
            ax.set_extent([
                self.config.GRID_WEST,    # Western boundary
                self.config.GRID_EAST,    # Eastern boundary
                self.config.GRID_SOUTH,   # Southern boundary
                self.config.GRID_NORTH    # Northern boundary
            ])
            
            # Add coastlines and borders
            ax.add_feature(cfeature.COASTLINE.with_scale('10m'), 
                          linewidth=1.2, 
                          edgecolor='black',
                          facecolor='none')
            ax.add_feature(cfeature.BORDERS.with_scale('10m'), 
                          linestyle=':', 
                          linewidth=1.2, 
                          color='gray',
                          facecolor='none',
                          zorder=3)
            ax.add_feature(cfeature.LAND, facecolor='lightgreen', alpha=0.3)
            
            # Add longitude-latitude grid
            gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', 
                             alpha=0.5, linestyle=':')
            gl.top_labels = False
            gl.right_labels = False
            gl.xformatter = LONGITUDE_FORMATTER
            gl.yformatter = LATITUDE_FORMATTER
            gl.xlabel_style = {'size': 13}
            gl.ylabel_style = {'size': 13}
            
            # Plot flood data
            im = ax.pcolormesh(
                flood_data.longitude,
                flood_data.latitude,
                flood_data,
                cmap=self.flood_cmap,
                norm=self.norm,
                transform=ccrs.PlateCarree()
            )
            
            # Add colorbar, align labels with color centers, set horizontal orientation
            cbar_ax = fig.add_axes([0.19, 0.08, 0.6, 0.02])
            cbar = plt.colorbar(im, cax=cbar_ax, 
                               orientation='horizontal',
                               ticks=[0, 1, 2, 3, 255])
            cbar.ax.set_xticklabels([
                'No water',
                'Water',
                'Recurring flood',
                'Flood (unusual)',
                'Insufficient data'
            ], fontsize=10)
            
            # Add title
            title_text = f'Asia-Oceania Flood Monitoring\n{formatted_date}'
            ax.text(94.5, 65.6, title_text,
                    fontsize=20, ha='center', va='center',
                    family='DejaVu Sans')
            
            # Set figure border
            ax.spines['geo'].set_edgecolor('black')
            
            # Save or show figure
            if output_path:
                plt.savefig(output_path, dpi=300, bbox_inches='tight', 
                           facecolor='white', edgecolor='none')
                self.logger.info(f"Image saved to: {output_path}")
            else:
                plt.show()
                
            plt.close()
            
        except Exception as e:
            self.logger.error(f"Error plotting: {e}")
            raise

    def plot_regional_floods(self, nc_file: Union[str, Path], output_dir: Union[str, Path]):
        """
        Plot flood maps for multiple regions
        
        Args:
            nc_file: Input flood data file
            output_dir: Output directory
        """
        # Define U.S. river basin ranges
        regions = {
            'Columbia River Basin': {'lon': (-125, -116), 'lat': (42, 49)}
        }

        try:
            # Read data
            ds = xr.open_dataset(nc_file)
            
            # Sample data (every 3rd point)
            flood_data = ds.flood.isel(
                latitude=slice(None, None, 3),
                longitude=slice(None, None, 3)
            )
            sampled_lons = ds.longitude[::3]
            sampled_lats = ds.latitude[::3]
            
            # Extract date from filename
            date_str = os.path.basename(nc_file).split('_')[-1].split('.')[0]
            date_obj = datetime.strptime(date_str, '%Y%m%d')
            formatted_date = date_obj.strftime('%B %d, %Y')
            
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)
            
            # Loop through each region
            for region_name, bounds in regions.items():
                self.logger.info(f"Plotting flood map for {region_name}...")
                
                # Create figure and subplot
                fig = plt.figure(figsize=(12, 10))
                gs = fig.add_gridspec(2, 1, height_ratios=[1, 0.04], hspace=0.05)
                
                # Main map subplot
                ax = fig.add_subplot(gs[0], projection=ccrs.PlateCarree())
                
                # Set map extent
                ax.set_extent([
                    bounds['lon'][0],  # Western boundary
                    bounds['lon'][1],  # Eastern boundary
                    bounds['lat'][0],  # Southern boundary
                    bounds['lat'][1]   # Northern boundary
                ])
                
                # Add coastlines and borders
                ax.add_feature(cfeature.COASTLINE.with_scale('10m'), 
                              linewidth=1.5, 
                              edgecolor='black',
                              facecolor='none')
                ax.add_feature(cfeature.BORDERS.with_scale('10m'), 
                              linestyle=':', 
                              linewidth=1.5, 
                              color='gray',
                              facecolor='none',
                              zorder=3)

                # Add longitude-latitude grid
                gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', 
                                alpha=0.5, linestyle=':')
                gl.top_labels = False
                gl.right_labels = False
                gl.xformatter = LONGITUDE_FORMATTER
                gl.yformatter = LATITUDE_FORMATTER
                gl.xlabel_style = {'size': 12}
                gl.ylabel_style = {'size': 12}
                
                # Plot flood data (using sampled data)
                im = ax.pcolormesh(
                    sampled_lons,
                    sampled_lats,
                    flood_data,
                    cmap=self.flood_cmap,
                    norm=self.norm,
                    transform=ccrs.PlateCarree()
                )
                
                # Add title
                ax.set_title(f'{region_name} Flooding\n{formatted_date}', 
                            fontsize=20, 
                            pad=20)
                
                # Add colorbar
                cbar_ax = fig.add_subplot(gs[1])
                cbar = plt.colorbar(im, cax=cbar_ax, 
                                  orientation='horizontal',
                                  ticks=[0, 1, 2, 3, 255])
                cbar.ax.set_xticklabels([
                    'No water',
                    'Water',
                    'Recurring flood',
                    'Flood (unusual)',
                    'Insufficient data'
                ], fontsize=10)
                
                # Save image
                output_path = os.path.join(output_dir, f"flood_{region_name}_{date_str}.png")
                plt.savefig(output_path, dpi=150, bbox_inches='tight')
                plt.close()
                
                self.logger.info(f"Saved {region_name} flood map to: {output_path}")
                
        except Exception as e:
            self.logger.error(f"Error plotting regional flood maps: {e}")
            raise

    def plot_all_regions(self, nc_file: Union[str, Path], output_dir: Union[str, Path]):
        """
        Plot flood maps for all regions (full region, Mekong region, sub-regions)
        
        Args:
            nc_file: Input flood data file
            output_dir: Output directory
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Extract date from filename
        date_str = os.path.basename(nc_file).split('_')[-1].split('.')[0]
        
        # Plot full region map
        full_output = output_dir / f"flood_full_{date_str}.png"
        
        # Plot sub-region maps
        self.plot_regional_floods(nc_file, output_dir)


def main():
    """Main function"""
    from datetime import datetime, timedelta
    
    config = Config()
    plotter = FloodPlotter(config)
    
    # Example: plot latest flood data
    processed_dir = Path(config.FLOOD_DIRS['output'])
    nc_files = list(processed_dir.rglob('*.nc'))
    if nc_files:
        latest_file = max(nc_files, key=lambda x: x.stat().st_mtime)
        output_dir = processed_dir / 'plots'
        
        print(f"Plotting flood maps: {latest_file}")
        plotter.plot_all_regions(latest_file, output_dir)
    else:
        print("No NetCDF files found")


if __name__ == "__main__":
    main()