#!/usr/bin/env python3
"""
Upload flood data files to S3 bucket with proper organization.
Structure: s3://floodmap/{region}/{date}/{filename}
"""

import os
import boto3
import re
from pathlib import Path
from botocore.exceptions import BotoCoreError, ClientError

def upload_flood_data_to_s3():
    """Upload flood data files to S3 with proper organization."""
    
    # S3 configuration
    bucket_name = "floodmap"
    s3_client = boto3.client('s3')
    
    # Base directory containing the data
    base_dir = Path("/Users/jjpeng/Downloads/flood_processor/data/flood/output")
    
    # Find all PNG files in plots directories
    png_files = list(base_dir.rglob("*.png"))
    
    if not png_files:
        print("No PNG files found to upload.")
        return
    
    print(f"Found {len(png_files)} PNG files to upload:")
    
    uploaded_count = 0
    failed_count = 0
    
    for png_file in png_files:
        try:
            # Extract region and date from filename
            # Expected format: flood_{region}_{YYYYMMDD}.png
            filename = png_file.name
            stem = png_file.stem
            
            # Parse filename to extract region and date
            pattern = re.compile(r"^flood_(?P<region>.+?)_(?P<date>\d{8})$")
            match = pattern.match(stem)
            
            if not match:
                print(f"  ⚠️  Skipping {filename} - doesn't match expected pattern")
                continue
            
            region = match.group("region")
            date_str = match.group("date")
            
            # Convert YYYYMMDD to YYYY-MM-DD for S3 key
            iso_date = f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}"
            
            # Construct S3 key
            s3_key = f"{region}/{iso_date}/{filename}"
            
            # Upload file
            print(f"  📤 Uploading {filename} -> s3://{bucket_name}/{s3_key}")
            
            s3_client.upload_file(
                str(png_file),
                bucket_name,
                s3_key,
                ExtraArgs={
                    "ContentType": "image/png",
                    "CacheControl": "public, max-age=3600"  # Cache for 1 hour
                }
            )
            
            uploaded_count += 1
            print(f"  ✅ Successfully uploaded {filename}")
            
        except (BotoCoreError, ClientError) as e:
            print(f"  ❌ Failed to upload {filename}: {e}")
            failed_count += 1
        except Exception as e:
            print(f"  ❌ Unexpected error uploading {filename}: {e}")
            failed_count += 1
    
    print(f"\n📊 Upload Summary:")
    print(f"  ✅ Successfully uploaded: {uploaded_count}")
    print(f"  ❌ Failed uploads: {failed_count}")
    print(f"  📁 Total files processed: {len(png_files)}")
    
    # Update availability.json if any files were uploaded
    if uploaded_count > 0:
        print(f"\n🔄 Updating availability.json...")
        update_availability_manifest(s3_client, bucket_name)

def update_availability_manifest(s3_client, bucket_name):
    """Update the availability.json manifest file."""
    try:
        # List all objects in the bucket
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name)
        
        regions_to_dates = {}
        
        # Parse S3 keys to build region/date mapping
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Skip manifest itself
                if key == "availability.json":
                    continue
                
                parts = key.split("/")
                if len(parts) >= 3 and parts[2].endswith(".png"):
                    region, date_part = parts[0], parts[1]
                    # date_part should be YYYY-MM-DD
                    if len(date_part) == 10 and date_part.count("-") == 2:
                        if region not in regions_to_dates:
                            regions_to_dates[region] = set()
                        regions_to_dates[region].add(date_part)
        
        # Create manifest
        manifest = {
            "regions": {region: sorted(list(dates)) for region, dates in regions_to_dates.items()}
        }
        
        # Upload manifest
        import json
        body = json.dumps(manifest, indent=2).encode("utf-8")
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key="availability.json",
            Body=body,
            ContentType="application/json",
            CacheControl="no-cache, no-store"
        )
        
        print(f"  ✅ Updated availability.json with {len(regions_to_dates)} regions")
        
    except Exception as e:
        print(f"  ❌ Failed to update availability.json: {e}")

if __name__ == "__main__":
    print("🚀 Starting flood data upload to S3...")
    upload_flood_data_to_s3()
    print("🏁 Upload process completed!")
