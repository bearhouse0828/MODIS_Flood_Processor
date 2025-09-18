#!/usr/bin/env python3
"""
Get Airflow standalone credentials
"""
import os
import sys
from pathlib import Path

# Set Airflow environment
AIRFLOW_HOME = Path(__file__).parent
os.environ['AIRFLOW_HOME'] = str(AIRFLOW_HOME)

def get_credentials():
    """Get Airflow standalone credentials"""
    try:
        from airflow.models import User
        from airflow.utils.db import create_session
        
        with create_session() as session:
            users = session.query(User).all()
            
            if users:
                print("Found Airflow users:")
                for user in users:
                    print(f"  Username: {user.username}")
                    print(f"  Email: {user.email}")
                    print(f"  First Name: {user.first_name}")
                    print(f"  Last Name: {user.last_name}")
                    print("  Password: [HIDDEN]")
                    print("---")
            else:
                print("No users found in database")
                
    except Exception as e:
        print(f"Error getting credentials: {e}")
        print("\nTrying alternative method...")
        
        # Try to find credentials in logs or config
        try:
            import sqlite3
            db_path = AIRFLOW_HOME / "airflow.db"
            if db_path.exists():
                conn = sqlite3.connect(str(db_path))
                cursor = conn.cursor()
                
                # Try different table names
                tables = ['ab_user', 'user', 'users', 'auth_user']
                for table in tables:
                    try:
                        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}';")
                        if cursor.fetchone():
                            cursor.execute(f"SELECT * FROM {table} LIMIT 5;")
                            rows = cursor.fetchall()
                            print(f"Found table {table} with {len(rows)} rows")
                            for row in rows:
                                print(f"  {row}")
                    except:
                        continue
                
                conn.close()
        except Exception as e2:
            print(f"Alternative method also failed: {e2}")

if __name__ == "__main__":
    get_credentials()

