"""
scripts/backup.py
Database backup script using Python/SQLAlchemy (no pg_dump required).

Usage:
    python scripts/backup.py                    # Default backup
    python scripts/backup.py --output my_backup # Custom filename
"""

import os
import sys
import argparse
import json
import logging
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from sqlalchemy import create_engine, text
from configs.settings import DATABASE_URL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BACKUP_DIR = "backups"
os.makedirs(BACKUP_DIR, exist_ok=True)


def create_backup(filename: str = None) -> str:
    """
    Create database backup as JSON.
    
    Args:
        filename: Custom backup filename (without extension)
    
    Returns:
        Path to backup file
    """
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"insurance_fraud_{timestamp}"
    
    backup_file = f"{BACKUP_DIR}/{filename}.json"
    
    engine = create_engine(DATABASE_URL)
    
    try:
        with engine.connect() as conn:
            # Get all claims
            result = conn.execute(text("SELECT * FROM claims"))
            rows = result.fetchall()
            
            # Get column names
            columns = result.keys()
            
            # Convert to list of dicts
            data = []
            for row in rows:
                data.append(dict(zip(columns, row)))
        
        # Write to JSON
        backup_data = {
            "timestamp": datetime.now().isoformat(),
            "table": "claims",
            "row_count": len(data),
            "data": data
        }
        
        with open(backup_file, "w") as f:
            json.dump(backup_data, f, indent=2, default=str)
        
        file_size = os.path.getsize(backup_file)
        logger.info(f"Backup created: {backup_file} ({file_size:,} bytes)")
        logger.info(f"Rows backed up: {len(data)}")
        
        return backup_file
    
    finally:
        engine.dispose()


def restore_backup(backup_file: str) -> int:
    """
    Restore database from JSON backup.
    
    Args:
        backup_file: Path to backup file
    
    Returns:
        Number of rows restored
    """
    if not os.path.exists(backup_file):
        raise FileNotFoundError(f"Backup file not found: {backup_file}")
    
    import pandas as pd
    
    with open(backup_file, "r") as f:
        backup_data = json.load(f)
    
    data = backup_data.get("data", [])
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    df = df.drop(columns=['id'], errors='ignore')
    
    engine = create_engine(DATABASE_URL)
    
    try:
        with engine.begin() as conn:
            # Clear existing data
            conn.execute(text("DELETE FROM claims"))
        
        # Use pandas to insert (handles all column types)
        df.to_sql("claims", engine, if_exists="append", index=False)
        
        logger.info(f"Restored {len(df)} rows from {backup_file}")
        return len(df)
    
    finally:
        engine.dispose()


def list_backups() -> list:
    """List all available backups."""
    if not os.path.exists(BACKUP_DIR):
        return []
    
    backups = []
    for f in os.listdir(BACKUP_DIR):
        if f.endswith(".json"):
            path = os.path.join(BACKUP_DIR, f)
            stat = os.stat(path)
            
            # Get row count from file
            with open(path, "r") as file:
                try:
                    data = json.load(file)
                    row_count = data.get("row_count", 0)
                except:
                    row_count = 0
            
            backups.append({
                "filename": f,
                "path": path,
                "size": stat.st_size,
                "rows": row_count,
                "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
            })
    
    return sorted(backups, key=lambda x: x["created"], reverse=True)


def verify_backup(backup_file: str) -> dict:
    """Verify backup file is valid."""
    try:
        with open(backup_file, "r") as f:
            data = json.load(f)
        
        return {
            "valid": True,
            "timestamp": data.get("timestamp"),
            "row_count": len(data.get("data", [])),
            "table": data.get("table"),
        }
    except Exception as e:
        return {"valid": False, "error": str(e)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Database backup utility")
    parser.add_argument("--output", "-o", help="Output filename")
    parser.add_argument("--list", "-l", action="store_true", help="List available backups")
    parser.add_argument("--restore", "-r", help="Restore from backup file")
    parser.add_argument("--verify", "-v", help="Verify backup file")
    
    args = parser.parse_args()
    
    if args.list:
        backups = list_backups()
        print(f"\nAvailable backups in {BACKUP_DIR}:")
        print("-" * 70)
        for b in backups:
            print(f"{b['created']} | {b['filename']} | {b['rows']:,} rows | {b['size']:,} bytes")
    
    elif args.verify:
        result = verify_backup(args.verify)
        if result["valid"]:
            print(f"\nBackup is valid:")
            print(f"  Table: {result['table']}")
            print(f"  Rows: {result['row_count']}")
            print(f"  Created: {result['timestamp']}")
        else:
            print(f"\nBackup is INVALID: {result['error']}")
    
    elif args.restore:
        confirm = input(f"Restore from {args.restore}? This will replace current data. (yes/no): ")
        if confirm.lower() == "yes":
            count = restore_backup(args.restore)
            print(f"\nRestored {count} rows.")
        else:
            print("Restore cancelled.")
    
    else:
        backup_file = create_backup(args.output)
        print(f"\nBackup successful: {backup_file}")
