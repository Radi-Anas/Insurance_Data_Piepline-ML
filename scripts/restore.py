"""
scripts/restore.py
Database restore script for PostgreSQL.

Usage:
    python scripts/restore.py backups/insurance_fraud_20260407.sql
    python scripts/restore.py backups/insurance_fraud.sql.gz
"""

import os
import sys
import argparse
import subprocess
import logging

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from configs.settings import DB_CONFIG
from scripts.backup import list_backups

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def restore_backup(backup_file: str, drop_tables: bool = True) -> bool:
    """
    Restore database from backup.
    
    Args:
        backup_file: Path to backup file (.sql or .sql.gz)
        drop_tables: Whether to drop existing tables before restore
    
    Returns:
        True if successful
    """
    if not os.path.exists(backup_file):
        raise FileNotFoundError(f"Backup file not found: {backup_file}")
    
    db = DB_CONFIG
    
    # Check if compressed
    if backup_file.endswith(".gz"):
        # Decompress and restore
        cmd = [
            "psql",
            "-h", db["host"],
            "-p", str(db["port"]),
            "-U", db["user"],
            "-d", db["database"],
        ]
        
        env = os.environ.copy()
        env["PGPASSWORD"] = db["password"]
        
        import gzip
        with gzip.open(backup_file, "rb") as f:
            proc = subprocess.Popen(cmd, stdin=f, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate()
        
        if proc.returncode != 0:
            raise RuntimeError(f"Restore failed: {stderr.decode()}")
    else:
        cmd = [
            "psql",
            "-h", db["host"],
            "-p", str(db["port"]),
            "-U", db["user"],
            "-d", db["database"],
            "-f", backup_file,
        ]
        
        env = os.environ.copy()
        env["PGPASSWORD"] = db["password"]
        
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise RuntimeError(f"Restore failed: {result.stderr}")
    
    logger.info(f"Restore successful: {backup_file}")
    return True


def verify_restore() -> dict:
    """
    Verify the restore was successful by checking row counts.
    
    Returns:
        Dictionary with verification results
    """
    from sqlalchemy import create_engine, text
    from configs.settings import DATABASE_URL
    
    engine = create_engine(DATABASE_URL)
    
    try:
        with engine.connect() as conn:
            # Check claims table
            result = conn.execute(text("SELECT COUNT(*) FROM claims"))
            claims_count = result.scalar()
            
            # Check for fraud count
            result = conn.execute(text("SELECT SUM(is_fraud) FROM claims"))
            fraud_count = result.scalar() or 0
            
            return {
                "claims_count": claims_count,
                "fraud_count": int(fraud_count),
                "legitimate_count": claims_count - int(fraud_count),
                "fraud_rate": round(fraud_count / claims_count * 100, 1) if claims_count > 0 else 0,
            }
    finally:
        engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Database restore utility")
    parser.add_argument("file", nargs="?", help="Backup file to restore")
    parser.add_argument("--list", "-l", action="store_true", help="List available backups")
    parser.add_argument("--verify", "-v", action="store_true", help="Verify current data")
    parser.add_argument("--drop-tables", action="store_true", default=False, help="Drop tables before restore")
    
    args = parser.parse_args()
    
    if args.list:
        backups = list_backups()
        print(f"\nAvailable backups:")
        print("-" * 60)
        for b in backups:
            print(f"{b['created']} | {b['filename']} | {b['size']:,} bytes")
    
    elif args.verify:
        result = verify_restore()
        print(f"\nDatabase verification:")
        print(f"  Total claims: {result['claims_count']}")
        print(f"  Fraud: {result['fraud_count']}")
        print(f"  Legitimate: {result['legitimate_count']}")
        print(f"  Fraud rate: {result['fraud_rate']}%")
    
    elif args.file:
        confirm = input(f"Restore from {args.file}? This will replace current data. (yes/no): ")
        if confirm.lower() == "yes":
            restore_backup(args.file, args.drop_tables)
            print("\nRestore complete!")
            
            # Verify
            result = verify_restore()
            print(f"Verification:")
            print(f"  Claims restored: {result['claims_count']}")
        else:
            print("Restore cancelled.")
    
    else:
        parser.print_help()
