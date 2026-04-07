"""
scripts/backup.py
Database backup script for PostgreSQL.

Usage:
    python scripts/backup.py                    # Default backup
    python scripts/backup.py --output my_backup # Custom filename
    python scripts/backup.py --compress          # Compress backup
"""

import os
import sys
import argparse
import subprocess
from datetime import datetime
import logging

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config.settings import DB_CONFIG

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BACKUP_DIR = "backups"
os.makedirs(BACKUP_DIR, exist_ok=True)


def create_backup(filename: str = None, compress: bool = False) -> str:
    """
    Create PostgreSQL backup.
    
    Args:
        filename: Custom backup filename (without extension)
        compress: Whether to compress the backup
    
    Returns:
        Path to backup file
    """
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"insurance_fraud_{timestamp}"
    
    db = DB_CONFIG
    
    if compress:
        backup_file = f"{BACKUP_DIR}/{filename}.sql.gz"
        cmd = [
            "pg_dump",
            "-h", db["host"],
            "-p", str(db["port"]),
            "-U", db["user"],
            "-d", db["database"],
            "-F", "p",  # Plain SQL format
        ]
        cmd += ["-f", "-"]  # Output to stdout
        
        # Compress with gzip
        with open(backup_file, "wb") as f:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            import gzip
            with gzip.open(f, "wb") as gz:
                import shutil
                shutil.copyfileobj(proc.stdout, gz)
            proc.wait()
    else:
        backup_file = f"{BACKUP_DIR}/{filename}.sql"
        cmd = [
            "pg_dump",
            "-h", db["host"],
            "-p", str(db["port"]),
            "-U", db["user"],
            "-d", db["database"],
            "-f", backup_file,
            "-F", "p",  # Plain SQL format
        ]
        
        # Set PGPASSWORD environment variable
        env = os.environ.copy()
        env["PGPASSWORD"] = db["password"]
        
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise RuntimeError(f"Backup failed: {result.stderr}")
    
    file_size = os.path.getsize(backup_file)
    logger.info(f"Backup created: {backup_file} ({file_size:,} bytes)")
    
    return backup_file


def list_backups() -> list:
    """List all available backups."""
    if not os.path.exists(BACKUP_DIR):
        return []
    
    backups = []
    for f in os.listdir(BACKUP_DIR):
        if f.endswith(".sql") or f.endswith(".sql.gz"):
            path = os.path.join(BACKUP_DIR, f)
            stat = os.stat(path)
            backups.append({
                "filename": f,
                "path": path,
                "size": stat.st_size,
                "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
            })
    
    return sorted(backups, key=lambda x: x["created"], reverse=True)


def cleanup_old_backups(keep: int = 7):
    """
    Remove old backups, keeping only the most recent N.
    
    Args:
        keep: Number of backups to keep
    """
    backups = list_backups()
    
    if len(backups) <= keep:
        logger.info(f"No cleanup needed. {len(backups)} backups, keeping all.")
        return
    
    removed = 0
    for backup in backups[keep:]:
        os.remove(backup["path"])
        logger.info(f"Removed old backup: {backup['filename']}")
        removed += 1
    
    logger.info(f"Cleanup complete. Removed {removed} old backups.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Database backup utility")
    parser.add_argument("--output", "-o", help="Output filename")
    parser.add_argument("--compress", "-c", action="store_true", help="Compress backup")
    parser.add_argument("--list", "-l", action="store_true", help="List available backups")
    parser.add_argument("--cleanup", type=int, default=0, help="Cleanup old backups (keep N)")
    
    args = parser.parse_args()
    
    if args.list:
        backups = list_backups()
        print(f"\nAvailable backups in {BACKUP_DIR}:")
        print("-" * 60)
        for b in backups:
            print(f"{b['created']} | {b['filename']} | {b['size']:,} bytes")
    
    elif args.cleanup > 0:
        cleanup_old_backups(args.cleanup)
    
    else:
        backup_file = create_backup(args.output, args.compress)
        print(f"\nBackup successful: {backup_file}")
