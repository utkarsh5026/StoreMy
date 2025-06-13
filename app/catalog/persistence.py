"""
Catalog persistence management module.
Handles saving/loading catalog metadata to/from storage.
"""
import json
import time
import threading
from pathlib import Path
from typing import Dict

from .table_info import TableMetadata
from ..core.exceptions import DbException


class CatalogPersistence:
    """Handles catalog metadata persistence operations."""

    def __init__(self, catalog_dir: Path, backup_dir: Path):
        self.catalog_dir = catalog_dir
        self.backup_dir = backup_dir
        self.metadata_file = catalog_dir / "catalog_metadata.json"
        self._lock = threading.RLock()

    def save_catalog(self, tables: Dict[str, TableMetadata]) -> None:
        """Save catalog metadata to persistent storage."""
        with self._lock:
            try:
                data = {
                    "version": "1.0",
                    "created_at": time.time(),
                    "tables": {name: meta.to_dict() for name, meta in tables.items()}
                }

                # Write to temp file first, then rename (atomic operation)
                temp_file = self.metadata_file.with_suffix('.tmp')
                with open(temp_file, 'w') as f:
                    json.dump(data, f, indent=2)

                temp_file.rename(self.metadata_file)

            except Exception as e:
                raise DbException(f"Failed to save catalog: {e}")

    def load_catalog(self) -> Dict[str, TableMetadata]:
        """Load catalog metadata from persistent storage."""
        if not self.metadata_file.exists():
            return {}

        with self._lock:
            try:
                with open(self.metadata_file, 'r') as f:
                    data = json.load(f)

                tables = {}
                for table_name, table_data in data.get("tables", {}).items():
                    metadata = TableMetadata.from_dict(table_data)
                    tables[table_name] = metadata

                return tables

            except Exception as e:
                raise DbException(f"Failed to load catalog: {e}")

    def create_backup(self, tables: Dict[str, TableMetadata]) -> Path:
        """Create a backup of the catalog."""
        with self._lock:
            timestamp = int(time.time())
            backup_file = self.backup_dir / f"catalog_backup_{timestamp}.json"

            backup_data = {
                "version": "1.0",
                "created_at": time.time(),
                "tables": {name: meta.to_dict() for name, meta in tables.items()}
            }

            with open(backup_file, 'w') as f:
                json.dump(backup_data, f, indent=2)

            return backup_file

    def restore_from_backup(self, backup_file: Path) -> Dict[str, TableMetadata]:
        """Restore catalog from a backup file."""
        with self._lock:
            if not backup_file.exists():
                raise DbException(f"Backup file not found: {backup_file}")

            with open(backup_file, 'r') as f:
                backup_data = json.load(f)

            tables = {}
            for table_name, table_data in backup_data["tables"].items():
                metadata = TableMetadata.from_dict(table_data)
                tables[table_name] = metadata

            return tables
