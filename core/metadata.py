"""
MetadataService: Manages object metadata and shard locations
"""
import sqlite3
import asyncio
import aiofiles
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)

@dataclass
class ObjectMetadata:
    """Metadata for a stored object"""
    key: str
    size: int
    content_type: str
    created_at: datetime
    updated_at: datetime
    version: int
    checksum: str
    shard_locations: List[Dict[str, str]]
    erasure_params: Dict[str, int]
    regions: List[str]

class MetadataService:
    """Service for managing object metadata"""
    
    def __init__(self, db_path: str = "metadata.db"):
        self.db_path = db_path
        self.lock = asyncio.Lock()
        self._init_db()
    
    def _init_db(self):
        """Initialize the SQLite database with required tables"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS objects (
                key TEXT PRIMARY KEY,
                size INTEGER,
                content_type TEXT,
                created_at TEXT,
                updated_at TEXT,
                version INTEGER,
                checksum TEXT,
                shard_locations TEXT,
                erasure_params TEXT,
                regions TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS shards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                object_key TEXT,
                shard_index INTEGER,
                node_id TEXT,
                shard_path TEXT,
                created_at TEXT,
                FOREIGN KEY (object_key) REFERENCES objects (key)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    async def store_object_metadata(self, metadata: ObjectMetadata) -> bool:
        """Store object metadata in the database"""
        async with self.lock:
            try:
                conn = sqlite3.connect(self.db_path, timeout=30.0)
                conn.execute("PRAGMA journal_mode=WAL")
                cursor = conn.cursor()
                
                shard_locations_json = json.dumps(metadata.shard_locations)
                erasure_params_json = json.dumps(metadata.erasure_params)
                regions_json = json.dumps(metadata.regions)
                
                cursor.execute('''
                    INSERT OR REPLACE INTO objects 
                    (key, size, content_type, created_at, updated_at, version, checksum, 
                     shard_locations, erasure_params, regions)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    metadata.key,
                    metadata.size,
                    metadata.content_type,
                    metadata.created_at.isoformat(),
                    metadata.updated_at.isoformat(),
                    metadata.version,
                    metadata.checksum,
                    shard_locations_json,
                    erasure_params_json,
                    regions_json
                ))
                
                cursor.execute('DELETE FROM shards WHERE object_key = ?', (metadata.key,))
                for i, shard_location in enumerate(metadata.shard_locations):
                    cursor.execute('''
                        INSERT INTO shards (object_key, shard_index, node_id, shard_path, created_at)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        metadata.key,
                        i,
                        shard_location['node_id'],
                        shard_location['shard_path'],
                        datetime.now().isoformat()
                    ))
                
                conn.commit()
                conn.close()
                logger.info(f"Stored metadata for object: {metadata.key}")
                return True
                
            except Exception as e:
                logger.error(f"Failed to store metadata for {metadata.key}: {e}")
                return False
    
    async def get_object_metadata(self, key: str) -> Optional[ObjectMetadata]:
        """Retrieve object metadata by key"""
        async with self.lock:
            try:
                conn = sqlite3.connect(self.db_path, timeout=30.0)
                conn.execute("PRAGMA journal_mode=WAL")
                cursor = conn.cursor()
                
                cursor.execute('SELECT * FROM objects WHERE key = ?', (key,))
                row = cursor.fetchone()
                
                if not row:
                    return None
                
                # Parse JSON fields
                shard_locations = json.loads(row[7])
                erasure_params = json.loads(row[8])
                regions = json.loads(row[9])
                
                metadata = ObjectMetadata(
                    key=row[0],
                    size=row[1],
                    content_type=row[2],
                    created_at=datetime.fromisoformat(row[3]),
                    updated_at=datetime.fromisoformat(row[4]),
                    version=row[5],
                    checksum=row[6],
                    shard_locations=shard_locations,
                    erasure_params=erasure_params,
                    regions=regions
                )
                
                conn.close()
                return metadata
                
            except Exception as e:
                logger.error(f"Failed to retrieve metadata for {key}: {e}")
                return None
    
    async def delete_object_metadata(self, key: str) -> bool:
        """Delete object metadata"""
        async with self.lock:
            try:
                conn = sqlite3.connect(self.db_path, timeout=30.0)
                conn.execute("PRAGMA journal_mode=WAL")
                cursor = conn.cursor()
                
                cursor.execute('DELETE FROM objects WHERE key = ?', (key,))
                cursor.execute('DELETE FROM shards WHERE object_key = ?', (key,))
                
                conn.commit()
                conn.close()
                logger.info(f"Deleted metadata for object: {key}")
                return True
                
            except Exception as e:
                logger.error(f"Failed to delete metadata for {key}: {e}")
                return False
    
    async def list_objects(self, prefix: str = "") -> List[str]:
        """List all object keys, optionally filtered by prefix"""
        async with self.lock:
            try:
                conn = sqlite3.connect(self.db_path, timeout=30.0)
                conn.execute("PRAGMA journal_mode=WAL")
                cursor = conn.cursor()
                
                if prefix:
                    cursor.execute('SELECT key FROM objects WHERE key LIKE ?', (f"{prefix}%",))
                else:
                    cursor.execute('SELECT key FROM objects')
                
                keys = [row[0] for row in cursor.fetchall()]
                conn.close()
                return keys
                
            except Exception as e:
                logger.error(f"Failed to list objects: {e}")
                return []
    
    async def get_shard_locations(self, key: str) -> List[Dict[str, str]]:
        """Get shard locations for an object"""
        metadata = await self.get_object_metadata(key)
        if metadata:
            return metadata.shard_locations
        return []
    
    async def update_shard_location(self, key: str, shard_index: int, new_node_id: str, new_shard_path: str) -> bool:
        """Update shard location after repair/replication"""
        async with self.lock:
            try:
                conn = sqlite3.connect(self.db_path, timeout=30.0)
                conn.execute("PRAGMA journal_mode=WAL")
                cursor = conn.cursor()
                
                # Update shards table
                cursor.execute('''
                    UPDATE shards 
                    SET node_id = ?, shard_path = ?
                    WHERE object_key = ? AND shard_index = ?
                ''', (new_node_id, new_shard_path, key, shard_index))
                
                # Update the shard_locations JSON in objects table
                metadata = await self.get_object_metadata(key)
                if metadata:
                    for i, location in enumerate(metadata.shard_locations):
                        if i == shard_index:
                            location['node_id'] = new_node_id
                            location['shard_path'] = new_shard_path
                            break
                    
                    cursor.execute('''
                        UPDATE objects 
                        SET shard_locations = ?
                        WHERE key = ?
                    ''', (json.dumps(metadata.shard_locations), key))
                
                conn.commit()
                conn.close()
                logger.info(f"Updated shard location for {key}, shard {shard_index}")
                return True
                
            except Exception as e:
                logger.error(f"Failed to update shard location: {e}")
                return False
