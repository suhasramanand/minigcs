"""
StorageNode: Handles storage operations for individual shards
"""
import asyncio
import aiofiles
import os
import logging
from typing import Optional, Dict, List
from datetime import datetime
import hashlib
import json

logger = logging.getLogger(__name__)

class StorageNode:
    """Storage node that can store and retrieve shards"""
    
    def __init__(self, node_id: str, region: str, storage_path: str):
        self.node_id = node_id
        self.region = region
        self.storage_path = storage_path
        self.lock = asyncio.Lock()
        
        os.makedirs(storage_path, exist_ok=True)
        self._init_node_metadata()
    
    def _init_node_metadata(self):
        """Initialize node metadata file"""
        metadata_file = os.path.join(self.storage_path, "node_metadata.json")
        if not os.path.exists(metadata_file):
            metadata = {
                "node_id": self.node_id,
                "region": self.region,
                "created_at": datetime.now().isoformat(),
                "storage_path": self.storage_path,
                "total_shards": 0,
                "total_size": 0
            }
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
    
    async def store_shard(self, shard_path: str, data: bytes, object_key: str) -> bool:
        """
        Store a shard at the specified path
        Returns True if successful, False otherwise
        """
        async with self.lock:
            try:
                full_path = os.path.join(self.storage_path, shard_path)
                
                # Ensure directory exists
                os.makedirs(os.path.dirname(full_path), exist_ok=True)
                
                # Write shard data
                async with aiofiles.open(full_path, 'wb') as f:
                    await f.write(data)
                
                # Update node metadata
                await self._update_metadata(len(data), 1)
                
                logger.info(f"Stored shard {shard_path} ({len(data)} bytes) on node {self.node_id}")
                return True
                
            except Exception as e:
                logger.error(f"Failed to store shard {shard_path} on node {self.node_id}: {e}")
                return False
    
    async def retrieve_shard(self, shard_path: str) -> Optional[bytes]:
        """
        Retrieve a shard from the specified path
        Returns shard data or None if not found
        """
        try:
            full_path = os.path.join(self.storage_path, shard_path)
            
            if not os.path.exists(full_path):
                logger.warning(f"Shard {shard_path} not found on node {self.node_id}")
                return None
            
            async with aiofiles.open(full_path, 'rb') as f:
                data = await f.read()
            
            logger.debug(f"Retrieved shard {shard_path} ({len(data)} bytes) from node {self.node_id}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to retrieve shard {shard_path} from node {self.node_id}: {e}")
            return None
    
    async def delete_shard(self, shard_path: str) -> bool:
        """
        Delete a shard from the specified path
        Returns True if successful, False otherwise
        """
        async with self.lock:
            try:
                full_path = os.path.join(self.storage_path, shard_path)
                
                if not os.path.exists(full_path):
                    logger.warning(f"Shard {shard_path} not found on node {self.node_id}")
                    return True  # Consider it successful if already deleted
                
                # Get file size before deletion
                file_size = os.path.getsize(full_path)
                
                # Delete the file
                os.remove(full_path)
                
                # Update node metadata
                await self._update_metadata(-file_size, -1)
                
                logger.info(f"Deleted shard {shard_path} from node {self.node_id}")
                return True
                
            except Exception as e:
                logger.error(f"Failed to delete shard {shard_path} from node {self.node_id}: {e}")
                return False
    
    async def list_shards(self, prefix: str = "") -> List[str]:
        """
        List all shards on this node, optionally filtered by prefix
        Returns list of relative shard paths
        """
        try:
            shards = []
            for root, dirs, files in os.walk(self.storage_path):
                for file in files:
                    if file == "node_metadata.json":
                        continue  # Skip metadata file
                    
                    rel_path = os.path.relpath(os.path.join(root, file), self.storage_path)
                    
                    if prefix and not rel_path.startswith(prefix):
                        continue
                    
                    shards.append(rel_path)
            
            return shards
            
        except Exception as e:
            logger.error(f"Failed to list shards on node {self.node_id}: {e}")
            return []
    
    async def get_shard_info(self, shard_path: str) -> Optional[Dict]:
        """
        Get information about a shard (size, checksum, etc.)
        """
        try:
            full_path = os.path.join(self.storage_path, shard_path)
            
            if not os.path.exists(full_path):
                return None
            
            stat = os.stat(full_path)
            async with aiofiles.open(full_path, 'rb') as f:
                data = await f.read()
                checksum = hashlib.sha256(data).hexdigest()
            
            return {
                "path": shard_path,
                "size": stat.st_size,
                "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "checksum": checksum
            }
            
        except Exception as e:
            logger.error(f"Failed to get shard info for {shard_path}: {e}")
            return None
    
    async def _update_metadata(self, size_delta: int, shard_delta: int):
        """Update node metadata file"""
        try:
            metadata_file = os.path.join(self.storage_path, "node_metadata.json")
            
            async with aiofiles.open(metadata_file, 'r') as f:
                metadata = json.loads(await f.read())
            
            metadata["total_shards"] += shard_delta
            metadata["total_size"] += size_delta
            metadata["updated_at"] = datetime.now().isoformat()
            
            async with aiofiles.open(metadata_file, 'w') as f:
                await f.write(json.dumps(metadata, indent=2))
                
        except Exception as e:
            logger.error(f"Failed to update node metadata: {e}")
    
    async def get_node_stats(self) -> Dict:
        """Get node statistics"""
        try:
            metadata_file = os.path.join(self.storage_path, "node_metadata.json")
            
            if os.path.exists(metadata_file):
                async with aiofiles.open(metadata_file, 'r') as f:
                    metadata = json.loads(await f.read())
                return metadata
            else:
                return {
                    "node_id": self.node_id,
                    "region": self.region,
                    "total_shards": 0,
                    "total_size": 0
                }
                
        except Exception as e:
            logger.error(f"Failed to get node stats: {e}")
            return {
                "node_id": self.node_id,
                "region": self.region,
                "total_shards": 0,
                "total_size": 0,
                "error": str(e)
            }
    
    async def health_check(self) -> bool:
        """
        Perform a health check on the storage node
        Returns True if healthy, False otherwise
        """
        try:
            # Check if storage directory is accessible
            if not os.path.exists(self.storage_path):
                return False
            
            # Try to write and read a test file
            test_file = os.path.join(self.storage_path, ".health_check")
            test_data = b"health_check_test"
            
            async with aiofiles.open(test_file, 'wb') as f:
                await f.write(test_data)
            
            async with aiofiles.open(test_file, 'rb') as f:
                read_data = await f.read()
            
            # Clean up test file
            os.remove(test_file)
            
            return read_data == test_data
            
        except Exception as e:
            logger.error(f"Health check failed for node {self.node_id}: {e}")
            return False
    
    async def cleanup(self):
        """Clean up resources when shutting down the node"""
        logger.info(f"Cleaning up storage node {self.node_id}")
        # Any cleanup operations can be added here
