"""
ReplicationManager: Handles replication and recovery operations
"""
import asyncio
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import random
import yaml

from .metadata import MetadataService, ObjectMetadata
from .storage_node import StorageNode
from .erasure_coding import ErasureCoding

logger = logging.getLogger(__name__)

class ReplicationManager:
    """Manages replication and recovery operations"""
    
    def __init__(self, config: Dict, metadata_service: MetadataService):
        self.config = config
        self.metadata_service = metadata_service
        self.nodes: Dict[str, StorageNode] = {}
        self.erasure_coding = ErasureCoding(
            config['erasure_coding']['k'],
            config['erasure_coding']['m']
        )
        
        self._init_nodes()
    
    def _init_nodes(self):
        """Initialize storage nodes from configuration"""
        for node_config in self.config['nodes']:
            node = StorageNode(
                node_id=node_config['id'],
                region=node_config['region'],
                storage_path=node_config['storage_path']
            )
            self.nodes[node_config['id']] = node
            logger.info(f"Initialized storage node: {node_config['id']} in {node_config['region']}")
    
    async def store_object(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> bool:
        """
        Store an object with erasure coding and replication
        """
        try:
            # Encode data into shards
            shards = await self.erasure_coding.encode(data)
            
            # Generate checksum
            checksum = self.erasure_coding.calculate_checksum(data)
            
            # Determine shard placement
            shard_locations = await self._determine_shard_placement(key, len(shards))
            
            # Store shards on nodes
            storage_success = await self._store_shards(shards, shard_locations)
            
            if not storage_success:
                logger.error(f"Failed to store all shards for object: {key}")
                return False
            
            # Create and store metadata
            metadata = ObjectMetadata(
                key=key,
                size=len(data),
                content_type=content_type,
                created_at=datetime.now(),
                updated_at=datetime.now(),
                version=1,
                checksum=checksum,
                shard_locations=shard_locations,
                erasure_params={"k": self.config['erasure_coding']['k'], "m": self.config['erasure_coding']['m']},
                regions=list(set(loc['region'] for loc in shard_locations))
            )
            
            success = await self.metadata_service.store_object_metadata(metadata)
            
            if success:
                logger.info(f"Successfully stored object: {key} ({len(data)} bytes)")
            else:
                logger.error(f"Failed to store metadata for object: {key}")
                # Clean up stored shards
                await self._cleanup_failed_upload(shard_locations)
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to store object {key}: {e}")
            return False
    
    async def retrieve_object(self, key: str) -> Optional[bytes]:
        """
        Retrieve an object by key
        """
        try:
            # Get metadata
            metadata = await self.metadata_service.get_object_metadata(key)
            if not metadata:
                logger.warning(f"Object not found: {key}")
                return None
            
            # Retrieve shards
            shards = await self._retrieve_shards(metadata.shard_locations)
            
            if not shards:
                logger.error(f"Failed to retrieve any shards for object: {key}")
                return None
            
            # Decode data from shards
            available_shards = [shard if shard is not None else None for shard in shards]
            shard_size = len(next(shard for shard in shards if shard is not None))
            
            try:
                data = await self.erasure_coding.decode(available_shards, shard_size)
                
                # Verify checksum
                calculated_checksum = self.erasure_coding.calculate_checksum(data)
                if calculated_checksum != metadata.checksum:
                    logger.warning(f"Checksum mismatch for object {key}")
                    # Still return data, but log warning
                
                logger.info(f"Successfully retrieved object: {key} ({len(data)} bytes)")
                return data
                
            except Exception as e:
                logger.error(f"Failed to decode shards for object {key}: {e}")
                return None
            
        except Exception as e:
            logger.error(f"Failed to retrieve object {key}: {e}")
            return None
    
    async def delete_object(self, key: str) -> bool:
        """
        Delete an object and all its shards
        """
        try:
            # Get metadata
            metadata = await self.metadata_service.get_object_metadata(key)
            if not metadata:
                logger.warning(f"Object not found for deletion: {key}")
                return True  # Consider it successful if already deleted
            
            # Delete shards from nodes
            delete_success = await self._delete_shards(metadata.shard_locations)
            
            # Delete metadata
            metadata_success = await self.metadata_service.delete_object_metadata(key)
            
            success = delete_success and metadata_success
            
            if success:
                logger.info(f"Successfully deleted object: {key}")
            else:
                logger.error(f"Failed to delete object: {key}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to delete object {key}: {e}")
            return False
    
    async def list_objects(self, prefix: str = "") -> List[str]:
        """
        List all objects, optionally filtered by prefix
        """
        return await self.metadata_service.list_objects(prefix)
    
    async def _determine_shard_placement(self, key: str, num_shards: int) -> List[Dict[str, str]]:
        """
        Determine which nodes should store each shard with improved fault tolerance
        """
        shard_locations = []
        
        # Get all available nodes
        available_nodes = list(self.nodes.keys())
        
        if len(available_nodes) < num_shards:
            logger.warning(f"Not enough nodes for shard distribution: {len(available_nodes)} < {num_shards}")
        
        # Distribute shards to maximize fault tolerance
        # Ensure each shard goes to a different node when possible
        for i in range(num_shards):
            # Use consistent hashing but with better distribution
            hash_input = f"{key}:{i}:{hash(key) % 1000}"
            
            # Select node with round-robin for better distribution
            node_index = (hash(hash_input) + i) % len(available_nodes)
            node_id = available_nodes[node_index]
            node = self.nodes[node_id]
            
            # Generate shard path
            shard_path = f"objects/{key}/shard_{i:03d}"
            
            shard_locations.append({
                "node_id": node_id,
                "region": node.region,
                "shard_path": shard_path
            })
        
        return shard_locations
    
    async def _store_shards(self, shards: List[bytes], shard_locations: List[Dict[str, str]]) -> bool:
        """
        Store shards on their designated nodes
        """
        tasks = []
        for i, (shard, location) in enumerate(zip(shards, shard_locations)):
            node_id = location['node_id']
            shard_path = location['shard_path']
            
            if node_id in self.nodes:
                task = self.nodes[node_id].store_shard(shard_path, shard, f"shard_{i}")
                tasks.append(task)
            else:
                logger.error(f"Node {node_id} not found")
                return False
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check if all storage operations succeeded
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to store shard {i}: {result}")
                return False
            elif not result:
                logger.error(f"Failed to store shard {i}")
                return False
        
        return True
    
    async def _retrieve_shards(self, shard_locations: List[Dict[str, str]]) -> List[Optional[bytes]]:
        """
        Retrieve shards from their designated nodes
        """
        tasks = []
        for location in shard_locations:
            node_id = location['node_id']
            shard_path = location['shard_path']
            
            if node_id in self.nodes:
                task = self.nodes[node_id].retrieve_shard(shard_path)
                tasks.append(task)
            else:
                logger.error(f"Node {node_id} not found")
                tasks.append(asyncio.create_task(asyncio.sleep(0)))  # Placeholder
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Convert exceptions to None
        shards = []
        for result in results:
            if isinstance(result, Exception):
                logger.warning(f"Failed to retrieve shard: {result}")
                shards.append(None)
            else:
                shards.append(result)
        
        return shards
    
    async def _delete_shards(self, shard_locations: List[Dict[str, str]]) -> bool:
        """
        Delete shards from their designated nodes
        """
        tasks = []
        for location in shard_locations:
            node_id = location['node_id']
            shard_path = location['shard_path']
            
            if node_id in self.nodes:
                task = self.nodes[node_id].delete_shard(shard_path)
                tasks.append(task)
            else:
                logger.warning(f"Node {node_id} not found")
                tasks.append(asyncio.create_task(asyncio.sleep(0)))  # Placeholder
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check if all deletion operations succeeded
        success = True
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to delete shard {i}: {result}")
                success = False
            elif not result:
                logger.error(f"Failed to delete shard {i}")
                success = False
        
        return success
    
    async def _cleanup_failed_upload(self, shard_locations: List[Dict[str, str]]):
        """
        Clean up shards after a failed upload
        """
        await self._delete_shards(shard_locations)
    
    async def detect_node_failures(self) -> List[str]:
        """
        Detect failed nodes by performing health checks
        """
        failed_nodes = []
        
        health_check_tasks = []
        node_ids = list(self.nodes.keys())
        
        for node_id in node_ids:
            task = self.nodes[node_id].health_check()
            health_check_tasks.append((node_id, task))
        
        results = await asyncio.gather(*[task for _, task in health_check_tasks], return_exceptions=True)
        
        for i, result in enumerate(results):
            node_id = node_ids[i]
            if isinstance(result, Exception) or not result:
                failed_nodes.append(node_id)
                logger.warning(f"Node {node_id} failed health check")
        
        return failed_nodes
    
    async def recover_from_node_failure(self, failed_node_id: str) -> bool:
        """
        Recover from a node failure by repairing/replicating affected shards
        """
        try:
            logger.info(f"Starting recovery for failed node: {failed_node_id}")
            
            # Find all objects affected by this node failure
            affected_objects = await self._find_affected_objects(failed_node_id)
            
            if not affected_objects:
                logger.info(f"No objects affected by node failure: {failed_node_id}")
                return True
            
            logger.info(f"Found {len(affected_objects)} objects affected by node failure")
            
            # Recover each affected object with improved error handling
            recovery_success = True
            recovered_count = 0
            
            for object_key in affected_objects:
                try:
                    success = await self._recover_object(object_key, failed_node_id)
                    if success:
                        recovered_count += 1
                        logger.info(f"Successfully recovered object: {object_key}")
                    else:
                        logger.error(f"Failed to recover object: {object_key}")
                        recovery_success = False
                except Exception as e:
                    logger.error(f"Error recovering object {object_key}: {e}")
                    recovery_success = False
            
            if recovery_success:
                logger.info(f"Successfully recovered from node failure: {failed_node_id} ({recovered_count}/{len(affected_objects)} objects)")
            else:
                logger.error(f"Partial recovery from node failure: {failed_node_id} ({recovered_count}/{len(affected_objects)} objects)")
            
            return recovery_success
            
        except Exception as e:
            logger.error(f"Failed to recover from node failure {failed_node_id}: {e}")
            return False
    
    async def _find_affected_objects(self, failed_node_id: str) -> List[str]:
        """
        Find all objects that have shards on the failed node
        """
        affected_objects = []
        
        # Get all objects
        all_objects = await self.metadata_service.list_objects()
        
        for object_key in all_objects:
            metadata = await self.metadata_service.get_object_metadata(object_key)
            if metadata:
                # Check if any shard is on the failed node
                for location in metadata.shard_locations:
                    if location['node_id'] == failed_node_id:
                        affected_objects.append(object_key)
                        break
        
        return affected_objects
    
    async def _recover_object(self, object_key: str, failed_node_id: str) -> bool:
        """
        Recover a specific object by repairing missing shards
        """
        try:
            # Get object metadata
            metadata = await self.metadata_service.get_object_metadata(object_key)
            if not metadata:
                logger.error(f"Metadata not found for object: {object_key}")
                return False
            
            # Find which shards are on the failed node
            failed_shard_indices = []
            for i, location in enumerate(metadata.shard_locations):
                if location['node_id'] == failed_node_id:
                    failed_shard_indices.append(i)
            
            if not failed_shard_indices:
                logger.warning(f"No shards found on failed node for object: {object_key}")
                return True
            
            # Retrieve available shards
            available_shards = await self._retrieve_shards(metadata.shard_locations)
            
            # Check if we have enough shards to recover
            available_count = sum(1 for shard in available_shards if shard is not None)
            required_count = metadata.erasure_params['k']
            
            if available_count < required_count:
                logger.error(f"Not enough shards available for recovery: {available_count} < {required_count}")
                # Try to recover from other nodes that might have the shards
                logger.info(f"Attempting to recover missing shards for object {object_key}")
                recovery_attempted = await self._attempt_shard_recovery(object_key, failed_shard_indices)
                if not recovery_attempted:
                    return False
                
                # Re-retrieve shards after recovery attempt
                available_shards = await self._retrieve_shards(metadata.shard_locations)
                available_count = sum(1 for shard in available_shards if shard is not None)
                
                if available_count < required_count:
                    logger.error(f"Still not enough shards after recovery attempt: {available_count} < {required_count}")
                    return False
            
            # Repair missing shards with improved error handling
            repair_success = True
            for shard_index in failed_shard_indices:
                try:
                    logger.info(f"Repairing shard {shard_index} for object {object_key}")
                    repaired_shard = await self.erasure_coding.repair_shard(available_shards, shard_index)
                    
                    # Find a new node to store the repaired shard
                    new_location = await self._find_new_shard_location(object_key, shard_index, failed_node_id)
                    
                    if new_location:
                        # Store repaired shard on new node
                        node_id = new_location['node_id']
                        shard_path = new_location['shard_path']
                        
                        if node_id in self.nodes:
                            success = await self.nodes[node_id].store_shard(shard_path, repaired_shard, object_key)
                            
                            if success:
                                # Update metadata
                                await self.metadata_service.update_shard_location(
                                    object_key, shard_index, node_id, shard_path
                                )
                                logger.info(f"Successfully repaired shard {shard_index} for object {object_key}")
                                
                                # Update the available_shards list for subsequent repairs
                                available_shards[shard_index] = repaired_shard
                            else:
                                logger.error(f"Failed to store repaired shard {shard_index} for object {object_key}")
                                repair_success = False
                        else:
                            logger.error(f"Node {node_id} not found for shard recovery")
                            repair_success = False
                    else:
                        logger.error(f"Could not find new location for shard {shard_index}")
                        repair_success = False
                        
                except Exception as e:
                    logger.error(f"Failed to repair shard {shard_index} for object {object_key}: {e}")
                    repair_success = False
            
            if not repair_success:
                logger.error(f"Some shards failed to repair for object {object_key}")
                return False
            
            logger.info(f"Successfully recovered object: {object_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to recover object {object_key}: {e}")
            return False
    
    async def _find_new_shard_location(self, object_key: str, shard_index: int, failed_node_id: str) -> Optional[Dict[str, str]]:
        """
        Find a new node to store a repaired shard
        """
        # Get available nodes (excluding failed node)
        available_nodes = [node_id for node_id in self.nodes.keys() if node_id != failed_node_id]
        
        if not available_nodes:
            logger.error("No available nodes for shard recovery")
            return None
        
        # Use consistent hashing to find a new location
        hash_input = f"{object_key}:{shard_index}:recovery"
        node_index = hash(hash_input) % len(available_nodes)
        new_node_id = available_nodes[node_index]
        
        # Get node region
        new_node = self.nodes[new_node_id]
        region = new_node.region
        
        # Generate new shard path
        shard_path = f"objects/{object_key}/shard_{shard_index:03d}"
        
        return {
            "node_id": new_node_id,
            "region": region,
            "shard_path": shard_path
        }
    
    async def get_cluster_stats(self) -> Dict:
        """
        Get cluster-wide statistics
        """
        stats = {
            "total_nodes": len(self.nodes),
            "nodes_by_region": {},
            "total_objects": 0,
            "total_size": 0,
            "node_stats": {}
        }
        
        # Get node statistics
        for node_id, node in self.nodes.items():
            node_stats = await node.get_node_stats()
            stats["node_stats"][node_id] = node_stats
            
            # Aggregate by region
            region = node.region
            if region not in stats["nodes_by_region"]:
                stats["nodes_by_region"][region] = 0
            stats["nodes_by_region"][region] += 1
            
            # Aggregate totals
            stats["total_size"] += node_stats.get("total_size", 0)
        
        # Get total objects count
        all_objects = await self.metadata_service.list_objects()
        stats["total_objects"] = len(all_objects)
        
        return stats
    
    async def _attempt_shard_recovery(self, object_key: str, failed_shard_indices: List[int]) -> bool:
        """
        Attempt to recover missing shards by checking if they exist on other nodes
        """
        try:
            # Get object metadata
            metadata = await self.metadata_service.get_object_metadata(object_key)
            if not metadata:
                return False
            
            # Check if any of the "failed" shards actually exist on other nodes
            for shard_index in failed_shard_indices:
                for location in metadata.shard_locations:
                    if location['node_id'] not in self.nodes:
                        continue
                    
                    # Try to retrieve the shard from this node
                    shard_data = await self.nodes[location['node_id']].retrieve_shard(location['shard_path'])
                    if shard_data is not None:
                        logger.info(f"Found shard {shard_index} on node {location['node_id']}")
                        # Update the shard location to point to the working node
                        await self.metadata_service.update_shard_location(
                            object_key, shard_index, location['node_id'], location['shard_path']
                        )
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error in shard recovery attempt: {e}")
            return False
