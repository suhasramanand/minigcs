"""
FastAPI server for MiniGCS
"""
import asyncio
import logging
import yaml
from typing import Optional
from fastapi import FastAPI, HTTPException, UploadFile, File, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import io

# Import core modules
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.metadata import MetadataService
from core.replication_manager import ReplicationManager
from core.metrics import MetricsCollector, MetricsContext

logger = logging.getLogger(__name__)

# Pydantic models for API
class ObjectInfo(BaseModel):
    key: str
    size: int
    content_type: str
    created_at: str
    updated_at: str
    version: int
    checksum: str

class ClusterStats(BaseModel):
    total_nodes: int
    nodes_by_region: dict
    total_objects: int
    total_size: int
    node_stats: dict

class MiniGCSServer:
    """Main server class"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
        self.metadata_service = MetadataService()
        self.replication_manager = ReplicationManager(self.config, self.metadata_service)
        self.metrics_collector = MetricsCollector(self.config['metrics']['output_file'])
        
        # Create FastAPI app
        self.app = FastAPI(
            title="MiniGCS",
            description="Distributed Object Storage with Erasure Coding and Multi-Region Replication",
            version="1.0.0"
        )
        
        # Setup routes
        self._setup_routes()
    
    def _load_config(self) -> dict:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {self.config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _setup_routes(self):
        """Setup FastAPI routes"""
        
        @self.app.get("/")
        async def root():
            return {"message": "MiniGCS API", "version": "1.0.0"}
        
        @self.app.post("/objects/{object_key}")
        async def upload_object(
            object_key: str,
            file: UploadFile = File(...),
            content_type: Optional[str] = None
        ):
            """Upload an object"""
            try:
                # Read file data
                file_data = await file.read()
                
                # Determine content type
                if content_type is None:
                    content_type = file.content_type or "application/octet-stream"
                
                # Check file size limit
                max_size = self.config['api']['max_file_size']
                if len(file_data) > max_size:
                    raise HTTPException(
                        status_code=413,
                        detail=f"File size {len(file_data)} exceeds maximum size {max_size}"
                    )
                
                # Store object with metrics
                async with MetricsContext(
                    self.metrics_collector,
                    "upload",
                    object_key,
                    len(file_data)
                ):
                    success = await self.replication_manager.store_object(
                        object_key, file_data, content_type
                    )
                
                if success:
                    return {"message": f"Object {object_key} uploaded successfully", "size": len(file_data)}
                else:
                    raise HTTPException(status_code=500, detail="Failed to store object")
                    
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Failed to upload object {object_key}: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/objects/{object_key}")
        async def download_object(object_key: str):
            """Download an object"""
            try:
                async with MetricsContext(
                    self.metrics_collector,
                    "download",
                    object_key
                ):
                    data = await self.replication_manager.retrieve_object(object_key)
                
                if data is None:
                    raise HTTPException(status_code=404, detail="Object not found")
                
                # Create streaming response
                stream = io.BytesIO(data)
                
                return StreamingResponse(
                    io.BytesIO(data),
                    media_type="application/octet-stream",
                    headers={"Content-Disposition": f"attachment; filename={object_key}"}
                )
                
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Failed to download object {object_key}: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.delete("/objects/{object_key}")
        async def delete_object(object_key: str):
            """Delete an object"""
            try:
                # Get object size for metrics
                metadata = await self.metadata_service.get_object_metadata(object_key)
                size = metadata.size if metadata else 0
                
                async with MetricsContext(
                    self.metrics_collector,
                    "delete",
                    object_key,
                    size
                ):
                    success = await self.replication_manager.delete_object(object_key)
                
                if success:
                    return {"message": f"Object {object_key} deleted successfully"}
                else:
                    raise HTTPException(status_code=500, detail="Failed to delete object")
                    
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Failed to delete object {object_key}: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/objects")
        async def list_objects(prefix: Optional[str] = Query(None, description="Filter objects by prefix")):
            """List objects"""
            try:
                async with MetricsContext(
                    self.metrics_collector,
                    "list",
                    prefix or "",
                    0
                ):
                    object_keys = await self.replication_manager.list_objects(prefix)
                
                return {"objects": object_keys, "count": len(object_keys)}
                
            except Exception as e:
                logger.error(f"Failed to list objects: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/objects/{object_key}/info")
        async def get_object_info(object_key: str):
            """Get object metadata"""
            try:
                metadata = await self.metadata_service.get_object_metadata(object_key)
                
                if metadata is None:
                    raise HTTPException(status_code=404, detail="Object not found")
                
                return ObjectInfo(
                    key=metadata.key,
                    size=metadata.size,
                    content_type=metadata.content_type,
                    created_at=metadata.created_at.isoformat(),
                    updated_at=metadata.updated_at.isoformat(),
                    version=metadata.version,
                    checksum=metadata.checksum
                )
                
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Failed to get object info for {object_key}: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/cluster/stats")
        async def get_cluster_stats():
            """Get cluster statistics"""
            try:
                stats = await self.replication_manager.get_cluster_stats()
                return ClusterStats(**stats)
                
            except Exception as e:
                logger.error(f"Failed to get cluster stats: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/cluster/health")
        async def health_check():
            """Health check endpoint"""
            try:
                # Check node health
                failed_nodes = await self.replication_manager.detect_node_failures()
                
                if failed_nodes:
                    return {
                        "status": "degraded",
                        "failed_nodes": failed_nodes,
                        "message": f"Nodes {failed_nodes} are not responding"
                    }
                else:
                    return {
                        "status": "healthy",
                        "failed_nodes": [],
                        "message": "All nodes are healthy"
                    }
                    
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                return {
                    "status": "error",
                    "failed_nodes": [],
                    "message": str(e)
                }
        
        @self.app.post("/cluster/recovery/{node_id}")
        async def trigger_recovery(node_id: str):
            """Trigger recovery for a failed node"""
            try:
                success = await self.replication_manager.recover_from_node_failure(node_id)
                
                if success:
                    return {"message": f"Recovery completed for node {node_id}"}
                else:
                    raise HTTPException(status_code=500, detail=f"Recovery failed for node {node_id}")
                    
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Failed to trigger recovery for node {node_id}: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/metrics/summary")
        async def get_metrics_summary():
            """Get metrics summary"""
            try:
                summary = self.metrics_collector.get_overall_summary()
                return summary
                
            except Exception as e:
                logger.error(f"Failed to get metrics summary: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/metrics/export")
        async def export_metrics():
            """Export metrics to JSON file"""
            try:
                filename = await self.metrics_collector.export_metrics()
                return {"message": f"Metrics exported to {filename}", "filename": filename}
                
            except Exception as e:
                logger.error(f"Failed to export metrics: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/metrics/clear")
        async def clear_metrics():
            """Clear all metrics"""
            try:
                self.metrics_collector.clear_metrics()
                return {"message": "Metrics cleared successfully"}
                
            except Exception as e:
                logger.error(f"Failed to clear metrics: {e}")
                raise HTTPException(status_code=500, detail=str(e))

def create_app(config_path: str = "config.yaml") -> FastAPI:
    """Create and return FastAPI app instance"""
    server = MiniGCSServer(config_path)
    return server.app

if __name__ == "__main__":
    import uvicorn
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create app
    app = create_app()
    
    # Load config for server settings
    with open("config.yaml", 'r') as f:
        config = yaml.safe_load(f)
    
    # Run server
    uvicorn.run(
        app,
        host=config['api']['host'],
        port=config['api']['port'],
        log_level="info"
    )
