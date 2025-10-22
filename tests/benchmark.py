"""
Benchmark tests for MiniGCS
"""
import asyncio
import logging
import random
import string
import time
import yaml
import sys
import os
from typing import List, Dict
import json

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.metadata import MetadataService
from core.replication_manager import ReplicationManager
from core.metrics import MetricsCollector, MetricsContext, RecoveryMetrics

logger = logging.getLogger(__name__)

class MiniGCSBenchmark:
    """Benchmark class for testing performance"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
        self.metadata_service = MetadataService()
        self.replication_manager = ReplicationManager(self.config, self.metadata_service)
        self.metrics_collector = MetricsCollector(self.config['metrics']['output_file'])
        
        # Test data
        self.test_objects: List[Dict] = []
        self.failed_node_id: str = None
        
    def _load_config(self) -> dict:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _generate_random_data(self, size_bytes: int) -> bytes:
        """Generate random data of specified size"""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=size_bytes)).encode('utf-8')
    
    def _generate_object_key(self, index: int) -> str:
        """Generate a unique object key"""
        return f"test_object_{index:06d}"
    
    async def upload_objects(self, count: int, size_range: tuple = (1024, 10240)) -> List[Dict]:
        """
        Upload multiple objects for testing
        Returns list of uploaded object info
        """
        logger.info(f"Starting upload of {count} objects...")
        
        objects = []
        start_time = time.time()
        
        for i in range(count):
            # Generate random size within range
            size = random.randint(int(size_range[0]), int(size_range[1]))
            data = self._generate_random_data(int(size))
            object_key = self._generate_object_key(i)
            
            try:
                # Upload with metrics
                async with MetricsContext(
                    self.metrics_collector,
                    "upload",
                    object_key,
                    int(size)  # Ensure size is an integer
                ):
                    success = await self.replication_manager.store_object(
                        object_key, data, "application/octet-stream"
                    )
                
                if success:
                    objects.append({
                        "key": object_key,
                        "size": size,
                        "data": data
                    })
                    
                    if (i + 1) % 100 == 0:
                        logger.info(f"Uploaded {i + 1}/{count} objects")
                else:
                    logger.error(f"Failed to upload object {object_key}")
                    
            except Exception as e:
                logger.error(f"Error uploading object {object_key}: {e}")
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"Upload completed: {len(objects)}/{count} objects in {duration:.2f}s")
        logger.info(f"Upload throughput: {len(objects)/duration:.2f} objects/sec")
        
        self.test_objects = objects
        return objects
    
    async def download_objects(self, objects: List[Dict]) -> Dict:
        """
        Download objects and verify integrity
        Returns download statistics
        """
        logger.info(f"Starting download of {len(objects)} objects...")
        
        successful_downloads = 0
        failed_downloads = 0
        total_size = 0
        start_time = time.time()
        
        for obj in objects:
            try:
                # Download with metrics
                async with MetricsContext(
                    self.metrics_collector,
                    "download",
                    obj["key"],
                    int(obj["size"])  # Ensure size is an integer
                ):
                    downloaded_data = await self.replication_manager.retrieve_object(obj["key"])
                
                if downloaded_data is not None:
                    # Verify data integrity
                    if downloaded_data == obj["data"]:
                        successful_downloads += 1
                        total_size += obj["size"]
                    else:
                        logger.error(f"Data integrity check failed for {obj['key']}")
                        failed_downloads += 1
                else:
                    logger.error(f"Failed to download object {obj['key']}")
                    failed_downloads += 1
                    
            except Exception as e:
                logger.error(f"Error downloading object {obj['key']}: {e}")
                failed_downloads += 1
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"Download completed: {successful_downloads}/{len(objects)} objects in {duration:.2f}s")
        logger.info(f"Download throughput: {successful_downloads/duration:.2f} objects/sec")
        logger.info(f"Data throughput: {total_size/(1024*1024)/duration:.2f} MB/s")
        
        return {
            "successful": successful_downloads,
            "failed": failed_downloads,
            "total_size": total_size,
            "duration": duration,
            "throughput_objects_per_sec": successful_downloads/duration if duration > 0 else 0,
            "throughput_mb_per_sec": total_size/(1024*1024)/duration if duration > 0 else 0
        }
    
    async def simulate_node_failure(self, node_id: str) -> bool:
        """
        Simulate a node failure by stopping the node
        In a real scenario, this would involve stopping the actual node process
        For this simulation, we'll just mark the node as failed
        """
        logger.info(f"Simulating failure of node: {node_id}")
        
        # In a real implementation, you would:
        # 1. Stop the node process
        # 2. Block network access to the node
        # 3. Or simulate other failure scenarios
        
        # For this simulation, we'll just record the failed node
        self.failed_node_id = node_id
        
        # Simulate some delay for the failure to be detected
        await asyncio.sleep(2)
        
        logger.info(f"Node {node_id} marked as failed")
        return True
    
    async def test_recovery(self) -> Dict:
        """
        Test recovery from node failure
        Returns recovery statistics
        """
        if not self.failed_node_id:
            logger.error("No failed node to recover from")
            return {}
        
        logger.info(f"Starting recovery from node failure: {self.failed_node_id}")
        
        start_time = time.time()
        
        try:
            # Trigger recovery
            success = await self.replication_manager.recover_from_node_failure(self.failed_node_id)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Record recovery metrics
            recovery_metrics = RecoveryMetrics(
                failed_node_id=self.failed_node_id,
                start_time=start_time,
                end_time=end_time,
                affected_objects=len(self.test_objects),  # All objects are potentially affected
                recovered_objects=len(self.test_objects) if success else 0,
                success=success
            )
            
            await self.metrics_collector.record_recovery(recovery_metrics)
            
            if success:
                logger.info(f"Recovery completed successfully in {duration:.2f}s")
                logger.info(f"MTTR (Mean Time To Recovery): {duration:.2f}s")
            else:
                logger.error(f"Recovery failed after {duration:.2f}s")
            
            return {
                "success": success,
                "duration": duration,
                "mttr_seconds": duration,
                "affected_objects": len(self.test_objects),
                "recovered_objects": len(self.test_objects) if success else 0
            }
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            logger.error(f"Recovery failed with error: {e}")
            
            # Record failed recovery metrics
            recovery_metrics = RecoveryMetrics(
                failed_node_id=self.failed_node_id,
                start_time=start_time,
                end_time=end_time,
                affected_objects=len(self.test_objects),
                recovered_objects=0,
                success=False,
                error_message=str(e)
            )
            
            await self.metrics_collector.record_recovery(recovery_metrics)
            
            return {
                "success": False,
                "duration": duration,
                "mttr_seconds": duration,
                "affected_objects": len(self.test_objects),
                "recovered_objects": 0,
                "error": str(e)
            }
    
    async def run_full_benchmark(self, object_count: int = 1000, size_range: tuple = (1024, 10240)) -> Dict:
        """
        Run the complete benchmark test suite
        """
        logger.info("Starting MiniGCS benchmark test suite...")
        
        benchmark_results = {
            "start_time": time.time(),
            "object_count": object_count,
            "size_range": size_range,
            "config": self.config
        }
        
        try:
            # Phase 1: Upload objects
            logger.info("=== Phase 1: Object Upload ===")
            upload_start = time.time()
            objects = await self.upload_objects(object_count, size_range)
            upload_end = time.time()
            
            benchmark_results["upload"] = {
                "objects_uploaded": len(objects),
                "duration": upload_end - upload_start,
                "success_rate": len(objects) / object_count * 100
            }
            
            # Phase 2: Download and verify objects
            logger.info("=== Phase 2: Object Download & Verification ===")
            download_results = await self.download_objects(objects)
            benchmark_results["download"] = download_results
            
            # Phase 3: Simulate node failure
            logger.info("=== Phase 3: Node Failure Simulation ===")
            # Select a random node to fail
            available_nodes = [node["id"] for node in self.config["nodes"]]
            failed_node = random.choice(available_nodes)
            
            failure_success = await self.simulate_node_failure(failed_node)
            benchmark_results["node_failure"] = {
                "failed_node": failed_node,
                "simulation_success": failure_success
            }
            
            # Phase 4: Test recovery
            logger.info("=== Phase 4: Recovery Testing ===")
            recovery_results = await self.test_recovery()
            benchmark_results["recovery"] = recovery_results
            
            # Phase 5: Post-recovery verification
            logger.info("=== Phase 5: Post-Recovery Verification ===")
            post_recovery_results = await self.download_objects(objects)
            benchmark_results["post_recovery"] = post_recovery_results
            
            # Get overall metrics summary
            benchmark_results["metrics_summary"] = self.metrics_collector.get_overall_summary()
            
            benchmark_results["end_time"] = time.time()
            benchmark_results["total_duration"] = benchmark_results["end_time"] - benchmark_results["start_time"]
            
            logger.info("=== Benchmark Test Suite Completed ===")
            self._print_summary(benchmark_results)
            
            return benchmark_results
            
        except Exception as e:
            logger.error(f"Benchmark test suite failed: {e}")
            benchmark_results["error"] = str(e)
            benchmark_results["end_time"] = time.time()
            return benchmark_results
    
    def _print_summary(self, results: Dict):
        """Print benchmark summary"""
        print("\n" + "="*60)
        print("MiniGCS Benchmark Results Summary")
        print("="*60)
        
        print(f"Total Duration: {results['total_duration']:.2f} seconds")
        print(f"Objects Tested: {results['object_count']}")
        
        # Upload summary
        upload = results.get("upload", {})
        print(f"\nUpload Results:")
        print(f"  Objects Uploaded: {upload.get('objects_uploaded', 0)}/{results['object_count']}")
        print(f"  Success Rate: {upload.get('success_rate', 0):.1f}%")
        print(f"  Duration: {upload.get('duration', 0):.2f}s")
        
        # Download summary
        download = results.get("download", {})
        print(f"\nDownload Results:")
        print(f"  Successful Downloads: {download.get('successful', 0)}")
        print(f"  Failed Downloads: {download.get('failed', 0)}")
        print(f"  Throughput: {download.get('throughput_mb_per_sec', 0):.2f} MB/s")
        
        # Recovery summary
        recovery = results.get("recovery", {})
        print(f"\nRecovery Results:")
        print(f"  Failed Node: {results.get('node_failure', {}).get('failed_node', 'N/A')}")
        print(f"  Recovery Success: {recovery.get('success', False)}")
        print(f"  MTTR: {recovery.get('mttr_seconds', 0):.2f}s")
        print(f"  Affected Objects: {recovery.get('affected_objects', 0)}")
        print(f"  Recovered Objects: {recovery.get('recovered_objects', 0)}")
        
        # Metrics summary
        metrics = results.get("metrics_summary", {})
        print(f"\nOverall Metrics:")
        print(f"  Total Operations: {metrics.get('total_operations', 0)}")
        print(f"  Success Rate: {metrics.get('overall_success_rate', 0):.1f}%")
        print(f"  Overall Throughput: {metrics.get('overall_throughput_mbps', 0):.2f} MB/s")
        print(f"  Total Data Processed: {metrics.get('total_data_processed_mb', 0):.2f} MB")
        
        print("="*60)
    
    async def cleanup(self):
        """Clean up test data"""
        logger.info("Cleaning up test objects...")
        
        cleanup_count = 0
        for obj in self.test_objects:
            try:
                success = await self.replication_manager.delete_object(obj["key"])
                if success:
                    cleanup_count += 1
            except Exception as e:
                logger.error(f"Failed to delete object {obj['key']}: {e}")
        
        logger.info(f"Cleaned up {cleanup_count}/{len(self.test_objects)} test objects")

async def main():
    """Main benchmark execution"""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create benchmark instance
    benchmark = MiniGCSBenchmark()
    
    try:
        # Run benchmark
        results = await benchmark.run_full_benchmark(
            object_count=1000,
            size_range=(1024, 10240)  # 1KB to 10KB objects
        )
        
        # Save results to file
        results_file = f"benchmark_results_{int(time.time())}.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\nBenchmark results saved to: {results_file}")
        
        # Export metrics
        metrics_file = await benchmark.metrics_collector.export_metrics()
        print(f"Metrics exported to: {metrics_file}")
        
        # Cleanup
        await benchmark.cleanup()
        
    except KeyboardInterrupt:
        logger.info("Benchmark interrupted by user")
    except Exception as e:
        logger.error(f"Benchmark failed: {e}")
    finally:
        # Cleanup
        await benchmark.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
