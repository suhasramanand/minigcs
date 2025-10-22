"""
Metrics collection and reporting
"""
import asyncio
import logging
import csv
import time
from typing import Dict, List, Optional
from datetime import datetime
from dataclasses import dataclass, field
import json

logger = logging.getLogger(__name__)

@dataclass
class OperationMetrics:
    """Metrics for a single operation"""
    operation_type: str
    object_key: str
    size_bytes: int
    start_time: float
    end_time: float
    success: bool
    error_message: Optional[str] = None
    node_id: Optional[str] = None
    
    @property
    def duration_ms(self) -> float:
        return (self.end_time - self.start_time) * 1000
    
    @property
    def throughput_mbps(self) -> float:
        if self.duration_ms == 0:
            return 0.0
        size_mb = self.size_bytes / (1024 * 1024)
        duration_s = self.duration_ms / 1000
        return size_mb / duration_s

@dataclass
class RecoveryMetrics:
    """Metrics for recovery operations"""
    failed_node_id: str
    start_time: float
    end_time: float
    affected_objects: int
    recovered_objects: int
    success: bool
    error_message: Optional[str] = None
    
    @property
    def duration_ms(self) -> float:
        return (self.end_time - self.start_time) * 1000
    
    @property
    def mttr_ms(self) -> float:
        return self.duration_ms

class MetricsCollector:
    """Collects and manages metrics for operations"""
    
    def __init__(self, output_file: str = "metrics.csv"):
        self.output_file = output_file
        self.operation_metrics: List[OperationMetrics] = []
        self.recovery_metrics: List[RecoveryMetrics] = []
        self.lock = asyncio.Lock()
        
        # Initialize CSV file
        self._init_csv_file()
    
    def _init_csv_file(self):
        """Initialize the CSV file with headers"""
        try:
            with open(self.output_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp', 'operation_type', 'object_key', 'size_bytes',
                    'duration_ms', 'throughput_mbps', 'success', 'error_message',
                    'node_id', 'metrics_type'
                ])
        except Exception as e:
            logger.error(f"Failed to initialize CSV file: {e}")
    
    async def record_operation(self, metrics: OperationMetrics):
        """Record operation metrics"""
        async with self.lock:
            self.operation_metrics.append(metrics)
            
            # Write to CSV file
            await self._write_operation_to_csv(metrics)
            
            logger.debug(f"Recorded operation metrics: {metrics.operation_type} for {metrics.object_key}")
    
    async def record_recovery(self, metrics: RecoveryMetrics):
        """Record recovery metrics"""
        async with self.lock:
            self.recovery_metrics.append(metrics)
            
            # Write to CSV file
            await self._write_recovery_to_csv(metrics)
            
            logger.info(f"Recorded recovery metrics: {metrics.failed_node_id}, MTTR: {metrics.mttr_ms:.2f}ms")
    
    async def _write_operation_to_csv(self, metrics: OperationMetrics):
        """Write operation metrics to CSV file"""
        try:
            with open(self.output_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    datetime.fromtimestamp(metrics.start_time).isoformat(),
                    metrics.operation_type,
                    metrics.object_key,
                    metrics.size_bytes,
                    f"{metrics.duration_ms:.2f}",
                    f"{metrics.throughput_mbps:.2f}",
                    metrics.success,
                    metrics.error_message or "",
                    metrics.node_id or "",
                    "operation"
                ])
        except Exception as e:
            logger.error(f"Failed to write operation metrics to CSV: {e}")
    
    async def _write_recovery_to_csv(self, metrics: RecoveryMetrics):
        """Write recovery metrics to CSV file"""
        try:
            with open(self.output_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    datetime.fromtimestamp(metrics.start_time).isoformat(),
                    "recovery",
                    metrics.failed_node_id,
                    metrics.affected_objects,
                    f"{metrics.duration_ms:.2f}",
                    f"{metrics.mttr_ms:.2f}",
                    metrics.success,
                    metrics.error_message or "",
                    "",
                    "recovery"
                ])
        except Exception as e:
            logger.error(f"Failed to write recovery metrics to CSV: {e}")
    
    def get_operation_summary(self) -> Dict:
        """Get summary statistics for operations"""
        if not self.operation_metrics:
            return {}
        
        # Group by operation type
        by_type = {}
        for metrics in self.operation_metrics:
            op_type = metrics.operation_type
            if op_type not in by_type:
                by_type[op_type] = []
            by_type[op_type].append(metrics)
        
        summary = {}
        for op_type, metrics_list in by_type.items():
            durations = [m.duration_ms for m in metrics_list]
            throughputs = [m.throughput_mbps for m in metrics_list if m.throughput_mbps > 0]
            success_count = sum(1 for m in metrics_list if m.success)
            
            summary[op_type] = {
                "count": len(metrics_list),
                "success_rate": success_count / len(metrics_list) * 100,
                "avg_duration_ms": sum(durations) / len(durations),
                "min_duration_ms": min(durations),
                "max_duration_ms": max(durations),
                "avg_throughput_mbps": sum(throughputs) / len(throughputs) if throughputs else 0,
                "total_size_bytes": sum(m.size_bytes for m in metrics_list)
            }
        
        return summary
    
    def get_recovery_summary(self) -> Dict:
        """Get summary statistics for recovery operations"""
        if not self.recovery_metrics:
            return {}
        
        durations = [m.duration_ms for m in self.recovery_metrics]
        mttrs = [m.mttr_ms for m in self.recovery_metrics]
        success_count = sum(1 for m in self.recovery_metrics if m.success)
        
        return {
            "count": len(self.recovery_metrics),
            "success_rate": success_count / len(self.recovery_metrics) * 100,
            "avg_mttr_ms": sum(mttrs) / len(mttrs),
            "min_mttr_ms": min(mttrs),
            "max_mttr_ms": max(mttrs),
            "total_affected_objects": sum(m.affected_objects for m in self.recovery_metrics),
            "total_recovered_objects": sum(m.recovered_objects for m in self.recovery_metrics)
        }
    
    def get_overall_summary(self) -> Dict:
        """Get overall system summary"""
        operation_summary = self.get_operation_summary()
        recovery_summary = self.get_recovery_summary()
        
        # Calculate overall statistics
        all_operations = self.operation_metrics
        all_recoveries = self.recovery_metrics
        
        total_operations = len(all_operations)
        total_recoveries = len(all_recoveries)
        total_successful_operations = sum(1 for m in all_operations if m.success)
        total_successful_recoveries = sum(1 for m in all_recoveries if m.success)
        
        # Calculate overall throughput
        total_size = sum(m.size_bytes for m in all_operations)
        total_time = sum(m.duration_ms for m in all_operations) / 1000  # Convert to seconds
        overall_throughput = (total_size / (1024 * 1024)) / total_time if total_time > 0 else 0
        
        return {
            "total_operations": total_operations,
            "total_recoveries": total_recoveries,
            "overall_success_rate": (total_successful_operations / total_operations * 100) if total_operations > 0 else 0,
            "recovery_success_rate": (total_successful_recoveries / total_recoveries * 100) if total_recoveries > 0 else 0,
            "overall_throughput_mbps": overall_throughput,
            "total_data_processed_mb": total_size / (1024 * 1024),
            "operation_summary": operation_summary,
            "recovery_summary": recovery_summary
        }
    
    async def export_metrics(self, filename: Optional[str] = None) -> str:
        """Export all metrics to a JSON file"""
        if filename is None:
            filename = f"metrics_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        export_data = {
            "export_timestamp": datetime.now().isoformat(),
            "operation_metrics": [
                {
                    "operation_type": m.operation_type,
                    "object_key": m.object_key,
                    "size_bytes": m.size_bytes,
                    "start_time": m.start_time,
                    "end_time": m.end_time,
                    "duration_ms": m.duration_ms,
                    "throughput_mbps": m.throughput_mbps,
                    "success": m.success,
                    "error_message": m.error_message,
                    "node_id": m.node_id
                }
                for m in self.operation_metrics
            ],
            "recovery_metrics": [
                {
                    "failed_node_id": m.failed_node_id,
                    "start_time": m.start_time,
                    "end_time": m.end_time,
                    "affected_objects": m.affected_objects,
                    "recovered_objects": m.recovered_objects,
                    "duration_ms": m.duration_ms,
                    "mttr_ms": m.mttr_ms,
                    "success": m.success,
                    "error_message": m.error_message
                }
                for m in self.recovery_metrics
            ],
            "summary": self.get_overall_summary()
        }
        
        try:
            with open(filename, 'w') as f:
                json.dump(export_data, f, indent=2)
            
            logger.info(f"Exported metrics to: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Failed to export metrics: {e}")
            raise
    
    def clear_metrics(self):
        """Clear all collected metrics"""
        async def _clear():
            async with self.lock:
                self.operation_metrics.clear()
                self.recovery_metrics.clear()
        
        asyncio.create_task(_clear())
        logger.info("Cleared all metrics")

class MetricsContext:
    """Context manager for measuring operation metrics"""
    
    def __init__(self, metrics_collector: MetricsCollector, operation_type: str, object_key: str, size_bytes: int = 0, node_id: Optional[str] = None):
        self.metrics_collector = metrics_collector
        self.operation_type = operation_type
        self.object_key = object_key
        self.size_bytes = int(size_bytes) if size_bytes is not None else 0  # Ensure it's an integer
        self.node_id = node_id
        self.start_time = None
        self.success = False
        self.error_message = None
    
    async def __aenter__(self):
        self.start_time = time.time()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        end_time = time.time()
        
        if exc_type is None:
            self.success = True
        else:
            self.success = False
            self.error_message = str(exc_val)
        
        metrics = OperationMetrics(
            operation_type=self.operation_type,
            object_key=self.object_key,
            size_bytes=self.size_bytes,
            start_time=self.start_time,
            end_time=end_time,
            success=self.success,
            error_message=self.error_message,
            node_id=self.node_id
        )
        
        await self.metrics_collector.record_operation(metrics)
