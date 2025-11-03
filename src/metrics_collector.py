"""
Metrics Collector for Query Performance Evaluation
Implements Artefacts A, B, C, D from assignment requirements

Usage:
    collector = MetricsCollector()
    
    # Start collection
    collector.start_query()
    results = indexer.query("machine learning")
    metrics = collector.end_query(results)
    
    # Get aggregated stats
    stats = collector.get_statistics()
"""
import time
import psutil
import tracemalloc
from typing import List, Dict, Any
from collections import defaultdict
import statistics


class MetricsCollector:
    """
    Collects performance metrics during query execution
    
    Artefacts:
    - A: System Response Time (latency, p95, p99)
    - B: System Throughput (QPS)
    - C: Memory Footprint (RAM usage)
    - D: Functional Metrics (precision, recall, MAP, nDCG)
    """
    
    def __init__(self):
        self.query_times = []  # List of query execution times
        self.memory_snapshots = []  # Memory usage snapshots
        self.query_results = []  # Store results for functional metrics
        
        # Current query tracking
        self.current_start_time = None
        self.current_memory_start = None
        
        # Process tracking
        self.process = psutil.Process()
        
    def start_query(self):
        """Start timing and memory tracking for a query"""
        # Start timing
        self.current_start_time = time.perf_counter()
        
        # Start memory tracking
        tracemalloc.start()
        self.current_memory_start = self.process.memory_info().rss / (1024 * 1024)  # MB
        
    def end_query(self, results: List[str]) -> Dict[str, Any]:
        """
        End query tracking and record metrics
        
        Args:
            results: List of document IDs returned by query
            
        Returns:
            Dictionary with query metrics
        """
        # End timing
        end_time = time.perf_counter()
        query_time = (end_time - self.current_start_time) * 1000  # Convert to ms
        self.query_times.append(query_time)
        
        # End memory tracking
        current_memory, peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        memory_end = self.process.memory_info().rss / (1024 * 1024)  # MB
        memory_used = memory_end - self.current_memory_start
        
        self.memory_snapshots.append({
            'start_mb': self.current_memory_start,
            'end_mb': memory_end,
            'delta_mb': memory_used,
            'peak_mb': peak_memory / (1024 * 1024)
        })
        
        # Store results
        self.query_results.append({
            'results': results,
            'count': len(results),
            'time_ms': query_time,
            'memory_mb': memory_used
        })
        
        return {
            'latency_ms': query_time,
            'memory_mb': memory_used,
            'result_count': len(results)
        }
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get aggregated statistics across all queries
        
        Returns:
            Dictionary with Artefacts A, B, C
        """
        if not self.query_times:
            return {}
        
        sorted_times = sorted(self.query_times)
        
        # Artefact A: System Response Time
        latency_stats = {
            'mean_ms': statistics.mean(self.query_times),
            'median_ms': statistics.median(self.query_times),
            'p95_ms': sorted_times[int(len(sorted_times) * 0.95)],
            'p99_ms': sorted_times[int(len(sorted_times) * 0.99)],
            'min_ms': min(self.query_times),
            'max_ms': max(self.query_times)
        }
        
        # Artefact B: System Throughput
        total_time_sec = sum(self.query_times) / 1000  # Convert ms to seconds
        throughput_stats = {
            'total_queries': len(self.query_times),
            'total_time_sec': total_time_sec,
            'qps': len(self.query_times) / total_time_sec if total_time_sec > 0 else 0
        }
        
        # Artefact C: Memory Footprint
        memory_deltas = [m['delta_mb'] for m in self.memory_snapshots]
        memory_peaks = [m['peak_mb'] for m in self.memory_snapshots]
        
        memory_stats = {
            'mean_delta_mb': statistics.mean(memory_deltas),
            'max_delta_mb': max(memory_deltas),
            'mean_peak_mb': statistics.mean(memory_peaks),
            'max_peak_mb': max(memory_peaks)
        }
        
        return {
            'artefact_a_latency': latency_stats,
            'artefact_b_throughput': throughput_stats,
            'artefact_c_memory': memory_stats,
            'query_count': len(self.query_times)
        }
    
    def reset(self):
        """Reset all collected metrics"""
        self.query_times.clear()
        self.memory_snapshots.clear()
        self.query_results.clear()
    
    def compute_precision_recall(self, results: List[str], ground_truth: List[str]) -> Dict[str, float]:
        """
        Artefact D: Compute precision and recall
        
        Args:
            results: Retrieved document IDs
            ground_truth: Relevant document IDs
            
        Returns:
            Dictionary with precision, recall, F1
        """
        results_set = set(results)
        truth_set = set(ground_truth)
        
        true_positives = len(results_set & truth_set)
        
        precision = true_positives / len(results_set) if results_set else 0.0
        recall = true_positives / len(truth_set) if truth_set else 0.0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
        
        return {
            'precision': precision,
            'recall': recall,
            'f1': f1,
            'true_positives': true_positives,
            'retrieved': len(results_set),
            'relevant': len(truth_set)
        }
    
    def export_to_json(self, filepath: str):
        """Export collected metrics to JSON file"""
        import json
        
        data = {
            'statistics': self.get_statistics(),
            'raw_query_times': self.query_times,
            'raw_memory_snapshots': self.memory_snapshots,
            'query_results': self.query_results
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"Metrics exported to {filepath}")
