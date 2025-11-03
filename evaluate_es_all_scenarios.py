"""
Evaluate Elasticsearch under different cache scenarios
Generates 3 result files to show complete performance picture
"""

import os
import sys
import json
import time
import numpy as np
from elasticsearch import Elasticsearch

# Direct import to avoid src/__init__.py NLTK dependency
import importlib.util
spec = importlib.util.spec_from_file_location("es_indexer", os.path.join(os.path.dirname(__file__), "src", "es_indexer.py"))
es_indexer_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(es_indexer_module)
ESIndexer = es_indexer_module.ESIndexer


def load_queries(queries_file='queries/test_queries.txt'):
    """Load and preprocess queries"""
    with open(queries_file, 'r', encoding='utf-8') as f:
        queries = [line.strip() for line in f if line.strip()]
    
    # Convert queries (remove PHRASE keyword and quotes)
    def convert_query(q):
        if q.startswith('PHRASE '):
            return q.replace('PHRASE ', '', 1).replace('"', '').strip()
        return q.replace('"', '').strip()
    
    return [convert_query(q) for q in queries]


def get_index_stats(es, index_name):
    """Get index statistics"""
    stats = es.indices.stats(index=index_name)
    index_stats = stats['indices'][index_name]['total']
    
    disk_bytes = index_stats['store']['size_in_bytes']
    disk_mb = disk_bytes / (1024 * 1024)
    
    doc_count = index_stats['docs']['count']
    
    return {
        "disk_mb": round(disk_mb, 2),
        "documents": doc_count
    }


def evaluate_true_cold(es, index_name, queries):
    """
    SCENARIO 1: TRUE COLD CACHE
    - Clears cache before EVERY query
    - Most honest architectural comparison with SelfIndex
    - Shows true first-query performance
    """
    print("\n" + "="*70)
    print("SCENARIO 1: TRUE COLD CACHE (Cache cleared before each query)")
    print("="*70)
    print(f"Total queries: {len(queries)}")
    print("⚠️  This will take 2-3 minutes due to cache clearing operations...")
    print()
    
    latencies = []
    total_results = 0
    
    for i, query in enumerate(queries, 1):
        # CLEAR ALL CACHES before each query
        es.indices.clear_cache(index=index_name, query=True, fielddata=True, request=True)
        time.sleep(0.01)  # Let cache clear propagate
        
        start = time.perf_counter()
        try:
            response = es.search(
                index=index_name,
                body={
                    "query": {"match": {"content": query}},
                    "size": 10,  # Top-10 like SelfIndex
                    "_source": False,  # No document fetching
                    "track_total_hits": True
                }
            )
            latency = (time.perf_counter() - start) * 1000
            latencies.append(latency)
            total_results += len(response['hits']['hits'])
        except Exception as e:
            print(f"  Error on query {i}: {e}")
            continue
        
        if i % 50 == 0:
            print(f"  Processed {i}/{len(queries)} queries | Avg: {np.mean(latencies):.2f}ms")
    
    latencies = np.array(latencies)
    
    results = {
        "avg_ms": float(np.mean(latencies)),
        "median_ms": float(np.median(latencies)),
        "p50_ms": float(np.percentile(latencies, 50)),
        "p90_ms": float(np.percentile(latencies, 90)),
        "p95_ms": float(np.percentile(latencies, 95)),
        "p99_ms": float(np.percentile(latencies, 99)),
        "min_ms": float(np.min(latencies)),
        "max_ms": float(np.max(latencies)),
        "std_ms": float(np.std(latencies))
    }
    
    print(f"\n✓ TRUE COLD Results:")
    print(f"  Average: {results['avg_ms']:.2f} ms")
    print(f"  P95:     {results['p95_ms']:.2f} ms")
    print(f"  P99:     {results['p99_ms']:.2f} ms")
    
    return results, len(queries), total_results


def evaluate_warm_cache(es, index_name, queries):
    """
    SCENARIO 2: WARM CACHE
    - Runs each query 3 times, measures last 2 runs
    - Simulates production scenario with query cache
    - Shows best-case ES performance
    """
    print("\n" + "="*70)
    print("SCENARIO 2: WARM CACHE (Repeated queries, cache populated)")
    print("="*70)
    print(f"Total queries: {len(queries)}")
    print("Running each query 3 times, measuring last 2 runs...")
    print()
    
    latencies = []
    total_results = 0
    
    for i, query in enumerate(queries, 1):
        # Warmup: run once (don't measure)
        try:
            es.search(
                index=index_name,
                body={
                    "query": {"match": {"content": query}},
                    "size": 10,
                    "_source": False,
                    "track_total_hits": True
                }
            )
        except:
            continue
        
        # Measure: run twice and take average
        query_latencies = []
        for _ in range(2):
            start = time.perf_counter()
            try:
                response = es.search(
                    index=index_name,
                    body={
                        "query": {"match": {"content": query}},
                        "size": 10,
                        "_source": False,
                        "track_total_hits": True
                    }
                )
                latency = (time.perf_counter() - start) * 1000
                query_latencies.append(latency)
                total_results += len(response['hits']['hits'])
            except Exception as e:
                continue
        
        if query_latencies:
            latencies.append(np.mean(query_latencies))
        
        if i % 50 == 0:
            print(f"  Processed {i}/{len(queries)} queries | Avg: {np.mean(latencies):.2f}ms")
    
    latencies = np.array(latencies)
    
    results = {
        "avg_ms": float(np.mean(latencies)),
        "median_ms": float(np.median(latencies)),
        "p50_ms": float(np.percentile(latencies, 50)),
        "p90_ms": float(np.percentile(latencies, 90)),
        "p95_ms": float(np.percentile(latencies, 95)),
        "p99_ms": float(np.percentile(latencies, 99)),
        "min_ms": float(np.min(latencies)),
        "max_ms": float(np.max(latencies)),
        "std_ms": float(np.std(latencies))
    }
    
    print(f"\n✓ WARM CACHE Results:")
    print(f"  Average: {results['avg_ms']:.2f} ms")
    print(f"  P95:     {results['p95_ms']:.2f} ms")
    print(f"  P99:     {results['p99_ms']:.2f} ms")
    
    return results, len(queries), total_results


def evaluate_mixed_cache(es, index_name, queries):
    """
    SCENARIO 3: MIXED CACHE (what we currently have)
    - Run queries once in order
    - Later queries may benefit from partial cache
    - Realistic scenario
    """
    print("\n" + "="*70)
    print("SCENARIO 3: MIXED CACHE (Single pass through queries)")
    print("="*70)
    print(f"Total queries: {len(queries)}")
    print("Running each query once (current evaluation method)...")
    print()
    
    latencies = []
    total_results = 0
    
    for i, query in enumerate(queries, 1):
        start = time.perf_counter()
        try:
            response = es.search(
                index=index_name,
                body={
                    "query": {"match": {"content": query}},
                    "size": 10,
                    "_source": False,
                    "track_total_hits": True
                }
            )
            latency = (time.perf_counter() - start) * 1000
            latencies.append(latency)
            total_results += len(response['hits']['hits'])
        except Exception as e:
            continue
        
        if i % 50 == 0:
            print(f"  Processed {i}/{len(queries)} queries | Avg: {np.mean(latencies):.2f}ms")
    
    latencies = np.array(latencies)
    
    results = {
        "avg_ms": float(np.mean(latencies)),
        "median_ms": float(np.median(latencies)),
        "p50_ms": float(np.percentile(latencies, 50)),
        "p90_ms": float(np.percentile(latencies, 90)),
        "p95_ms": float(np.percentile(latencies, 95)),
        "p99_ms": float(np.percentile(latencies, 99)),
        "min_ms": float(np.min(latencies)),
        "max_ms": float(np.max(latencies)),
        "std_ms": float(np.std(latencies))
    }
    
    print(f"\n✓ MIXED CACHE Results:")
    print(f"  Average: {results['avg_ms']:.2f} ms")
    print(f"  P95:     {results['p95_ms']:.2f} ms")
    print(f"  P99:     {results['p99_ms']:.2f} ms")
    
    return results, len(queries), total_results


def main():
    """Run all three evaluation scenarios"""
    
    print("="*70)
    print("ELASTICSEARCH COMPREHENSIVE EVALUATION")
    print("Testing 3 Cache Scenarios for Complete Performance Picture")
    print("="*70)
    
    # Configuration
    es_host = 'localhost'
    es_port = 9200
    index_name = 'esindex-v1.0'
    
    # Connect to ES
    print(f"\nConnecting to Elasticsearch at {es_host}:{es_port}...")
    es = Elasticsearch([f"http://{es_host}:{es_port}"], request_timeout=30)
    
    if not es.ping():
        print("✗ Could not connect to Elasticsearch")
        sys.exit(1)
    
    print("✓ Connected to Elasticsearch")
    
    # Check index
    if not es.indices.exists(index=index_name):
        print(f"✗ Index '{index_name}' does not exist")
        sys.exit(1)
    
    print(f"✓ Index '{index_name}' found")
    
    # Load queries
    print("\nLoading queries...")
    queries = load_queries()
    print(f"✓ Loaded {len(queries)} queries")
    
    # Get index stats
    index_stats = get_index_stats(es, index_name)
    print(f"✓ Index stats: {index_stats['documents']} docs, {index_stats['disk_mb']} MB")
    
    # SCENARIO 1: TRUE COLD
    cold_results, cold_queries, cold_total_results = evaluate_true_cold(es, index_name, queries)
    
    # SCENARIO 2: WARM CACHE
    warm_results, warm_queries, warm_total_results = evaluate_warm_cache(es, index_name, queries)
    
    # SCENARIO 3: MIXED CACHE (recreate current results)
    mixed_results, mixed_queries, mixed_total_results = evaluate_mixed_cache(es, index_name, queries)
    
    # Calculate throughput for each scenario
    print("\n" + "="*70)
    print("CALCULATING THROUGHPUT (Artifact B)")
    print("="*70)
    
    # For throughput, measure time to execute 100 queries
    throughput_queries = queries[:100]
    
    print("\nThroughput measurement (100 queries)...")
    
    # Cold throughput
    start_time = time.time()
    for q in throughput_queries:
        es.indices.clear_cache(index=index_name, query=True)
        es.search(index=index_name, body={"query": {"match": {"content": q}}, "size": 10, "_source": False})
    cold_throughput = 100 / (time.time() - start_time)
    
    # Warm throughput
    start_time = time.time()
    for q in throughput_queries:
        es.search(index=index_name, body={"query": {"match": {"content": q}}, "size": 10, "_source": False})
    warm_throughput = 100 / (time.time() - start_time)
    
    # Mixed throughput (use average latency)
    mixed_throughput = 1000 / mixed_results['avg_ms']
    
    print(f"✓ Cold throughput: {cold_throughput:.2f} QPS")
    print(f"✓ Warm throughput: {warm_throughput:.2f} QPS")
    print(f"✓ Mixed throughput: {mixed_throughput:.2f} QPS")
    
    # Save results
    os.makedirs('results', exist_ok=True)
    
    # TRUE COLD results
    cold_output = {
        "index_id": index_name,
        "scenario": "TRUE_COLD_CACHE",
        "description": "Cache cleared before each query - most honest architectural comparison",
        "configuration": {
            "top_k": 10,
            "fields_searched": ["content"],
            "document_fetching": False,
            "cache_clearing": "before_every_query"
        },
        "artifact_A_latency": cold_results,
        "artifact_B_throughput": {
            "queries_per_second": round(cold_throughput, 2),
            "total_queries": cold_queries,
            "note": "Measured with cache clearing"
        },
        "artifact_C_memory": {
            "disk_mb": index_stats['disk_mb'],
            "documents": index_stats['documents']
        }
    }
    
    # WARM CACHE results
    warm_output = {
        "index_id": index_name,
        "scenario": "WARM_CACHE",
        "description": "Queries run repeatedly - shows best-case ES performance with full cache",
        "configuration": {
            "top_k": 10,
            "fields_searched": ["content"],
            "document_fetching": False,
            "cache_warming": "queries_run_3x_each"
        },
        "artifact_A_latency": warm_results,
        "artifact_B_throughput": {
            "queries_per_second": round(warm_throughput, 2),
            "total_queries": warm_queries,
            "note": "Measured with warm cache"
        },
        "artifact_C_memory": {
            "disk_mb": index_stats['disk_mb'],
            "documents": index_stats['documents']
        }
    }
    
    # MIXED CACHE results
    mixed_output = {
        "index_id": index_name,
        "scenario": "MIXED_CACHE",
        "description": "Single pass through queries - realistic production scenario",
        "configuration": {
            "top_k": 10,
            "fields_searched": ["content"],
            "document_fetching": False,
            "cache_behavior": "natural_buildup"
        },
        "artifact_A_latency": mixed_results,
        "artifact_B_throughput": {
            "queries_per_second": round(mixed_throughput, 2),
            "total_queries": mixed_queries,
            "note": "Calculated from average latency"
        },
        "artifact_C_memory": {
            "disk_mb": index_stats['disk_mb'],
            "documents": index_stats['documents']
        }
    }
    
    # Write files
    with open('results/eval_esindex-v1.0_COLD.json', 'w') as f:
        json.dump(cold_output, f, indent=2)
    
    with open('results/eval_esindex-v1.0_WARM.json', 'w') as f:
        json.dump(warm_output, f, indent=2)
    
    with open('results/eval_esindex-v1.0_MIXED.json', 'w') as f:
        json.dump(mixed_output, f, indent=2)
    
    # Summary
    print("\n" + "="*70)
    print("EVALUATION COMPLETE - SUMMARY")
    print("="*70)
    print(f"\n{'Scenario':<20} {'P95 Latency':<15} {'Throughput':<15} {'Use Case':<30}")
    print("-" * 80)
    print(f"{'TRUE COLD':<20} {cold_results['p95_ms']:>8.2f} ms     {cold_throughput:>8.2f} QPS   {'Honest comparison':<30}")
    print(f"{'MIXED':<20} {mixed_results['p95_ms']:>8.2f} ms     {mixed_throughput:>8.2f} QPS   {'Realistic production':<30}")
    print(f"{'WARM':<20} {warm_results['p95_ms']:>8.2f} ms     {warm_throughput:>8.2f} QPS   {'Best-case performance':<30}")
    print(f"{'SelfIndex (ref)':<20} {'~9.00 ms':>12}     {'~200 QPS':>12}   {'In-memory baseline':<30}")
    
    print("\n" + "="*70)
    print("FILES SAVED:")
    print("  - results/eval_esindex-v1.0_COLD.json  (TRUE COLD)")
    print("  - results/eval_esindex-v1.0_MIXED.json (MIXED)")
    print("  - results/eval_esindex-v1.0_WARM.json  (WARM)")
    print("="*70)
    
    print("\n💡 RECOMMENDATION FOR ASSIGNMENT:")
    print("  Use COLD results for fairest comparison with SelfIndex")
    print("  Include all three in plots to show ES performance range")
    print("  SelfIndex is 'always warm' (in-memory), so COLD shows")
    print("  true architectural overhead of client-server design")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()
