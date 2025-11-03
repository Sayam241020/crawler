#!/usr/bin/env python
"""
Evaluation Script for SelfIndex following index_base.py specification

Evaluates indices and measures Artifacts A, B, C as per assignment.

Usage:
    python evaluate_v2.py -x 1 -y 1 -z 1 -q T -optim 0
    python evaluate_v2.py -x 3 -y 1 -z 1 -q T -optim 0 --queries custom_queries.txt
"""

import sys
import os
import argparse
import json
import time
import psutil
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.self_indexer import SelfIndexer
from src.self_indexer_x2 import SelfIndexer_x2
from src.self_indexer_x3 import SelfIndexer_x3


def load_indexer(x, y, z, optim, mode='TAAT', use_lazy=True):
    """
    Load the built index with memory-efficient strategy
    
    Args:
        x: Index type (1=Boolean, 2=TF, 3=TF-IDF)
        y: Datastore (1=JSON, 2=SQLite)
        z: Compression (1=None, 2=Elias, 3=Zlib)
        optim: Optimization (0, sp, th, es)
        mode: Query mode - 'TAAT' or 'DAAT' (runtime parameter, NOT in identifier)
        use_lazy: If True, use lazy loading for compressed JSON indices (saves RAM)
    
    Returns:
        Tuple of (indexer, identifier, query_mode)
    
    Note: Query mode (TAAT/DAAT) is a RUNTIME choice, not part of index identifier!
    """
    # Build identifier WITHOUT query mode
    identifier = f"SelfIndex_i{x}d{y}c{z}o{optim}"
    
    # Determine compression type
    compr_map = {1: 'NONE', 2: 'CODE', 3: 'CLIB'}
    compression = compr_map[z]
    
    # Map optimization parameter value to enum name
    optim_map = {'0': 'Null', 'sp': 'Skipping', 'th': 'Thresholding', 'es': 'EarlyStopping'}
    optim_name = optim_map.get(optim, 'Null')
    
    # ===== JSON DATASTORE (y=1) =====
    if y == 1:
        print(f"Loading JSON index (datastore=CUSTOM)")
        
        # For compressed indices (z=2 or z=3), use lazy loading to save RAM
        if use_lazy and z in [2, 3]:
            print(f"  Using LAZY loading for compressed index (memory-efficient)")
            from src.lazy_indexer import LazyCompressedIndexer_x2, LazyCompressedIndexer_x3
            
            if x == 1:
                indexer = SelfIndexer()  # Boolean doesn't have lazy version yet
            elif x == 2:
                indexer = LazyCompressedIndexer_x2(compression_type=compression)
            elif x == 3:
                indexer = LazyCompressedIndexer_x3(compression_type=compression)
            else:
                raise ValueError(f"Invalid index type: {x}")
        else:
            # Normal loading for uncompressed JSON indices
            if x == 1:
                indexer = SelfIndexer()
            elif x == 2:
                indexer = SelfIndexer_x2()
            elif x == 3:
                indexer = SelfIndexer_x3()
            else:
                raise ValueError(f"Invalid index type: {x}")
    
    # ===== SQLite DATASTORE (y=2) =====
    elif y == 2:
        print(f"Loading SQLite index (datastore=DB1)")
        from src.sqlite_indexer import SQLiteIndexer_x1, SQLiteIndexer_x2, SQLiteIndexer_x3
        
        if x == 1:
            indexer = SQLiteIndexer_x1(compression_type=compression, optim=optim_name)
        elif x == 2:
            indexer = SQLiteIndexer_x2(compression_type=compression, optim=optim_name)
        elif x == 3:
            indexer = SQLiteIndexer_x3(compression_type=compression, optim=optim_name)
        else:
            raise ValueError(f"Invalid index type: {x}")
    
    else:
        raise ValueError(f"Datastore y={y} not supported. Use y=1 (JSON) or y=2 (SQLite)")
    
    # Load index
    print(f"Loading index: {identifier}")
    indexer.load_index(index_id=identifier)
    
    # Return indexer, identifier, and query mode
    return indexer, identifier, mode


def evaluate_index(x, y, z, optim, query_mode='TAAT', query_file='queries.txt', top_k=10):
    """
    Evaluate index and measure Artifacts A, B, C
    
    Args:
        x: Index type (1=Boolean, 2=TF, 3=TF-IDF)
        y: Datastore (1=JSON, 2=SQLite)
        z: Compression (1=None, 2=Elias, 3=Zlib)
        optim: Optimization (0, sp, th, es)
        query_mode: 'TAAT' or 'DAAT' (runtime parameter)
        query_file: Path to queries file
        top_k: Number of results to return
    
    Artifact A: Latency (response time with p95, p99)
    Artifact B: Throughput (queries/second)
    Artifact C: Memory footprint (disk + RAM)
    """
    # Build identifier WITHOUT query mode
    identifier = f"SelfIndex_i{x}d{y}c{z}o{optim}"
    
    print(f"\n{'='*80}")
    print(f"Evaluating: {identifier}")
    print(f"Query Mode: {query_mode} (runtime parameter)")
    print(f"{'='*80}\n")
    
    # Load indexer with query mode
    print("Loading index...")
    indexer, identifier, mode = load_indexer(x, y, z, optim, mode=query_mode)
    print(f"✓ Index loaded: {identifier}")
    print(f"✓ Query mode: {mode}")
    
    # Load queries
    print(f"\nLoading queries from: {query_file}")
    with open(query_file, 'r', encoding='utf-8') as f:
        queries = [line.strip() for line in f if line.strip()]
    print(f"✓ Loaded {len(queries)} queries")
    
    # Warmup: Run first 5 queries to warm up caches
    print("\nWarming up (running first 5 queries)...")
    for query in queries[:5]:
        try:
            if x == 1:
                _ = indexer.query(query)
            else:
                _ = indexer.query(query, mode=mode, top_k=top_k)
        except:
            pass
    print("✓ Warmup complete")
    
    # ===== ARTIFACT A: LATENCY =====
    print(f"\n{'='*80}")
    print("ARTIFACT A: Measuring Latency")
    print(f"{'='*80}")
    
    latencies = []
    for i, query in enumerate(queries, 1):
        start_time = time.perf_counter()  # High-precision timer
        try:
            # Boolean indexer (x=1) doesn't have mode parameter
            if x == 1:
                results = indexer.query(query)
            else:
                results = indexer.query(query, mode=mode, top_k=top_k)
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            latencies.append(elapsed_ms)
            
            if i % 10 == 0:
                print(f"  Processed {i}/{len(queries)} queries...")
        except Exception as e:
            print(f"  Warning: Query {i} failed: {e}")
            continue
    
    artifact_A = {
        'average_ms': float(np.mean(latencies)),
        'median_ms': float(np.median(latencies)),
        'p50_ms': float(np.percentile(latencies, 50)),  # Explicit median
        'p90_ms': float(np.percentile(latencies, 90)),
        'p95_ms': float(np.percentile(latencies, 95)),
        'p99_ms': float(np.percentile(latencies, 99)),
        'min_ms': float(np.min(latencies)),
        'max_ms': float(np.max(latencies)),
        'std_ms': float(np.std(latencies)),
        'num_zero_latency': int(sum(1 for l in latencies if l < 0.001))  # Count sub-microsecond queries
    }
    
    print(f"\n✓ Latency Results:")
    print(f"  Average: {artifact_A['average_ms']:.4f} ms")
    print(f"  Median:  {artifact_A['median_ms']:.4f} ms")
    print(f"  P90:     {artifact_A['p90_ms']:.4f} ms")
    print(f"  P95:     {artifact_A['p95_ms']:.4f} ms")
    print(f"  P99:     {artifact_A['p99_ms']:.4f} ms")
    print(f"  Min:     {artifact_A['min_ms']:.4f} ms")
    print(f"  Max:     {artifact_A['max_ms']:.4f} ms")
    print(f"  StdDev:  {artifact_A['std_ms']:.4f} ms")
    if artifact_A['num_zero_latency'] > 0:
        print(f"  ⚠ Warning: {artifact_A['num_zero_latency']} queries < 0.001ms (possible measurement issues)")
    
    # ===== ARTIFACT B: THROUGHPUT =====
    print(f"\n{'='*80}")
    print("ARTIFACT B: Measuring Throughput")
    print(f"{'='*80}")
    
    total_time = sum(latencies) / 1000  # Convert to seconds
    qps = len(queries) / total_time if total_time > 0 else 0
    
    artifact_B = {
        'queries_per_second': float(qps),
        'total_queries': len(queries),
        'total_time_seconds': float(total_time)
    }
    
    print(f"\n✓ Throughput Results:")
    print(f"  Queries/second: {artifact_B['queries_per_second']:.2f}")
    print(f"  Total time: {artifact_B['total_time_seconds']:.2f} seconds")
    
    # ===== ARTIFACT C: MEMORY FOOTPRINT =====
    print(f"\n{'='*80}")
    print("ARTIFACT C: Measuring Memory Footprint")
    print(f"{'='*80}")
    
    # Disk size
    index_file = f"indices/{identifier}.json"
    if os.path.exists(index_file):
        disk_size_bytes = os.path.getsize(index_file)
        disk_size_mb = disk_size_bytes / (1024 * 1024)
    else:
        disk_size_mb = 0
    
    # RAM usage
    process = psutil.Process()
    ram_bytes = process.memory_info().rss
    ram_gb = ram_bytes / (1024 ** 3)
    
    artifact_C = {
        'disk_mb': float(disk_size_mb),
        'ram_gb': float(ram_gb),
        'disk_bytes': int(disk_size_bytes) if os.path.exists(index_file) else 0
    }
    
    print(f"\n✓ Memory Results:")
    print(f"  Disk size: {artifact_C['disk_mb']:.2f} MB")
    print(f"  RAM usage: {artifact_C['ram_gb']:.4f} GB")
    
    # ===== SAVE RESULTS =====
    os.makedirs('results', exist_ok=True)
    # Include query mode in filename to differentiate TAAT vs DAAT results
    result_file = f"results/eval_{identifier}_q{query_mode}.json"
    
    results = {
        'identifier': identifier,
        'configuration': {
            'index_type': x,
            'datastore': y,
            'compression': z,
            'query_mode': query_mode,  # Runtime parameter
            'optimization': optim
        },
        'artifact_A_latency': artifact_A,
        'artifact_B_throughput': artifact_B,
        'artifact_C_memory': artifact_C,
        'evaluation_params': {
            'query_file': query_file,
            'num_queries': len(queries),
            'top_k': top_k
        }
    }
    
    with open(result_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n{'='*80}")
    print(f"✓ Evaluation complete!")
    print(f"  Results saved to: {result_file}")
    print(f"{'='*80}\n")
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate SelfIndex following index_base.py specification",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Evaluate with TAAT (default)
  python evaluate.py -x 3 -y 1 -z 1 -optim 0
  
  # Evaluate with DAAT
  python evaluate.py -x 3 -y 1 -z 1 -optim 0 -q D
  
  # Compare datastores
  python evaluate.py -x 3 -y 1 -z 1 -optim 0  # JSON
  python evaluate.py -x 3 -y 2 -z 1 -optim 0  # SQLite
        """
    )
    
    parser.add_argument('-x', '--index-type', type=int, choices=[1, 2, 3], required=True,
                        help='Index type: 1=Boolean, 2=TF, 3=TF-IDF')
    parser.add_argument('-y', '--datastore', type=int, choices=[1, 2], required=True,
                        help='Datastore: 1=JSON, 2=SQLite')
    parser.add_argument('-z', '--compression', type=int, choices=[1, 2, 3], required=True,
                        help='Compression: 1=None, 2=Elias, 3=Zlib')
    parser.add_argument('-q', '--query-mode', choices=['T', 'D'], default='T',
                        help='Query mode: T=TAAT (default), D=DAAT (runtime parameter)')
    parser.add_argument('-optim', '--optimization', choices=['0', 'sp', 'th', 'es'], required=True,
                        help='Optimization: 0=None, sp=SkipPointers, th=Thresholding, es=EarlyStopping')
    parser.add_argument('--queries', default='queries/test_queries.txt',
                        help='Query file (default: queries/test_queries.txt)')
    parser.add_argument('--top-k', type=int, default=10,
                        help='Number of results to return (default: 10)')
    
    args = parser.parse_args()
    
    # Convert query mode: T -> TAAT, D -> DAAT
    query_mode = 'TAAT' if args.query_mode == 'T' else 'DAAT'
    
    try:
        evaluate_index(
            x=args.index_type,
            y=args.datastore,
            z=args.compression,
            optim=args.optimization,
            query_mode=query_mode,
            query_file=args.queries,
            top_k=args.top_k
        )
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
