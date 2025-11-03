"""
Evaluate Elasticsearch Index Performance
Measures Artifacts A (latency), B (throughput), C (memory)
"""

import os
import sys
import json
import time
import argparse
import numpy as np
from elasticsearch import Elasticsearch

# Direct import to avoid src/__init__.py NLTK dependency
import importlib.util
spec = importlib.util.spec_from_file_location("es_indexer", os.path.join(os.path.dirname(__file__), "src", "es_indexer.py"))
es_indexer_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(es_indexer_module)
ESIndexer = es_indexer_module.ESIndexer


def preprocess_query_for_es(query_text):
    """
    Convert assignment-format query to Elasticsearch query.
    
    Removes PHRASE keyword and quotes for ES compatibility.
    ES will handle phrase queries with match_phrase.
    
    Args:
        query_text: Original query (e.g., PHRASE "machine learning")
        
    Returns:
        Processed query for ES (e.g., "machine learning")
    """
    # Remove PHRASE keyword
    if query_text.startswith('PHRASE '):
        query_text = query_text.replace('PHRASE ', '', 1)
    
    # Remove quotes
    query_text = query_text.replace('"', '')
    
    return query_text.strip()


def is_phrase_query(query_text):
    """Check if query is a phrase query"""
    return query_text.startswith('PHRASE ')


def warmup_cache(es, index_id, warmup_queries, warmup_count=20):
    """
    Warm up Elasticsearch cache before evaluation.
    Runs a subset of queries to populate filter and query caches.
    
    Args:
        es: Elasticsearch client
        index_id: Name of the index
        warmup_queries: List of preprocessed queries
        warmup_count: Number of queries to use for warmup
    """
    print(f"\nWarming up Elasticsearch cache with {warmup_count} queries...")
    
    for i, query_text in enumerate(warmup_queries[:warmup_count], 1):
        try:
            # Simple match query on content field only
            es.search(
                index=index_id,
                body={
                    "query": {
                        "match": {
                            "content": query_text
                        }
                    },
                    "size": 10,
                    "_source": False  # Don't fetch document content
                }
            )
        except Exception:
            pass  # Ignore errors during warmup
    
    print(f"✓ Cache warmup complete")


def execute_query(es_indexer, index_id, query_text):
    """
    Execute a single query on Elasticsearch with optimizations.
    
    OPTIMIZATIONS FOR FAIR COMPARISON WITH SELFINDEX:
    - Top-10 results only (matches SelfIndex ranking)
    - Single field search on 'content' (matches SelfIndex)
    - Don't fetch full document content (matches SelfIndex counting)
    
    Args:
        es_indexer: ESIndexer instance
        index_id: Name of the index
        query_text: Query string
        
    Returns:
        tuple: (num_results, latency_ms, is_phrase)
    """
    # Preprocess query
    is_phrase = is_phrase_query(query_text)
    processed_query = preprocess_query_for_es(query_text)
    
    # Measure query time with high precision
    start_time = time.perf_counter()
    
    # Execute based on query type
    if is_phrase:
        # Phrase query - use match_phrase on content field only
        query_body = {
            "query": {
                "match_phrase": {
                    "content": processed_query
                }
            },
            "size": 10,  # TOP-10 like SelfIndex
            "_source": False,  # Don't fetch full documents (just count)
            "track_total_hits": True
        }
    else:
        # Regular query - use match on content field only (like SelfIndex)
        query_body = {
            "query": {
                "match": {
                    "content": processed_query
                }
            },
            "size": 10,  # TOP-10 like SelfIndex
            "_source": False,  # Don't fetch full documents
            "track_total_hits": True
        }
    
    # Execute query
    try:
        response = es_indexer.es.search(index=index_id, body=query_body)
        end_time = time.perf_counter()
        latency_ms = (end_time - start_time) * 1000
        
        num_results = response['hits']['total']['value']
        return num_results, latency_ms, is_phrase
        
    except Exception as e:
        end_time = time.perf_counter()
        latency_ms = (end_time - start_time) * 1000
        print(f"Error executing query '{query_text}': {e}")
        return 0, latency_ms, is_phrase


def evaluate_index(es_host='localhost', es_port=9200, index_name='esindex-v1.0', 
                   queries_file='queries/test_queries.txt'):
    """
    Evaluate Elasticsearch index performance.
    
    Args:
        es_host: Elasticsearch host
        es_port: Elasticsearch port
        index_name: Name of the index to evaluate
        queries_file: Path to queries file
        
    Returns:
        dict: Evaluation results
    """
    print("=" * 70)
    print("ELASTICSEARCH INDEX EVALUATION")
    print("=" * 70)
    print(f"Index: {index_name}")
    print(f"Server: {es_host}:{es_port}")
    print(f"Queries: {queries_file}")
    print()
    
    # Connect to Elasticsearch
    print("Connecting to Elasticsearch...")
    es = Elasticsearch([f"http://{es_host}:{es_port}"], request_timeout=30)
    
    if not es.ping():
        print("✗ Could not connect to Elasticsearch")
        sys.exit(1)
    
    print(f"✓ Connected to Elasticsearch")
    
    # Create indexer
    indexer = ESIndexer(es)
    
    # Check index exists
    if not es.indices.exists(index=index_name):
        print(f"✗ Index '{index_name}' does not exist")
        sys.exit(1)
    
    print(f"✓ Index '{index_name}' found")
    
    # Load queries
    print(f"\nLoading queries from {queries_file}...")
    if not os.path.exists(queries_file):
        print(f"✗ Query file not found: {queries_file}")
        sys.exit(1)
    
    with open(queries_file, 'r', encoding='utf-8') as f:
        queries = [line.strip() for line in f if line.strip()]
    
    print(f"✓ Loaded {len(queries)} queries")
    
    # Preprocess all queries
    preprocessed_queries = [preprocess_query_for_es(q) for q in queries]
    
    # NO WARMUP - Fair architectural comparison
    # SelfIndex is always "warm" (in-memory), so ES should be measured cold too
    # This shows true client-server overhead vs in-memory performance
    
    # Execute queries and collect metrics
    print(f"\nExecuting queries (COLD CACHE - Fair architectural comparison)...")
    print(f"  - Top-10 results only (matches SelfIndex)")
    print(f"  - Single field search on 'content' (matches SelfIndex)")
    print(f"  - No document fetching (matches SelfIndex counting)")
    print(f"  - Cold cache (matches SelfIndex always-warm in-memory architecture)")
    print("-" * 70)
    
    latencies = []
    phrase_queries = 0
    regular_queries = 0
    total_results = 0
    
    for i, query in enumerate(queries, 1):
        num_results, latency, is_phrase = execute_query(indexer, index_name, query)
        latencies.append(latency)
        total_results += num_results
        
        if is_phrase:
            phrase_queries += 1
        else:
            regular_queries += 1
        
        # Progress indicator
        if i % 50 == 0:
            avg_latency = np.mean(latencies)
            print(f"  Processed {i}/{len(queries)} queries | Avg latency: {avg_latency:.2f}ms")
    
    print(f"✓ Completed {len(queries)} queries")
    print(f"  Phrase queries: {phrase_queries}")
    print(f"  Regular queries: {regular_queries}")
    print(f"  Total results found: {total_results}")
    
    # Calculate Artifact A: Latency metrics
    print("\n" + "=" * 70)
    print("ARTIFACT A: LATENCY METRICS")
    print("=" * 70)
    
    latency_metrics = {
        "average_ms": float(np.mean(latencies)),
        "median_ms": float(np.median(latencies)),
        "p50_ms": float(np.percentile(latencies, 50)),
        "p90_ms": float(np.percentile(latencies, 90)),
        "p95_ms": float(np.percentile(latencies, 95)),
        "p99_ms": float(np.percentile(latencies, 99)),
        "min_ms": float(np.min(latencies)),
        "max_ms": float(np.max(latencies)),
        "std_dev_ms": float(np.std(latencies))
    }
    
    print(f"Average latency: {latency_metrics['average_ms']:.2f} ms")
    print(f"Median latency: {latency_metrics['median_ms']:.2f} ms")
    print(f"P95 latency: {latency_metrics['p95_ms']:.2f} ms")
    print(f"P99 latency: {latency_metrics['p99_ms']:.2f} ms")
    
    # Calculate Artifact B: Throughput
    print("\n" + "=" * 70)
    print("ARTIFACT B: THROUGHPUT")
    print("=" * 70)
    
    total_time_sec = sum(latencies) / 1000.0
    queries_per_second = len(queries) / total_time_sec if total_time_sec > 0 else 0
    
    throughput_metrics = {
        "queries_per_second": float(queries_per_second),
        "total_queries": len(queries),
        "total_time_sec": float(total_time_sec)
    }
    
    print(f"Queries per second: {queries_per_second:.2f}")
    print(f"Total queries: {len(queries)}")
    print(f"Total time: {total_time_sec:.2f} seconds")
    
    # Get Artifact C: Memory footprint
    print("\n" + "=" * 70)
    print("ARTIFACT C: MEMORY FOOTPRINT")
    print("=" * 70)
    
    try:
        # Get index stats
        stats = es.cat.indices(index=index_name, format="json", 
                               h="store.size,pri.store.size,docs.count")
        
        if stats:
            store_size = stats[0]['store.size']  # e.g., "359.9mb"
            pri_store_size = stats[0]['pri.store.size']
            doc_count = stats[0]['docs.count']
            
            # Convert to MB
            def parse_size(size_str):
                """Convert ES size string to MB"""
                size_str = size_str.lower()
                if 'gb' in size_str:
                    return float(size_str.replace('gb', '')) * 1024
                elif 'mb' in size_str:
                    return float(size_str.replace('mb', ''))
                elif 'kb' in size_str:
                    return float(size_str.replace('kb', '')) / 1024
                elif 'b' in size_str:
                    return float(size_str.replace('b', '')) / (1024 * 1024)
                return 0
            
            disk_mb = parse_size(store_size)
            
            memory_metrics = {
                "disk_size_mb": float(disk_mb),
                "disk_size_readable": store_size,
                "primary_size_readable": pri_store_size,
                "document_count": int(doc_count),
                "ram_usage_gb": 0.0  # ES manages its own memory, not directly measurable
            }
            
            print(f"Disk size: {disk_mb:.2f} MB ({store_size})")
            print(f"Primary size: {pri_store_size}")
            print(f"Documents: {doc_count}")
            print(f"RAM usage: N/A (managed by ES JVM)")
        else:
            memory_metrics = {
                "disk_size_mb": 0.0,
                "disk_size_readable": "N/A",
                "ram_usage_gb": 0.0
            }
    except Exception as e:
        print(f"Could not retrieve memory stats: {e}")
        memory_metrics = {
            "disk_size_mb": 0.0,
            "disk_size_readable": "N/A",
            "ram_usage_gb": 0.0
        }
    
    # Compile results
    results = {
        "index_name": index_name,
        "index_type": "elasticsearch",
        "configuration": {
            "analyzer": "custom_english",
            "preprocessing": "lowercase + stopwords + stemming"
        },
        "query_count": len(queries),
        "phrase_queries": phrase_queries,
        "regular_queries": regular_queries,
        "artifact_a_latency": latency_metrics,
        "artifact_b_throughput": throughput_metrics,
        "artifact_c_memory": memory_metrics
    }
    
    return results


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Evaluate Elasticsearch index performance',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--host',
        default='localhost',
        help='Elasticsearch host (default: localhost)'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        default=9200,
        help='Elasticsearch port (default: 9200)'
    )
    
    parser.add_argument(
        '--index',
        default='esindex-v1.0',
        help='Index name (default: esindex-v1.0)'
    )
    
    parser.add_argument(
        '--queries',
        default='queries/test_queries.txt',
        help='Path to queries file (default: queries/test_queries.txt)'
    )
    
    parser.add_argument(
        '--output',
        default=None,
        help='Output file for results (default: results/eval_<index>.json)'
    )
    
    args = parser.parse_args()
    
    try:
        # Run evaluation
        results = evaluate_index(
            es_host=args.host,
            es_port=args.port,
            index_name=args.index,
            queries_file=args.queries
        )
        
        # Save results
        if args.output is None:
            output_file = f"results/eval_{args.index}.json"
        else:
            output_file = args.output
        
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        
        print("\n" + "=" * 70)
        print("EVALUATION COMPLETE")
        print("=" * 70)
        print(f"Results saved to: {output_file}")
        print()
        
    except KeyboardInterrupt:
        print("\n✗ Evaluation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Evaluation failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
