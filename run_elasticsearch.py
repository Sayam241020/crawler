#!/usr/bin/env python3
"""
Main script to run Elasticsearch indexing and evaluation
Generates all required artifacts for Activity 1

This script:
1. Starts Elasticsearch (via Docker)
2. Indexes news and wiki data
3. Runs search queries
4. Measures performance (latency, throughput, memory)
5. Saves all metrics to results/metrics/

Artifacts Generated:
- Artifact A: Search latency (P95, P99)
- Artifact B: Throughput (docs/sec)
- Artifact C: Memory footprint (MB)
"""

import json
import os
import sys
from pathlib import Path

# Add src to path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent))

from src.elasticsearch_impl.es_indexer import ElasticsearchIndexer, load_jsonl_data


def ensure_directories():
    """Create results directories if they don't exist"""
    os.makedirs('results/metrics', exist_ok=True)
    os.makedirs('results/plots', exist_ok=True)
    os.makedirs('tests/query_sets', exist_ok=True)


def generate_test_queries():
    """
    Generate diverse test queries for evaluation
    
    Why diverse?
    - Different query lengths (1-4 words)
    - Different domains (tech, politics, science, etc.)
    - Common vs rare terms
    - This tests various aspects of the search system
    """
    queries = [
        # Technology (common terms)
        "artificial intelligence machine learning",
        "software development programming",
        "data science analytics",
        "cloud computing infrastructure",
        "cybersecurity network protection",
        "quantum computing research",
        
        # Science (moderate frequency)
        "climate change environment",
        "space exploration technology",
        "medical research health",
        "renewable energy solar",
        "genetic engineering biology",
        
        # Politics/Economy (common terms)
        "economic policy government",
        "election campaign politics",
        "international relations diplomacy",
        
        # General (mixed)
        "education system reform",
        "transportation infrastructure",
        "financial markets investment",
        "social media platform",
        "healthcare insurance coverage",
        
        # Single word queries
        "technology",
        "science",
        "education",
        
        # Rare terms (likely low frequency)
        "quantum entanglement photon",
        "blockchain cryptocurrency decentralized"
    ]
    
    return queries


def main():
    """Main execution function"""
    
    print("="*70)
    print(" " * 15 + "ELASTICSEARCH INDEXING (ESIndex-v1.0)")
    print("="*70)
    print("\nThis script will:")
    print("  1. Connect to Elasticsearch (starts Docker if needed)")
    print("  2. Index news and wiki datasets")
    print("  3. Run test queries and measure performance")
    print("  4. Generate artifacts A, B, C")
    print("\nEstimated time: 3-5 minutes")
    print("="*70)
    
    # Ensure directories exist
    ensure_directories()
    
    try:
        # ========================================
        # Initialize Elasticsearch
        # ========================================
        print("\n" + "="*70)
        print("STEP 1: CONNECTING TO ELASTICSEARCH")
        print("="*70)
        
        indexer = ElasticsearchIndexer(auto_start_docker=True)
        
        # ========================================
        # Index News Data
        # ========================================
        print("\n" + "="*70)
        print("STEP 2: INDEXING NEWS DATA")
        print("="*70)
        
        # Load news data
        news_file = 'data/raw/news/news_raw.jsonl'
        if not os.path.exists(news_file):
            print(f"\n❌ Error: {news_file} not found!")
            print("Please run: python src/utils/data_downloader.py")
            return 1
        
        news_docs = load_jsonl_data(news_file, limit=5000)
        
        # Create index with custom settings
        news_index = 'esindex-v1.0-news'
        indexer.create_index(news_index)
        
        # Index documents and measure throughput (Artifact B)
        news_stats = indexer.index_documents(news_index, news_docs)
        
        # Save indexing stats
        with open('results/metrics/es_news_indexing_stats.json', 'w') as f:
            json.dump(news_stats, f, indent=2)
        
        print(f"✓ Saved indexing stats to results/metrics/es_news_indexing_stats.json")
        
        # ========================================
        # Index Wiki Data
        # ========================================
        print("\n" + "="*70)
        print("STEP 3: INDEXING WIKI DATA")
        print("="*70)
        
        # Load wiki data
        wiki_file = 'data/raw/wiki/wiki_raw.jsonl'
        if not os.path.exists(wiki_file):
            print(f"\n❌ Error: {wiki_file} not found!")
            print("Please run: python src/utils/data_downloader.py")
            return 1
        
        wiki_docs = load_jsonl_data(wiki_file, limit=5000)
        
        # Create index
        wiki_index = 'esindex-v1.0-wiki'
        indexer.create_index(wiki_index)
        
        # Index documents
        wiki_stats = indexer.index_documents(wiki_index, wiki_docs)
        
        # Save stats
        with open('results/metrics/es_wiki_indexing_stats.json', 'w') as f:
            json.dump(wiki_stats, f, indent=2)
        
        print(f"✓ Saved indexing stats to results/metrics/es_wiki_indexing_stats.json")
        
        # ========================================
        # Generate Test Queries
        # ========================================
        print("\n" + "="*70)
        print("STEP 4: GENERATING TEST QUERIES")
        print("="*70)
        
        test_queries = generate_test_queries()
        print(f"\n📝 Generated {len(test_queries)} test queries covering:")
        print("   - Technology, Science, Politics domains")
        print("   - Various query lengths (1-4 words)")
        print("   - Common and rare terms")
        
        # Save queries
        with open('tests/query_sets/test_queries.json', 'w') as f:
            json.dump(test_queries, f, indent=2)
        
        print(f"✓ Saved queries to tests/query_sets/test_queries.json")
        
        # ========================================
        # Evaluate Search Performance (Artifact A)
        # ========================================
        print("\n" + "="*70)
        print("STEP 5: EVALUATING SEARCH LATENCY (Artifact A)")
        print("="*70)
        
        print("\n📊 Testing NEWS index...")
        news_results, news_search_stats = indexer.batch_search(
            news_index, 
            test_queries,
            size=10
        )
        
        with open('results/metrics/es_news_search_stats.json', 'w') as f:
            json.dump(news_search_stats, f, indent=2)
        
        print(f"✓ Saved search stats to results/metrics/es_news_search_stats.json")
        
        print("\n📊 Testing WIKI index...")
        wiki_results, wiki_search_stats = indexer.batch_search(
            wiki_index,
            test_queries,
            size=10
        )
        
        with open('results/metrics/es_wiki_search_stats.json', 'w') as f:
            json.dump(wiki_search_stats, f, indent=2)
        
        print(f"✓ Saved search stats to results/metrics/es_wiki_search_stats.json")
        
        # ========================================
        # Measure Memory Footprint (Artifact C)
        # ========================================
        print("\n" + "="*70)
        print("STEP 6: MEASURING MEMORY FOOTPRINT (Artifact C)")
        print("="*70)
        
        news_memory = indexer.get_index_stats(news_index)
        wiki_memory = indexer.get_index_stats(wiki_index)
        
        with open('results/metrics/es_news_memory.json', 'w') as f:
            json.dump(news_memory, f, indent=2)
        
        with open('results/metrics/es_wiki_memory.json', 'w') as f:
            json.dump(wiki_memory, f, indent=2)
        
        print(f"✓ Saved memory stats to results/metrics/")
        
        # ========================================
        # Generate Summary Report
        # ========================================
        print("\n" + "="*70)
        print("FINAL SUMMARY - ASSIGNMENT ARTIFACTS")
        print("="*70)
        
        summary = {
            "activity": "Activity 1 - Elasticsearch Implementation",
            "version": "ESIndex-v1.0",
            "datasets": {
                "news": {
                    "documents": news_stats['total_docs'],
                    "index_name": news_index
                },
                "wiki": {
                    "documents": wiki_stats['total_docs'],
                    "index_name": wiki_index
                }
            },
            "artifacts": {
                "A_latency": {
                    "news": {
                        "p95_ms": news_search_stats['p95_latency_ms'],
                        "p99_ms": news_search_stats['p99_latency_ms'],
                        "mean_ms": news_search_stats['mean_latency_ms']
                    },
                    "wiki": {
                        "p95_ms": wiki_search_stats['p95_latency_ms'],
                        "p99_ms": wiki_search_stats['p99_latency_ms'],
                        "mean_ms": wiki_search_stats['mean_latency_ms']
                    }
                },
                "B_throughput": {
                    "news_indexing_docs_per_sec": news_stats['docs_per_second'],
                    "wiki_indexing_docs_per_sec": wiki_stats['docs_per_second']
                },
                "C_memory": {
                    "news_mb": news_memory['store_size_mb'],
                    "wiki_mb": wiki_memory['store_size_mb']
                }
            },
            "query_count": len(test_queries)
        }
        
        # Save summary
        with open('results/metrics/summary_report.json', 'w') as f:
            json.dump(summary, f, indent=2)
        
        # Print summary
        print("\n📊 ARTIFACT A - SEARCH LATENCY (Response Time)")
        print("   News Dataset:")
        print(f"      Mean:   {news_search_stats['mean_latency_ms']:7.2f} ms")
        print(f"      Median: {news_search_stats['median_latency_ms']:7.2f} ms")
        print(f"      P95:    {news_search_stats['p95_latency_ms']:7.2f} ms ⭐")
        print(f"      P99:    {news_search_stats['p99_latency_ms']:7.2f} ms ⭐")
        print("\n   Wiki Dataset:")
        print(f"      Mean:   {wiki_search_stats['mean_latency_ms']:7.2f} ms")
        print(f"      Median: {wiki_search_stats['median_latency_ms']:7.2f} ms")
        print(f"      P95:    {wiki_search_stats['p95_latency_ms']:7.2f} ms ⭐")
        print(f"      P99:    {wiki_search_stats['p99_latency_ms']:7.2f} ms ⭐")
        
        print("\n⚡ ARTIFACT B - THROUGHPUT (Indexing Speed)")
        print(f"   News indexing: {news_stats['docs_per_second']:8,.2f} docs/sec")
        print(f"   Wiki indexing: {wiki_stats['docs_per_second']:8,.2f} docs/sec")
        
        print("\n💾 ARTIFACT C - MEMORY FOOTPRINT (Index Size)")
        print(f"   News index:    {news_memory['store_size_mb']:8.2f} MB")
        print(f"   Wiki index:    {wiki_memory['store_size_mb']:8.2f} MB")
        print(f"   Total:         {news_memory['store_size_mb'] + wiki_memory['store_size_mb']:8.2f} MB")
        
        print("\n" + "="*70)
        print("✓ ACTIVITY 1 COMPLETE!")
        print("="*70)
        
        print("\n📁 All results saved to:")
        print("   results/metrics/")
        print("      ├── es_news_indexing_stats.json")
        print("      ├── es_news_search_stats.json")
        print("      ├── es_news_memory.json")
        print("      ├── es_wiki_indexing_stats.json")
        print("      ├── es_wiki_search_stats.json")
        print("      ├── es_wiki_memory.json")
        print("      └── summary_report.json")
        
        print("\n💡 Next steps:")
        print("   1. Review the metrics in results/metrics/")
        print("   2. Check frequency plots in results/plots/")
        print("   3. Commit your code to GitHub")
        print("   4. Start Activity 2 (SelfIndex-v1.0)")
        
        print("\n" + "="*70)
        
        return 0
        
    except FileNotFoundError as e:
        print(f"\n❌ Error: Required file not found")
        print(f"   {e}")
        print("\n💡 Make sure you've run:")
        print("   python src/utils/data_downloader.py")
        return 1
        
    except ConnectionError as e:
        print(f"\n❌ Error: Cannot connect to Elasticsearch")
        print(f"   {e}")
        print("\n💡 Try:")
        print("   python docker_setup.py start")
        return 1
        
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        print("\n💡 For debugging:")
        print("   1. Check Docker: python docker_setup.py status")
        print("   2. Check logs: python docker_setup.py logs")
        print("   3. Check data files exist in data/raw/")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)