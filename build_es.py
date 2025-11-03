"""
Build Elasticsearch Index (ESIndex-v1.0)
Creates and indexes 100K documents into Elasticsearch
"""

import os
import sys
import time
import argparse
from elasticsearch import Elasticsearch
from src.es_indexer import ESIndexer
from src.data_loader import load_wiki_data, load_news_data


def connect_to_elasticsearch(host='localhost', port=9200):
    """
    Connect to Elasticsearch server.
    
    Args:
        host: Elasticsearch host (default: localhost)
        port: Elasticsearch port (default: 9200)
        
    Returns:
        Elasticsearch client instance
    """
    try:
        es = Elasticsearch([f"http://{host}:{port}"], request_timeout=30)
        
        # Test connection
        if es.ping():
            info = es.info()
            print(f"✓ Connected to Elasticsearch {info['version']['number']}")
            return es
        else:
            print("✗ Could not connect to Elasticsearch")
            print(f"  Make sure Elasticsearch is running on {host}:{port}")
            sys.exit(1)
    except Exception as e:
        print(f"✗ Error connecting to Elasticsearch: {e}")
        print(f"  Make sure Elasticsearch is running on {host}:{port}")
        sys.exit(1)


def load_documents():
    """
    Load ORIGINAL (unprocessed) documents from raw data sources.
    
    This loads the same 100K documents that were used for SelfIndex,
    but in their ORIGINAL form (before tokenization/stemming).
    
    Elasticsearch will apply its own preprocessing pipeline:
    - Lowercase
    - Stop word removal  
    - Stemming (using English stemmer)
    
    Returns:
        Generator yielding document dictionaries with original text
    """
    print("Loading ORIGINAL (unprocessed) data...")
    print("=" * 70)
    
    # Load raw Wikipedia data (50K documents)
    wiki_dir = 'data/wiki'
    print(f"Loading Wikipedia data from: {wiki_dir}")
    wiki_docs = []
    for doc in load_wiki_data(wiki_dir, limit=50000):
        wiki_docs.append(doc)
    print(f"✓ Loaded {len(wiki_docs):,} Wikipedia documents")
    
    # Load raw News data (50K documents)
    news_dir = 'data/News_Datasets'
    print(f"Loading News data from: {news_dir}")
    news_docs = []
    for doc in load_news_data(news_dir, limit=50000):
        news_docs.append(doc)
    print(f"✓ Loaded {len(news_docs):,} News documents")
    
    total_docs = len(wiki_docs) + len(news_docs)
    print(f"✓ Total documents: {total_docs:,}")
    print("=" * 70)
    print("NOTE: Using ORIGINAL text (not preprocessed tokens)")
    print("      Elasticsearch will apply its own preprocessing")
    print("=" * 70)
    
    # Yield all documents
    for doc in wiki_docs:
        yield doc
    for doc in news_docs:
        yield doc


def build_es_index(es_host='localhost', es_port=9200, index_name='esindex-v1.0', force=False):
    """
    Build Elasticsearch index with 100K documents.
    
    Args:
        es_host: Elasticsearch host
        es_port: Elasticsearch port
        index_name: Name of the index to create
        force: Force rebuild if index exists
    """
    print("=" * 70)
    print("ELASTICSEARCH INDEX BUILD")
    print("=" * 70)
    print(f"Index name: {index_name}")
    print(f"ES Server: {es_host}:{es_port}")
    print()
    
    # Connect to Elasticsearch
    es_client = connect_to_elasticsearch(es_host, es_port)
    
    # Check if index already exists
    if es_client.indices.exists(index=index_name):
        if not force:
            print(f"⚠ Index '{index_name}' already exists")
            print("  Use --force to rebuild")
            
            # Show index stats
            stats = es_client.cat.indices(index=index_name, format="json", h="docs.count,store.size")
            if stats:
                print(f"  Current stats: {stats[0]['docs.count']} documents, {stats[0]['store.size']}")
            sys.exit(0)
        else:
            print(f"⚠ Force rebuild enabled - will delete existing index")
    
    # Create indexer
    indexer = ESIndexer(es_client)
    
    # Load documents
    print()
    print("Loading documents...")
    print("-" * 70)
    documents = list(load_documents())
    
    # Build index
    print()
    print("Building index...")
    print("-" * 70)
    start_time = time.time()
    
    indexer.create_index(index_name, documents)
    
    end_time = time.time()
    elapsed = end_time - start_time
    
    # Show final stats
    print()
    print("=" * 70)
    print("BUILD COMPLETE")
    print("=" * 70)
    
    try:
        stats = es_client.cat.indices(index=index_name, format="json", h="docs.count,store.size")
        if stats:
            doc_count = stats[0]['docs.count']
            size = stats[0]['store.size']
            print(f"Documents indexed: {doc_count}")
            print(f"Index size: {size}")
            print(f"Time taken: {elapsed:.2f} seconds")
            print(f"Throughput: {int(doc_count) / elapsed:.1f} docs/sec")
    except Exception as e:
        print(f"Could not retrieve final stats: {e}")
    
    print()
    print(f"✓ Index '{index_name}' is ready for querying")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Build Elasticsearch index for IRE Assignment',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python build_es.py                          # Build with defaults
  python build_es.py --force                  # Force rebuild
  python build_es.py --host 192.168.1.100     # Use remote ES server
  python build_es.py --index ESIndex-test     # Use custom index name
        """
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
        '--force',
        action='store_true',
        help='Force rebuild if index exists'
    )
    
    args = parser.parse_args()
    
    try:
        build_es_index(
            es_host=args.host,
            es_port=args.port,
            index_name=args.index,
            force=args.force
        )
    except KeyboardInterrupt:
        print("\n✗ Build interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Build failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
