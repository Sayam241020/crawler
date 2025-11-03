"""Test Elasticsearch connection"""
from elasticsearch import Elasticsearch

try:
    # Connect to Elasticsearch
    es = Elasticsearch(['http://localhost:9200'])
    
    # Get cluster info
    info = es.info()
    
    print("=" * 60)
    print("✓ ELASTICSEARCH CONNECTION SUCCESSFUL!")
    print("=" * 60)
    print(f"Cluster Name: {info['cluster_name']}")
    print(f"Version: {info['version']['number']}")
    print(f"Lucene Version: {info['version']['lucene_version']}")
    print(f"Build: {info['version']['build_type']}")
    print("=" * 60)
    print("Status: Ready to build ESIndex-v1.0!")
    print("=" * 60)
    
except Exception as e:
    print("=" * 60)
    print("✗ CONNECTION FAILED")
    print("=" * 60)
    print(f"Error: {e}")
    print("\nMake sure Elasticsearch is running on http://localhost:9200")
    print("=" * 60)
