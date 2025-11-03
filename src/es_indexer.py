"""
Elasticsearch Indexer for IRE Assignment
Implements IndexBase interface for Elasticsearch integration
"""

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from typing import Iterable, Dict

# Import IndexBase directly to avoid src/__init__.py
import importlib.util
import os
spec = importlib.util.spec_from_file_location(
    "index_base",
    os.path.join(os.path.dirname(__file__), "index_base.py")
)
index_base_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(index_base_module)
IndexBase = index_base_module.IndexBase


class ESIndexer(IndexBase):
    """
    Elasticsearch indexer using TF-IDF ranking with custom English analyzer.
    
    Configuration:
    - core: ESIndex (Elasticsearch-based indexing)
    - info: TFIDF (ranking model)
    - dstore: DB2 (Elasticsearch as datastore)
    - qproc: TERMatat (term-at-a-time query processing)
    - compr: NONE (no compression)
    - optim: Null (no optimization)
    """
    
    def __init__(self, es_client: Elasticsearch):
        """
        Initialize ES indexer with client connection.
        
        Args:
            es_client: Elasticsearch client instance
        """
        self.es = es_client
        super().__init__(
            core='ESIndex',
            info='TFIDF',
            dstore='DB2',
            qproc='TERMatat',
            compr='NONE',
            optim='Null'
        )

    def create_index(self, index_id: str, documents: Iterable[Dict]):
        """
        Create Elasticsearch index with custom English analyzer and bulk index documents.
        
        Args:
            index_id: Name of the index to create (e.g., 'ESIndex-v1.0')
            documents: Iterable of document dictionaries with fields:
                      - doc_id: Unique document identifier (required)
                      - content: Main text content (required)
                      - title: Document title (optional)
                      - source: Document source (wiki/news) (optional)
                      - url: Document URL (optional)
                      - author: Document author (optional)
                      - published_date: Publication date (optional)
        """
        # Define index mapping and settings
        index_body = {
            "mappings": {
                "properties": {
                    "original_id": {"type": "keyword"},
                    "content": {"type": "text", "analyzer": "my_english_analyzer"},
                    "source": {"type": "keyword"},
                    "title": {"type": "text", "analyzer": "my_english_analyzer"},
                    "url": {"type": "keyword"},
                    "author": {"type": "text"},
                    "published_date": {"type": "date"}
                }
            },
            "settings": {
                "analysis": {
                    "analyzer": {
                        "my_english_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "english_stop", "english_stemmer"]
                        }
                    },
                    "filter": {
                        "english_stop": {
                            "type": "stop",
                            "stopwords": "_english_"
                        },
                        "english_stemmer": {
                            "type": "stemmer",
                            "language": "english"
                        }
                    }
                }
            }
        }

        # Delete existing index if it exists
        if self.es.indices.exists(index=index_id):
            self.es.indices.delete(index=index_id)
            print(f"✓ Deleted existing index: {index_id}")

        # Create new index
        self.es.indices.create(index=index_id, body=index_body)
        print(f"✓ Created index '{index_id}' with custom English analyzer")

        # Bulk index documents
        def doc_generator():
            """Generator for bulk indexing"""
            for doc in documents:
                # Extract doc_id and use rest as source
                doc_copy = doc.copy()
                doc_id = doc_copy.pop("doc_id")
                yield {
                    "_index": index_id,
                    "_id": doc_id,
                    "_source": doc_copy
                }

        print(f"Starting bulk indexing...")
        success, failed = bulk(
            self.es,
            doc_generator(),
            chunk_size=500,
            request_timeout=120
        )
        
        print(f"✓ Successfully indexed {success} documents")
        if failed:
            print(f"⚠ Failed to index {len(failed)} documents")

    def query(self, index_id: str, query_text: str, fields: list = None):
        """
        Perform multi-field keyword search using Elasticsearch.
        
        Args:
            index_id: Name of the index to search
            query_text: Query string (already preprocessed)
            fields: Fields to search in (default: ["content", "title"])
            
        Returns:
            List of matching documents with scores
        """
        if fields is None:
            fields = ["content", "title"]
            
        if not self.es.indices.exists(index=index_id):
            print(f"Index '{index_id}' does not exist.")
            return []

        query_body = {
            "query": {
                "multi_match": {
                    "query": query_text,
                    "fields": fields
                }
            }
        }
        
        try:
            response = self.es.search(index=index_id, body=query_body, size=1000)
            return response['hits']['hits']
        except Exception as e:
            print(f"Error during search: {e}")
            return []

    def get_memory_footprint(self, index_id: str) -> str:
        """
        Get disk usage for the specified index.
        
        Args:
            index_id: Name of the index
            
        Returns:
            String representation of index size (e.g., "123.4mb")
        """
        try:
            stats = self.es.cat.indices(index=index_id, format="json", h="store.size")
            if stats:
                return stats[0]['store.size']
            return "N/A"
        except Exception as e:
            print(f"Could not retrieve memory footprint: {e}")
            return "Error"

    # --- Placeholder methods from IndexBase ---
    def load_index(self, serialized_index_dump: str):
        """Not implemented for Elasticsearch (uses native persistence)"""
        pass

    def update_index(self, index_id: str, remove_files: Iterable[Dict], add_files: Iterable[Dict]):
        """Not implemented for this assignment"""
        pass

    def delete_index(self, index_id: str):
        """Delete an Elasticsearch index"""
        if self.es.indices.exists(index=index_id):
            self.es.indices.delete(index=index_id)
            print(f"Deleted index: {index_id}")

    def list_indices(self) -> Iterable[str]:
        """List all Elasticsearch indices"""
        try:
            indices = self.es.cat.indices(format="json", h="index")
            return [idx['index'] for idx in indices]
        except Exception as e:
            print(f"Error listing indices: {e}")
            return []

    def list_indexed_files(self, index_id: str) -> Iterable[str]:
        """Not implemented for this assignment"""
        return []
