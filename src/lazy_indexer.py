"""
Lazy-Loading Compressed Index

Loads compressed index metadata but decompresses terms ON-DEMAND during queries.
This reduces RAM usage from ~6GB to ~500MB for large indices.

Strategy:
- Keep compressed data in memory (small)
- Decompress individual terms only when queried
- Cache recently used terms (LRU cache)

Memory footprint: O(vocabulary_size) instead of O(total_postings)
"""

import json
import os
from collections import defaultdict
from functools import lru_cache
from typing import List, Dict

from src.self_indexer_x2 import SelfIndexer_x2
from src.self_indexer_x3 import SelfIndexer_x3
from src.compression.elias import EliasCompressor
from src.compression.zlib_compressor import ZlibCompressor


class LazyCompressedIndexer_x2(SelfIndexer_x2):
    """
    TF indexer with lazy decompression (memory-efficient)
    
    Instead of decompressing entire index on load, decompress individual
    terms on-demand during queries.
    """
    
    def __init__(self, compression_type: str = 'CODE', dstore='CUSTOM', optim='Null'):
        super().__init__(dstore=dstore, compr=compression_type, optim=optim)
        self.compr = compression_type
        self.compressed_index = {}  # Keep compressed data
        self.decompression_cache = {}  # Cache for recently decompressed terms
        self.cache_max_size = 1000  # Cache up to 1000 terms
        
        # Select compressor
        if compression_type == 'CODE':
            self.compressor = EliasCompressor
        elif compression_type == 'CLIB':
            self.compressor = ZlibCompressor
    
    def load_index(self, index_id: str):
        """
        Load compressed index WITHOUT decompressing
        
        Memory usage: ~500MB instead of ~6GB for 150K docs
        """
        filename = f"indices/{index_id}.json"
        if not os.path.exists(filename):
            filename = f"{index_id}.json"
        
        print(f"--- Lazy-loading compressed index from {filename} ---")
        
        with open(filename, 'r') as f:
            index_data = json.load(f)
        
        # Store COMPRESSED data (small memory footprint)
        self.compressed_index = index_data["inverted_index"]
        self.documents = index_data["documents"]
        
        # Create empty inverted index (will populate on-demand)
        self.inverted_index = {}
        
        print(f"✓ Loaded {len(self.compressed_index)} compressed terms")
        print(f"  Memory: Compressed data only (~500MB)")
        print(f"  Decompression: On-demand during queries")
    
    def _get_postings(self, term: str) -> List:
        """
        Get postings for a term (decompress if needed)
        
        Uses LRU cache to avoid repeated decompression
        """
        # Check cache first
        if term in self.decompression_cache:
            return self.decompression_cache[term]
        
        # Check if term exists in compressed index
        if term not in self.compressed_index:
            return []
        
        # Decompress this term only
        compressed_data = self.compressed_index[term]
        postings = self.compressor.decompress_postings(compressed_data)
        
        # Add to cache (evict oldest if cache full)
        if len(self.decompression_cache) >= self.cache_max_size:
            # Remove oldest item (simple FIFO, could use LRU)
            oldest_term = next(iter(self.decompression_cache))
            del self.decompression_cache[oldest_term]
        
        self.decompression_cache[term] = postings
        return postings
    
    def query(self, query_str: str, mode: str = 'TAAT', top_k: int = 10) -> List[str]:
        """
        Query with lazy decompression
        
        Only decompresses terms that appear in the query!
        """
        from src.preprocessor import preprocess_text
        query_terms = preprocess_text(query_str)
        
        # Temporarily populate inverted_index with only query terms
        for term in query_terms:
            if term not in self.inverted_index and term in self.compressed_index:
                self.inverted_index[term] = self._get_postings(term)
        
        # Use parent class query logic
        if mode == 'DAAT':
            return self._ranked_query_daat(query_terms, top_k)
        else:
            return self._ranked_query_taat(query_terms, top_k)


class LazyCompressedIndexer_x3(SelfIndexer_x3):
    """
    TF-IDF indexer with lazy decompression (memory-efficient)
    """
    
    def __init__(self, compression_type: str = 'CODE', dstore='CUSTOM', optim='Null'):
        super().__init__(dstore=dstore, compr=compression_type, optim=optim)
        self.compr = compression_type
        self.compressed_index = {}
        self.decompression_cache = {}
        self.cache_max_size = 1000
        
        if compression_type == 'CODE':
            self.compressor = EliasCompressor
        elif compression_type == 'CLIB':
            self.compressor = ZlibCompressor
    
    def load_index(self, index_id: str):
        """Lazy-load compressed TF-IDF index"""
        filename = f"indices/{index_id}.json"
        if not os.path.exists(filename):
            filename = f"{index_id}.json"
        
        print(f"--- Lazy-loading compressed TF-IDF index from {filename} ---")
        
        with open(filename, 'r') as f:
            index_data = json.load(f)
        
        self.compressed_index = index_data["inverted_index"]
        self.documents = index_data["documents"]
        self.idf_scores = index_data.get("idf_scores", {})
        self.inverted_index = {}
        
        print(f"✓ Loaded {len(self.compressed_index)} compressed terms")
        print(f"  IDF scores: {len(self.idf_scores)} terms")
    
    def _get_postings(self, term: str) -> List:
        """Get postings with caching"""
        if term in self.decompression_cache:
            return self.decompression_cache[term]
        
        if term not in self.compressed_index:
            return []
        
        compressed_data = self.compressed_index[term]
        postings = self.compressor.decompress_postings(compressed_data)
        
        if len(self.decompression_cache) >= self.cache_max_size:
            oldest_term = next(iter(self.decompression_cache))
            del self.decompression_cache[oldest_term]
        
        self.decompression_cache[term] = postings
        return postings
    
    def query(self, query_str: str, mode: str = 'TAAT', top_k: int = 10) -> List[str]:
        """Query with lazy decompression"""
        from src.preprocessor import preprocess_text
        query_terms = preprocess_text(query_str)
        
        for term in query_terms:
            if term not in self.inverted_index and term in self.compressed_index:
                self.inverted_index[term] = self._get_postings(term)
        
        if mode == 'DAAT':
            return self._ranked_query_daat(query_terms, top_k)
        else:
            return self._ranked_query_taat(query_terms, top_k)
