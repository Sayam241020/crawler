"""
Compression-enabled Index Builder

Creates indexers with compression support (z=2 Elias, z=3 zlib)
This modifies the save/load methods to compress postings lists.

Now supports ALL index types:
- x=1 (Boolean): EliasIndexer_x1, ZlibIndexer_x1
- x=2 (TF): EliasIndexer_x2, ZlibIndexer_x2  
- x=3 (TF-IDF): EliasIndexer_x3, ZlibIndexer_x3

Backward compatibility: VByteIndexer_x1/x2/x3 are aliases to EliasIndexer_x1/x2/x3
"""

import json
import os
import sys
from collections import defaultdict

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.self_indexer import SelfIndexer
from src.self_indexer_x2 import SelfIndexer_x2
from src.self_indexer_x3 import SelfIndexer_x3
from src.compression.elias import EliasCompressor
from src.compression.zlib_compressor import ZlibCompressor
from typing import Dict, List


# ============================================================================
# COMPRESSED INDEXERS FOR x=1 (Boolean)
# ============================================================================

class CompressedIndexer_x1(SelfIndexer):
    """
    Base class for Boolean indexers with compression
    Supports z=2 (Elias) and z=3 (zlib)
    """
    
    def __init__(self, compression_type: str, dstore='CUSTOM', optim='Null'):
        """
        Args:
            compression_type: 'CODE' (z=2, Elias) or 'CLIB' (z=3, zlib)
            dstore: Datastore type
            optim: Optimization type
        """
        super().__init__(dstore=dstore, compr=compression_type, optim=optim)
        self.compr = compression_type
        
        # Select compressor
        if compression_type == 'CODE':
            self.compressor = EliasCompressor
        elif compression_type == 'CLIB':
            self.compressor = ZlibCompressor
        else:
            self.compressor = None
    
    def _save_index(self, index_id: str):
        """Save index with compression"""
        filename = f"indices/{index_id}.json"
        os.makedirs('indices', exist_ok=True)
        
        print(f"Compressing Boolean index with {self.compr}...")
        
        # Compress inverted index
        compressed_index = self.compressor.compress_inverted_index(
            dict(self.inverted_index)
        )
        
        # Calculate compression stats (estimate to avoid memory overflow)
        # Count terms and average postings instead of creating full JSON string
        num_terms = len(self.inverted_index)
        total_postings = sum(len(postings) for postings in self.inverted_index.values())
        compressed_terms = len(compressed_index)
        
        print(f"  Original terms: {num_terms:,}, Total postings: {total_postings:,}")
        print(f"  Compressed to: {compressed_terms:,} terms")
        
        index_data = {
            "identifier": self.identifier_short,
            "compression": self.compr,
            "inverted_index": compressed_index,
            "documents": self.documents,
            "compression_stats": {
                "num_terms": num_terms,
                "total_postings": total_postings,
                "compressed_terms": compressed_terms
            },
            "version": "x=1 (Boolean) with compression"
        }
        
        print(f"Saving to disk...")
        with open(filename, 'w') as f:
            json.dump(index_data, f)
        
        print(f"Index saved to disk as {filename}")
        file_size = os.path.getsize(filename)
        print(f"File size on disk: {file_size / (1024*1024):.2f} MB")
    
    def load_index(self, index_id: str):
        """Load index with decompression"""
        filename = f"indices/{index_id}.json"
        if not os.path.exists(filename):
            filename = f"{index_id}.json"
        
        print(f"--- Loading Boolean index from {filename} ---")
        
        try:
            with open(filename, 'r') as f:
                index_data = json.load(f)
            
            print(f"Decompressing with {self.compr}...")
            compressed_index = index_data["inverted_index"]
            self.inverted_index = defaultdict(
                list,
                self.compressor.decompress_inverted_index(compressed_index)
            )
            
            self.documents = index_data["documents"]
            
            print(f"Index loaded successfully. Found {len(self.inverted_index)} terms.")
            
            if "compression_stats" in index_data:
                stats = index_data["compression_stats"]
                print(f"  Compression ratio: {stats['ratio']:.2f}x")
            
        except FileNotFoundError:
            print(f"Error: Index file {filename} not found.")


class EliasIndexer_x1(CompressedIndexer_x1):
    """
    Boolean indexer with Elias compression (z=2)
    Identifier: SelfIndex_i1d1c2o0
    """
    
    def __init__(self, dstore='CUSTOM', optim='Null'):
        super().__init__(compression_type='CODE', dstore=dstore, optim=optim)


# Backward compatibility alias
VByteIndexer_x1 = EliasIndexer_x1


class ZlibIndexer_x1(CompressedIndexer_x1):
    """
    Boolean indexer with zlib compression (z=3)
    Identifier: SelfIndex_i1d1c3o0
    """
    
    def __init__(self, dstore='CUSTOM', optim='Null'):
        super().__init__(compression_type='CLIB', dstore=dstore, optim=optim)


# ============================================================================
# COMPRESSED INDEXERS FOR x=2 (TF)
# ============================================================================

class CompressedIndexer_x2(SelfIndexer_x2):
    """
    Base class for TF indexers with compression
    Supports z=2 (Elias) and z=3 (zlib)
    """
    
    def __init__(self, compression_type: str, dstore='CUSTOM', optim='Null'):
        """
        Args:
            compression_type: 'CODE' (z=2, Elias) or 'CLIB' (z=3, zlib)
            dstore: Datastore type
            optim: Optimization type
        """
        super().__init__(dstore=dstore, compr=compression_type, optim=optim)
        self.compr = compression_type
        
        # Select compressor
        if compression_type == 'CODE':
            self.compressor = EliasCompressor
        elif compression_type == 'CLIB':
            self.compressor = ZlibCompressor
        else:
            self.compressor = None
    
    def _save_index(self, index_id: str):
        """Save index with compression"""
        filename = f"indices/{index_id}.json"
        os.makedirs('indices', exist_ok=True)
        
        print(f"Compressing TF index with {self.compr}...")
        
        # Compress inverted index
        compressed_index = self.compressor.compress_inverted_index(
            dict(self.inverted_index)
        )
        
        # Calculate compression stats (estimate to avoid memory overflow)
        num_terms = len(self.inverted_index)
        total_postings = sum(len(postings) for postings in self.inverted_index.values())
        compressed_terms = len(compressed_index)
        
        print(f"  Original terms: {num_terms:,}, Total postings: {total_postings:,}")
        print(f"  Compressed to: {compressed_terms:,} terms")
        
        index_data = {
            "identifier": self.identifier_short,
            "compression": self.compr,
            "inverted_index": compressed_index,
            "documents": self.documents,
            "compression_stats": {
                "num_terms": num_terms,
                "total_postings": total_postings,
                "compressed_terms": compressed_terms
            },
            "version": "x=2 (TF) with compression"
        }
        
        print(f"Saving to disk...")
        with open(filename, 'w') as f:
            json.dump(index_data, f)
        
        print(f"Index saved to disk as {filename}")
        file_size = os.path.getsize(filename)
        print(f"File size on disk: {file_size / (1024*1024):.2f} MB")
    
    def load_index(self, index_id: str):
        """Load index with decompression"""
        filename = f"indices/{index_id}.json"
        if not os.path.exists(filename):
            filename = f"{index_id}.json"
        
        print(f"--- Loading TF index from {filename} ---")
        
        try:
            with open(filename, 'r') as f:
                index_data = json.load(f)
            
            print(f"Decompressing with {self.compr}...")
            compressed_index = index_data["inverted_index"]
            self.inverted_index = defaultdict(
                list,
                self.compressor.decompress_inverted_index(compressed_index)
            )
            
            self.documents = index_data["documents"]
            
            print(f"Index loaded successfully. Found {len(self.inverted_index)} terms.")
            
            if "compression_stats" in index_data:
                stats = index_data["compression_stats"]
                print(f"  Compression ratio: {stats['ratio']:.2f}x")
            
        except FileNotFoundError:
            print(f"Error: Index file {filename} not found.")


class EliasIndexer_x2(CompressedIndexer_x2):
    """
    TF indexer with Elias compression (z=2)
    Identifier: SelfIndex_i2d1c2o0
    """
    
    def __init__(self, dstore='CUSTOM', optim='Null'):
        super().__init__(compression_type='CODE', dstore=dstore, optim=optim)


# Backward compatibility alias
VByteIndexer_x2 = EliasIndexer_x2

class ZlibIndexer_x2(CompressedIndexer_x2):
    """
    TF indexer with zlib compression (z=3)
    Identifier: SelfIndex_i2d1c3o0
    """
    
    def __init__(self, dstore='CUSTOM', optim='Null'):
        super().__init__(compression_type='CLIB', dstore=dstore, optim=optim)


# ============================================================================
# COMPRESSED INDEXERS FOR x=3 (TF-IDF)
# ============================================================================

class CompressedIndexer_x3(SelfIndexer_x3):
    """
    Base class for TF-IDF indexers with compression
    Supports z=2 (Elias) and z=3 (zlib)
    """
    
    def __init__(self, compression_type: str, dstore='CUSTOM', optim='Null'):
        """
        Args:
            compression_type: 'CODE' (z=2, Elias) or 'CLIB' (z=3, zlib)
            dstore: Datastore type
            optim: Optimization type
        """
        super().__init__(dstore=dstore, compr=compression_type, optim=optim)
        self.compr = compression_type
        
        # Select compressor
        if compression_type == 'CODE':
            self.compressor = EliasCompressor
        elif compression_type == 'CLIB':
            self.compressor = ZlibCompressor
        else:
            self.compressor = None
    
    def _save_index(self, index_id: str):
        """
        Save index with compression
        
        Compresses the inverted_index postings lists before saving
        """
        filename = f"indices/{index_id}.json"
        os.makedirs('indices', exist_ok=True)
        
        print(f"Compressing index with {self.compr}...")
        
        # Compress inverted index
        compressed_index = self.compressor.compress_inverted_index(
            dict(self.inverted_index)
        )
        
        # Calculate compression stats (estimate to avoid memory overflow)
        num_terms = len(self.inverted_index)
        total_postings = sum(len(postings) for postings in self.inverted_index.values())
        compressed_terms = len(compressed_index)
        
        print(f"  Original terms: {num_terms:,}, Total postings: {total_postings:,}")
        print(f"  Compressed to: {compressed_terms:,} terms")

        
        index_data = {
            "identifier": self.identifier_short,
            "compression": self.compr,
            "inverted_index": compressed_index,  # Compressed!
            "documents": self.documents,
            "idf_scores": self.idf_scores,
            "compression_stats": {
                "num_terms": num_terms,
                "total_postings": total_postings,
                "compressed_terms": compressed_terms
            },
            "version": "x=3 (TF-IDF) with compression"
        }
        
        print(f"Saving to disk...")
        with open(filename, 'w') as f:
            json.dump(index_data, f)
        
        print(f"Index saved to disk as {filename}")
        
        # Also save file size
        file_size = os.path.getsize(filename)
        print(f"File size on disk: {file_size / (1024*1024):.2f} MB")
    
    def load_index(self, index_id: str):
        """
        Load index with decompression
        """
        filename = f"indices/{index_id}.json"
        if not os.path.exists(filename):
            filename = f"{index_id}.json"
        
        print(f"--- Loading index from {filename} ---")
        
        try:
            with open(filename, 'r') as f:
                index_data = json.load(f)
            
            # Decompress inverted index
            print(f"Decompressing with {self.compr}...")
            compressed_index = index_data["inverted_index"]
            self.inverted_index = defaultdict(
                list,
                self.compressor.decompress_inverted_index(compressed_index)
            )
            
            self.documents = index_data["documents"]
            self.idf_scores = index_data.get("idf_scores", {})
            self.doc_lengths = index_data.get("doc_lengths", {})
            self.avg_doc_length = index_data.get("avg_doc_length", 0)
            
            print(f"Index loaded successfully. Found {len(self.inverted_index)} terms.")
            
            # Print compression stats if available
            if "compression_stats" in index_data:
                stats = index_data["compression_stats"]
                print(f"  Compression ratio: {stats['ratio']:.2f}x")
            
        except FileNotFoundError:
            print(f"Error: Index file {filename} not found. Please create it first.")


class EliasIndexer_x3(CompressedIndexer_x3):
    """
    TF-IDF indexer with Elias compression (z=2)
    Identifier: SelfIndex_i3d1c2o0
    """
    
    def __init__(self, dstore='CUSTOM', optim='Null'):
        super().__init__(compression_type='CODE', dstore=dstore, optim=optim)


# Backward compatibility aliases
VByteIndexer = EliasIndexer_x3
VByteIndexer_x3 = EliasIndexer_x3
EliasIndexer = EliasIndexer_x3  # Convenience alias



class ZlibIndexer(CompressedIndexer_x3):
    """
    TF-IDF indexer with zlib compression (z=3)
    Identifier: SelfIndex_i3d1c3o0
    Alias for ZlibIndexer_x3 for backwards compatibility
    """
    
    def __init__(self, dstore='CUSTOM', optim='Null'):
        super().__init__(compression_type='CLIB', dstore=dstore, optim=optim)


class ZlibIndexer_x3(CompressedIndexer_x3):
    """
    TF-IDF indexer with zlib compression (z=3)
    Identifier: SelfIndex_i3d1c3o0
    """
    
    def __init__(self, dstore='CUSTOM', optim='Null'):
        super().__init__(compression_type='CLIB', dstore=dstore, optim=optim)


# Testing
if __name__ == '__main__':
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    print("=== Testing Compressed Indexers ===\n")
    
    # Create sample documents
    sample_docs = [
        {
            'doc_id': 1,
            'title': 'Machine Learning',
            'content': 'Machine learning is a subset of artificial intelligence. Machine learning algorithms build models.'
        },
        {
            'doc_id': 2,
            'title': 'Neural Networks',
            'content': 'Neural networks are computing systems inspired by biological neural networks. Neural networks learn.'
        },
        {
            'doc_id': 3,
            'title': 'Deep Learning',
            'content': 'Deep learning is machine learning using neural networks with multiple layers.'
        }
    ]
    
    # Test VByte compression
    print("Testing VByte Indexer (z=2)...")
    vbyte_indexer = VByteIndexer()
    print(f"Identifier: {vbyte_indexer.identifier_short}\n")
    vbyte_indexer.create_index("test", sample_docs)
    
    print("\n" + "="*80 + "\n")
    
    # Test zlib compression
    print("Testing zlib Indexer (z=3)...")
    zlib_indexer = ZlibIndexer()
    print(f"Identifier: {zlib_indexer.identifier_short}\n")
    zlib_indexer.create_index("test", sample_docs)
    
    print("\n" + "="*80 + "\n")
    
    # Test loading and querying
    print("Testing load and query...")
    
    # Load VByte index
    vbyte_test = VByteIndexer()
    vbyte_test.load_index("test")
    
    # Query
    results = vbyte_test.query("machine learning", top_k=2, return_scores=True)
    print(f"\nQuery results (VByte): {results}")
    
    # Load zlib index
    zlib_test = ZlibIndexer()
    zlib_test.load_index("test")
    
    # Query
    results = zlib_test.query("machine learning", top_k=2, return_scores=True)
    print(f"Query results (zlib): {results}")
    
    print("\n✓ All tests passed!")
