"""
zlib-based Compression for Inverted Index

Uses Python's built-in zlib library (which wraps the zlib C library)
for compressing postings lists.

zlib provides:
- Good compression ratios
- Fast decompression
- Battle-tested reliability
- Part of Python standard library

This is the "off-the-shelf library" option (z=3) as per assignment.
"""

import zlib
import json
import base64
from typing import List, Any, Dict


class ZlibCompressor:
    """
    zlib-based compression for postings lists
    
    Compresses the JSON representation of postings lists using zlib.
    Provides better compression than VByte but with more CPU overhead.
    """
    
    # Compression level (1-9, where 9 is maximum compression)
    DEFAULT_COMPRESSION_LEVEL = 6  # Good balance of speed vs compression
    MAX_COMPRESSION_LEVEL = 9      # Best compression (slower)
    
    @staticmethod
    def compress_postings_list(postings: List[List[Any]], 
                               compression_level: int = DEFAULT_COMPRESSION_LEVEL) -> bytes:
        """
        Compress a postings list using zlib
        
        Args:
            postings: List of [doc_id, positions] pairs
            compression_level: zlib compression level (1-9)
            
        Returns:
            bytes: Compressed representation
        """
        if not postings:
            return b''
        
        # Convert to JSON bytes
        json_str = json.dumps(postings, separators=(',', ':'))  # Compact JSON
        json_bytes = json_str.encode('utf-8')
        
        # Compress with zlib
        compressed = zlib.compress(json_bytes, level=compression_level)
        
        return compressed
    
    @staticmethod
    def decompress_postings_list(data: bytes) -> List[List[Any]]:
        """
        Decompress a postings list
        
        Args:
            data: Compressed byte array
            
        Returns:
            List of [doc_id, positions] pairs
        """
        if not data:
            return []
        
        # Decompress with zlib
        decompressed_bytes = zlib.decompress(data)
        
        # Parse JSON
        json_str = decompressed_bytes.decode('utf-8')
        postings = json.loads(json_str)
        
        return postings
    
    @staticmethod
    def decompress_postings(data) -> List[List[Any]]:
        """
        Alias for decompress_postings_list (for compatibility with lazy_indexer)
        
        Handles both bytes and base64-encoded strings (from JSON storage)
        
        Args:
            data: Compressed byte array OR base64 string
            
        Returns:
            List of [doc_id, positions] pairs
        """
        # If data is a string (base64 from JSON), decode it first
        if isinstance(data, str):
            data = base64.b64decode(data)
        
        return ZlibCompressor.decompress_postings_list(data)
    
    @staticmethod
    def compress_inverted_index(inverted_index: Dict[str, List], 
                                compression_level: int = DEFAULT_COMPRESSION_LEVEL) -> Dict[str, str]:
        """
        Compress an entire inverted index
        
        Args:
            inverted_index: Dictionary of term -> postings list
            compression_level: zlib compression level (1-9)
            
        Returns:
            Dictionary of term -> compressed bytes (as base64 string for JSON)
        """
        compressed = {}
        
        for term, postings in inverted_index.items():
            compressed_bytes = ZlibCompressor.compress_postings_list(
                postings, 
                compression_level
            )
            # Convert to base64 for JSON serialization
            compressed[term] = base64.b64encode(compressed_bytes).decode('ascii')
        
        return compressed
    
    @staticmethod
    def decompress_inverted_index(compressed_index: Dict[str, str]) -> Dict[str, List]:
        """
        Decompress an entire inverted index
        
        Args:
            compressed_index: Dictionary of term -> compressed bytes (base64)
            
        Returns:
            Dictionary of term -> postings list
        """
        decompressed = {}
        
        for term, compressed_str in compressed_index.items():
            compressed_bytes = base64.b64decode(compressed_str)
            postings = ZlibCompressor.decompress_postings_list(compressed_bytes)
            decompressed[term] = postings
        
        return decompressed
    
    @staticmethod
    def get_compression_stats(inverted_index: Dict[str, List],
                             compression_level: int = DEFAULT_COMPRESSION_LEVEL) -> Dict:
        """
        Get compression statistics for an index
        
        Args:
            inverted_index: Dictionary of term -> postings list
            compression_level: zlib compression level
            
        Returns:
            Dictionary with compression statistics
        """
        # Original size (as JSON)
        original_json = json.dumps(inverted_index)
        original_size = len(original_json.encode('utf-8'))
        
        # Compressed size
        compressed = ZlibCompressor.compress_inverted_index(inverted_index, compression_level)
        compressed_json = json.dumps(compressed)
        compressed_size = len(compressed_json.encode('utf-8'))
        
        return {
            'original_size_bytes': original_size,
            'compressed_size_bytes': compressed_size,
            'compression_ratio': original_size / compressed_size if compressed_size > 0 else 0,
            'space_saved_bytes': original_size - compressed_size,
            'space_saved_percent': ((original_size - compressed_size) / original_size * 100) 
                                   if original_size > 0 else 0,
            'compression_level': compression_level
        }


# Example usage and testing
if __name__ == '__main__':
    print("=== Testing zlib Compression ===\n")
    
    # Test postings list compression
    postings = [
        [1, [0, 5, 10]],
        [3, [2, 7, 15, 20]],
        [5, [1, 3]],
        [100, [0, 1, 2, 3, 4]]
    ]
    
    print("Original postings:", postings)
    
    # Test different compression levels
    for level in [1, 6, 9]:
        compressed = ZlibCompressor.compress_postings_list(postings, level)
        decompressed = ZlibCompressor.decompress_postings_list(compressed)
        
        original_json = json.dumps(postings)
        ratio = len(original_json) / len(compressed)
        
        print(f"\nCompression level {level}:")
        print(f"  Compressed: {len(compressed)} bytes")
        print(f"  Ratio: {ratio:.2f}x")
        print(f"  Match: {postings == decompressed}")
    
    # Test inverted index compression
    print("\n=== Testing Inverted Index Compression ===\n")
    
    sample_index = {
        'machine': [[1, [0, 10]], [5, [3, 8]], [10, [1]]],
        'learning': [[1, [1, 11]], [5, [4, 9]], [10, [2]]],
        'algorithm': [[1, [2]], [8, [0, 5, 10]]],
        'neural': [[5, [0, 5]], [10, [0]]],
        'network': [[5, [1, 6]], [10, [1]]]
    }
    
    # Get compression stats
    stats = ZlibCompressor.get_compression_stats(sample_index, compression_level=6)
    
    print(f"Original size: {stats['original_size_bytes']:,} bytes")
    print(f"Compressed size: {stats['compressed_size_bytes']:,} bytes")
    print(f"Compression ratio: {stats['compression_ratio']:.2f}x")
    print(f"Space saved: {stats['space_saved_bytes']:,} bytes ({stats['space_saved_percent']:.1f}%)")
    
    # Test round-trip
    compressed_index = ZlibCompressor.compress_inverted_index(sample_index)
    decompressed_index = ZlibCompressor.decompress_inverted_index(compressed_index)
    
    print(f"\nRound-trip test: {sample_index == decompressed_index}")
    
    # Compare with VByte if available
    try:
        from .vbyte import VByteCompressor
        
        print("\n=== Comparison: zlib vs VByte ===\n")
        
        # zlib compression
        zlib_compressed = ZlibCompressor.compress_postings_list(postings, 6)
        
        # VByte compression
        vbyte_compressed = VByteCompressor.compress_postings_list(postings)
        
        original_json = json.dumps(postings)
        print(f"Original (JSON): {len(original_json)} bytes")
        print(f"zlib: {len(zlib_compressed)} bytes ({len(original_json)/len(zlib_compressed):.2f}x)")
        print(f"VByte: {len(vbyte_compressed)} bytes ({len(original_json)/len(vbyte_compressed):.2f}x)")
        
        if len(zlib_compressed) < len(vbyte_compressed):
            saving = (len(vbyte_compressed) - len(zlib_compressed)) / len(vbyte_compressed) * 100
            print(f"\nzlib is {saving:.1f}% smaller than VByte")
        else:
            saving = (len(zlib_compressed) - len(vbyte_compressed)) / len(zlib_compressed) * 100
            print(f"\nVByte is {saving:.1f}% smaller than zlib")
            
    except ImportError:
        print("\n(VByte module not available for comparison)")
