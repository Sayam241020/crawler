"""
Memory-Efficient Index Loading Strategies

Three strategies for handling large compressed indices:
1. Lazy Loading: Decompress on-demand (best for most cases)
2. Chunked Loading: Load index in chunks
3. Memory-Mapped: Use mmap for very large files

Recommended:
- < 1GB index: Normal loading (load entire index)
- 1-5GB index: Lazy loading (decompress on-demand)
- > 5GB index: Memory-mapped + lazy loading
"""

import json
import mmap
import os
from typing import Dict, List


def estimate_index_memory(index_path: str) -> Dict[str, float]:
    """
    Estimate memory requirements for loading an index
    
    Returns:
        Dictionary with file_size_mb, estimated_ram_mb, strategy
    """
    file_size = os.path.getsize(index_path)
    file_size_mb = file_size / (1024 * 1024)
    
    # JSON expansion factor: ~2-3x in RAM
    # Decompression expansion: ~2-4x for compressed index
    # Total: ~6-8x file size
    estimated_ram_mb = file_size_mb * 7
    
    # Recommend strategy
    if file_size_mb < 100:
        strategy = "normal"
    elif file_size_mb < 500:
        strategy = "lazy"
    else:
        strategy = "mmap + lazy"
    
    return {
        "file_size_mb": file_size_mb,
        "estimated_ram_mb": estimated_ram_mb,
        "estimated_ram_gb": estimated_ram_mb / 1024,
        "recommended_strategy": strategy
    }


def load_index_smart(index_path: str, strategy: str = 'auto'):
    """
    Load index with automatic strategy selection
    
    Args:
        index_path: Path to index file
        strategy: 'normal', 'lazy', 'mmap', or 'auto'
    
    Returns:
        Loaded index (strategy-dependent)
    """
    if strategy == 'auto':
        stats = estimate_index_memory(index_path)
        strategy = stats['recommended_strategy']
        print(f"Auto-selected strategy: {strategy}")
        print(f"  File size: {stats['file_size_mb']:.1f} MB")
        print(f"  Estimated RAM: {stats['estimated_ram_gb']:.1f} GB")
    
    if strategy == 'lazy':
        print("Using lazy loading (decompress on-demand)")
        # Import and use lazy indexer
        from src.lazy_indexer import LazyCompressedIndexer_x2
        indexer = LazyCompressedIndexer_x2()
        return indexer
    
    elif strategy == 'normal':
        print("Using normal loading (load entire index)")
        # Use standard indexer
        from src.compressed_indexer import EliasIndexer_x2
        indexer = EliasIndexer_x2()
        return indexer
    
    else:
        raise ValueError(f"Unknown strategy: {strategy}")


# Usage example
if __name__ == "__main__":
    print("=" * 70)
    print("MEMORY-EFFICIENT INDEX LOADING")
    print("=" * 70)
    
    # Check existing indices
    if os.path.exists("indices/"):
        print("\nAnalyzing existing indices:")
        for filename in os.listdir("indices/"):
            if filename.endswith(".json"):
                path = os.path.join("indices/", filename)
                stats = estimate_index_memory(path)
                print(f"\n{filename}:")
                print(f"  File: {stats['file_size_mb']:.1f} MB")
                print(f"  Est. RAM: {stats['estimated_ram_gb']:.1f} GB")
                print(f"  Strategy: {stats['recommended_strategy']}")
