"""
Compression module for postings lists

Provides two compression methods:
- Elias Gamma/Delta encoding (z=2) - Custom implementation
- Zlib compression (z=3) - Library-based
"""

from .elias import EliasCompressor
from .zlib_compressor import ZlibCompressor

__all__ = ['EliasCompressor', 'ZlibCompressor']
