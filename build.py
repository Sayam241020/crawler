#!/usr/bin/env python
"""
Build Script for SelfIndex following index_base.py specification

ALL 18 COMBINATIONS SUPPORTED:
- 3 index types (x): Boolean (1), TF (2), TF-IDF (3)
- 2 datastores (y): JSON (1), SQLite (2)
- 3 compression methods (z): None (1), Elias (2), zlib (3)

Format: SelfIndex_i{x}d{y}c{z}q{q}o{optim}
- i{x}: IndexInfo (1=BOOLEAN, 2=WORDCOUNT/TF, 3=TFIDF)
- d{y}: DataStore (1=CUSTOM/JSON, 2=DB1/SQLite)
- c{z}: Compression (1=NONE, 2=CODE/Elias, 3=CLIB/zlib)
- q{q}: QueryProc ('T'=TERMatat, 'D'=DOCatat)
- o{optim}: Optimizations ('0'=Null, 'sp'=Skipping, 'th'=Thresholding, 'es'=EarlyStopping)

Usage:
    python build.py -x 1 -y 1 -z 1 -q T -optim 0  # Boolean + JSON + No compression
    python build.py -x 3 -y 2 -z 2 -q T -optim 0  # TF-IDF + SQLite + Elias
    python build.py -x 2 -y 1 -z 3 -q T -optim 0  # TF + JSON + zlib
"""

import sys
import os
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.self_indexer import SelfIndexer
from src.self_indexer_x2 import SelfIndexer_x2
from src.self_indexer_x3 import SelfIndexer_x3

# Compressed indexers for JSON (y=1) with all index types
from src.compressed_indexer import (
    EliasIndexer_x1, ZlibIndexer_x1,
    EliasIndexer_x2, ZlibIndexer_x2,
    EliasIndexer_x3, ZlibIndexer_x3
)

# SQLite indexers (y=2) with compression support
from src.sqlite_indexer import (
    SQLiteIndexer_x1, SQLiteIndexer_x2, SQLiteIndexer_x3
)

from src.data_loader import load_documents, load_documents_preprocessed


def build_index(x, y, z, optim, limit=None, force=False):
    """
    Build index with specified parameters
    ALL 18 COMBINATIONS SUPPORTED!
    
    Args:
        x: Index type (1=Boolean, 2=TF, 3=TF-IDF)
        y: Datastore (1=JSON, 2=SQLite)
        z: Compression (1=None, 2=Elias, 3=zlib)
        optim: Optimization ('0'=None, 'sp'=Skip, 'th'=Threshold, 'es'=EarlyStop)
        force: Skip rebuild confirmation prompts
        
    Note: Query mode (TAAT vs DAAT) is specified at query time, not build time
    """
    # Map index types to enum names for index_base.py
    index_map = {1: 'BOOLEAN', 2: 'WORDCOUNT', 3: 'TFIDF'}
    datastore_map = {1: 'CUSTOM/JSON', 2: 'DB1/SQLite'}
    compress_map = {1: 'NONE', 2: 'CODE/Elias', 3: 'CLIB/zlib'}
    optim_map = {'0': 'Null', 'sp': 'Skipping', 'th': 'Thresholding', 'es': 'EarlyStopping'}
    
    print(f"\n{'='*80}")
    print(f"Building SelfIndex with configuration:")
    print(f"  Index Type: {index_map[x]} (i={x})")
    print(f"  Datastore: {datastore_map[y]} (d={y})")
    print(f"  Compression: {compress_map[z]} (c={z})")
    print(f"  Optimization: {optim_map[optim]} (o={optim})")
    print(f"  Query Mode: Runtime selectable (TAAT/DAAT)")
    print(f"{'='*80}\n")
    
    # Select compression type string
    compression_type = 'NONE' if z == 1 else ('CODE' if z == 2 else 'CLIB')
    
    # Select indexer class based on x, y, and z
    if y == 1:
        # JSON datastore - supports all compressions
        if x == 1:
            if z == 1:
                indexer = SelfIndexer(optim=optim_map[optim])
            elif z == 2:
                indexer = EliasIndexer_x1(optim=optim_map[optim])
            elif z == 3:
                indexer = ZlibIndexer_x1(optim=optim_map[optim])
        elif x == 2:
            if z == 1:
                indexer = SelfIndexer_x2(optim=optim_map[optim])
            elif z == 2:
                indexer = EliasIndexer_x2(optim=optim_map[optim])
            elif z == 3:
                indexer = ZlibIndexer_x2(optim=optim_map[optim])
        elif x == 3:
            if z == 1:
                indexer = SelfIndexer_x3(optim=optim_map[optim])
            elif z == 2:
                indexer = EliasIndexer_x3(optim=optim_map[optim])
            elif z == 3:
                indexer = ZlibIndexer_x3(optim=optim_map[optim])
        else:
            raise ValueError(f"Invalid index type: {x}")
    
    elif y == 2:
        # SQLite datastore - supports all compressions
        if x == 1:
            indexer = SQLiteIndexer_x1(compression_type)
        elif x == 2:
            indexer = SQLiteIndexer_x2(compression_type)
        elif x == 3:
            indexer = SQLiteIndexer_x3(compression_type)
        else:
            raise ValueError(f"Invalid index type: {x}")
    
    else:
        raise ValueError(f"Invalid datastore: {y}. Must be 1 (JSON) or 2 (SQLite)")
    
    # Create identifier following updated format (no query mode)
    identifier = f"SelfIndex_i{x}d{y}c{z}o{optim}"
    print(f"Index Identifier: {identifier}")
    
    # Check if already exists (for JSON, check .json file; for SQLite, check .db)
    if y == 1:
        index_path = f"indices/{identifier}.json"
        if os.path.exists(index_path):
            if not force:
                print(f"⚠ Index already exists: {index_path}")
                response = input("Rebuild? (y/n): ")
                if response.lower() != 'y':
                    print("Aborted.")
                    return
            else:
                print(f"⚠ Index already exists: {index_path} - Rebuilding (--force)")
    elif y == 2:
        index_path = f"indices/{identifier}.db"
        if os.path.exists(index_path):
            if not force:
                print(f"⚠ SQLite database already exists: {index_path}")
                response = input("Rebuild? (y/n): ")
                if response.lower() != 'y':
                    print("Aborted.")
                    return
            else:
                print(f"⚠ SQLite database already exists: {index_path} - Rebuilding (--force)")
    
    # Load documents (keep as iterator to avoid memory overflow)
    print("\nLoading documents...")
    
    # Always try to use preprocessed cache first (MUCH FASTER!)
    if limit:
        print(f"⚠ TESTING MODE: Limited to {limit:,} documents per source ({limit*2:,} total)")
    
    try:
        # Default to 50000 if no limit specified
        doc_limit = limit if limit else 50000
        documents = load_documents_preprocessed(limit=doc_limit)
    except FileNotFoundError:
        print(f"⚠️  Preprocessed cache not found, falling back to slow method...")
        if limit:
            print(f"   Consider running: python scripts/preprocess_corpus.py --limit {limit}")
            documents = load_documents(limit_per_source=limit)
        else:
            print(f"   Processing documents as stream (may take a while)...")
            documents = load_documents()
    
    # Build index (indexer will iterate and count internally)
    print(f"\nBuilding index...")
    indexer.create_index(index_id=identifier, documents=documents)
    
    print(f"\n✓ Index built successfully: {identifier}")
    if y == 1:
        print(f"  Location: indices/{identifier}.json")
    elif y == 2:
        print(f"  Location: indices/{identifier}.db (SQLite database)")
    
    # Show stats
    if hasattr(indexer, 'stats'):
        stats = indexer.stats()
        print(f"\nIndex Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")


def main():
    parser = argparse.ArgumentParser(
        description="Build SelfIndex following index_base.py specification",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Boolean index with JSON storage
  python build.py -x 1 -y 1 -z 1 -optim 0
  
  # TF index with JSON storage
  python build.py -x 2 -y 1 -z 1 -optim 0
  
  # TF-IDF index with JSON storage
  python build.py -x 3 -y 1 -z 1 -optim 0
  
  # TF-IDF with skip pointers
  python build.py -x 3 -y 1 -z 1 -optim sp
  
  # SQLite with Elias compression
  python build.py -x 3 -y 2 -z 2 -optim 0

Format: SelfIndex_i{x}d{y}c{z}o{i}
  i{x}: 1=Boolean, 2=TF, 3=TF-IDF
  d{y}: 1=CUSTOM/JSON, 2=DB1/SQLite
  c{z}: 1=NONE, 2=CODE/Elias, 3=CLIB/zlib
  o{i}: 0=None, sp=SkipPointers, th=Threshold, es=EarlyStop
  
Note: Query mode (TAAT/DAAT) is selected at runtime, not during build
        """
    )
    
    parser.add_argument('-x', '--index-type', type=int, choices=[1, 2, 3], required=True,
                        help='Index type: 1=Boolean, 2=TF, 3=TF-IDF')
    parser.add_argument('-y', '--datastore', type=int, choices=[1, 2], required=True,
                        help='Datastore: 1=CUSTOM/JSON, 2=DB1/SQLite')
    parser.add_argument('-z', '--compression', type=int, choices=[1, 2, 3], required=True,
                        help='Compression: 1=NONE, 2=CODE/Elias, 3=CLIB/zlib')
    parser.add_argument('-optim', '--optimization', choices=['0', 'sp', 'th', 'es'], required=True,
                        help='Optimization: 0=None, sp=Skip, th=Threshold, es=EarlyStop')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit documents per source (for testing with subset)')
    parser.add_argument('--force', action='store_true',
                        help='Skip rebuild confirmation prompts')
    
    args = parser.parse_args()
    
    try:
        build_index(
            x=args.index_type,
            y=args.datastore,
            z=args.compression,
            optim=args.optimization,
            limit=args.limit,
            force=args.force
        )
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
