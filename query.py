#!/usr/bin/env python
"""
Interactive Query Interface for SelfIndex

Following index_base.py specification: SelfIndex_i{x}d{y}c{z}q{q}o{optim}

Usage:
    python query.py -x 1 -y 1 -z 1 -q T -optim 0 --query "machine learning"
    python query.py -x 3 -y 1 -z 1 -q T -optim 0 --interactive
"""

import sys
import os
import argparse
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.self_indexer import SelfIndexer
from src.self_indexer_x2 import SelfIndexer_x2
from src.self_indexer_x3 import SelfIndexer_x3


def load_indexer(x, y, z, q, optim):
    """Load the built index"""
    identifier = f"SelfIndex_i{x}d{y}c{z}q{q}o{optim}"
    
    # Select indexer class
    if x == 1:
        indexer = SelfIndexer()
    elif x == 2:
        indexer = SelfIndexer_x2()
    elif x == 3:
        indexer = SelfIndexer_x3()
    else:
        raise ValueError(f"Invalid index type: {x}")
    
    # Load index
    indexer.load_index(index_id=identifier)
    return indexer, identifier


def run_query(indexer, query, x, q_mode, top_k=10):
    """Run a single query using TAAT or DAAT"""
    print(f"\nQuery: {query}")
    print(f"Mode: {'TAAT (Term-at-a-Time)' if q_mode == 'T' else 'DAAT (Document-at-a-Time)'}")
    print("-" * 80)
    
    start_time = time.time()
    
    # Choose query method based on q_mode
    if q_mode == 'D':
        # DAAT mode
        if hasattr(indexer, 'query_daat'):
            if x == 1:
                results = indexer.query_daat(query)
            else:
                results = indexer.query_daat(query, top_k=top_k, return_scores=True)
        else:
            print("Warning: DAAT not implemented for this indexer, falling back to TAAT")
            q_mode = 'T'
    
    if q_mode == 'T':
        # TAAT mode (default)
        if x == 1:
            results = indexer.query(query)
        else:
            results = indexer.query(query, top_k=top_k, return_scores=True)
    
    elapsed_ms = (time.time() - start_time) * 1000
    
    if not results:
        print("No results found.")
        print(f"Query time: {elapsed_ms:.2f} ms")
        return
    
    print(f"Found {len(results)} results in {elapsed_ms:.2f} ms\n")
    
    # Display results
    if x == 1:
        # Boolean: just doc IDs
        for i, doc_id in enumerate(results[:top_k], 1):
            print(f"{i}. Document ID: {doc_id}")
    else:
        # TF/TF-IDF: doc_id, score pairs
        for i, (doc_id, score) in enumerate(results[:top_k], 1):
            print(f"{i}. Document ID: {doc_id}, Score: {score:.4f}")
    
    print("-" * 80)


def interactive_mode(indexer, x, q_mode, top_k=10):
    """Interactive query mode"""
    print("\n" + "="*80)
    print("Interactive Query Mode")
    print(f"Algorithm: {'TAAT (Term-at-a-Time)' if q_mode == 'T' else 'DAAT (Document-at-a-Time)'}")
    print("="*80)
    print("Commands:")
    print("  - Enter a query to search")
    print("  - 'quit' or 'exit' to stop")
    print("="*80 + "\n")
    
    while True:
        try:
            query = input("Query> ").strip()
            
            if not query:
                continue
            
            if query.lower() in ['quit', 'exit', 'q']:
                print("Goodbye!")
                break
            
            run_query(indexer, query, x, q_mode, top_k)
            
        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Query interface for SelfIndex",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single query
  python query.py -x 1 -y 1 -z 1 -q T -optim 0 --query "machine learning"
  
  # Interactive mode
  python query.py -x 3 -y 1 -z 1 -q T -optim 0 --interactive
  
Format: SelfIndex_i{x}d{y}c{z}q{q}o{optim}
  i{x}: 1=Boolean, 2=TF, 3=TF-IDF
  d{y}: 1=JSON, 2=DB1, 3=DB2
  c{z}: 1=None, 2=Code, 3=Library
  q{q}: T=Term-at-a-time, D=Document-at-a-time
  o{optim}: 0=None, sp=Skip, th=Threshold, es=EarlyStop
        """
    )
    
    parser.add_argument('-x', '--index-type', type=int, choices=[1, 2, 3], required=True,
                        help='Index type: 1=Boolean, 2=TF, 3=TF-IDF')
    parser.add_argument('-y', '--datastore', type=int, choices=[1, 2, 3], required=True,
                        help='Datastore: 1=JSON, 2=DB1, 3=DB2')
    parser.add_argument('-z', '--compression', type=int, choices=[1, 2, 3], required=True,
                        help='Compression: 1=None, 2=Code, 3=Library')
    parser.add_argument('-q', '--query-mode', choices=['T', 'D'], required=True,
                        help='Query mode: T=Term-at-a-time, D=Document-at-a-time')
    parser.add_argument('-optim', '--optimization', choices=['0', 'sp', 'th', 'es'], required=True,
                        help='Optimization: 0=None, sp=Skip, th=Threshold, es=EarlyStop')
    
    parser.add_argument('--query', help='Query string to search')
    parser.add_argument('--interactive', action='store_true', help='Interactive mode')
    parser.add_argument('--top-k', type=int, default=10, help='Number of results (default: 10)')
    
    args = parser.parse_args()
    
    try:
        # Load indexer
        print(f"Loading index...")
        indexer, identifier = load_indexer(
            args.index_type, args.datastore, args.compression,
            args.query_mode, args.optimization
        )
        print(f"✓ Loaded: {identifier}\n")
        
        # Run query or interactive mode
        if args.interactive:
            interactive_mode(indexer, args.index_type, args.query_mode, args.top_k)
        elif args.query:
            run_query(indexer, args.query, args.index_type, args.query_mode, args.top_k)
        else:
            print("Error: Provide --query or --interactive")
            sys.exit(1)
    
    except FileNotFoundError as e:
        print(f"Error: Index not found. Build it first:")
        print(f"  python build.py -x {args.index_type} -y {args.datastore} -z {args.compression} -q {args.query_mode} -optim {args.optimization}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
