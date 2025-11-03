"""
Skip Pointer Builder - Build-time integration

This module adds skip pointers to inverted indices during the indexing phase.
Skip pointers enable efficient merging of postings lists in Boolean queries.

Build-time vs Runtime:
- BUILD-TIME: Skip pointers are embedded in the index structure itself
- RUNTIME: The index_base.py enable_skip_pointers() creates in-memory skip index

This file implements BUILD-TIME skip pointer embedding.
"""

import math
from typing import Dict, List, Any

def calculate_skip_interval(postings_length: int) -> int:
    """
    Calculate optimal skip pointer interval for a postings list.
    
    Standard approach: sqrt(n) spacing for optimal performance.
    
    Args:
        postings_length: Number of postings in the list
        
    Returns:
        Skip interval (minimum 2 to ensure at least one skip)
    """
    if postings_length < 3:
        return postings_length  # No skip pointers needed
    
    # Use square root spacing (optimal for most cases)
    skip_interval = int(math.sqrt(postings_length))
    
    # Ensure minimum skip of 2 (avoid skip_interval=1)
    return max(2, skip_interval)

def add_skip_pointers_to_postings(postings_list: List[Any], skip_interval: int) -> List[Dict]:
    """
    Adds skip pointers to a single postings list.
    
    Each posting becomes: {
        'doc_id': str,
        'positions': list[int],
        'skip': int | None  # Index of next skip pointer, or None
    }
    
    Args:
        postings_list: Original postings [[doc_id, positions], ...]
        skip_interval: Distance between skip pointers
        
    Returns:
        Enhanced postings list with skip pointers
    """
    enhanced_postings = []
    length = len(postings_list)
    
    for i, posting in enumerate(postings_list):
        doc_id, positions = posting
        
        # Calculate skip pointer target
        skip_target = i + skip_interval
        
        if skip_target < length:
            # Add skip pointer to this position
            enhanced_posting = {
                'doc_id': doc_id,
                'positions': positions,
                'skip': skip_target
            }
        else:
            # No skip pointer (near end of list)
            enhanced_posting = {
                'doc_id': doc_id,
                'positions': positions,
                'skip': None
            }
        
        enhanced_postings.append(enhanced_posting)
    
    return enhanced_postings

def add_skip_pointers(inverted_index: Dict[str, List]) -> Dict[str, Dict]:
    """
    Add skip pointers to an entire inverted index.
    
    This transforms a standard inverted index into a skip-pointer-enhanced index:
    
    BEFORE:
    {
        'term1': [[doc1, [pos1, pos2]], [doc2, [pos3]], ...],
        'term2': [[doc3, [pos4]], ...]
    }
    
    AFTER:
    {
        'term1': {
            'postings': [
                {'doc_id': doc1, 'positions': [pos1, pos2], 'skip': 3},
                {'doc_id': doc2, 'positions': [pos3], 'skip': 6},
                ...
            ],
            'skip_interval': 3
        },
        'term2': {
            'postings': [...],
            'skip_interval': 2
        }
    }
    
    Args:
        inverted_index: Original inverted index
        
    Returns:
        Enhanced inverted index with skip pointers
    """
    print("Adding skip pointers to inverted index...")
    enhanced_index = {}
    
    total_terms = len(inverted_index)
    processed = 0
    
    for term, postings_list in inverted_index.items():
        # Calculate optimal skip interval
        skip_interval = calculate_skip_interval(len(postings_list))
        
        # Add skip pointers to this term's postings
        enhanced_postings = add_skip_pointers_to_postings(postings_list, skip_interval)
        
        # Store enhanced postings with metadata
        enhanced_index[term] = {
            'postings': enhanced_postings,
            'skip_interval': skip_interval,
            'length': len(enhanced_postings)
        }
        
        processed += 1
        if processed % 10000 == 0:
            print(f"Processed {processed}/{total_terms} terms...")
    
    print(f"Skip pointers added to {total_terms} terms.")
    return enhanced_index

def verify_skip_pointers(enhanced_index: Dict[str, Dict]) -> Dict[str, Any]:
    """
    Verify skip pointer integrity and gather statistics.
    
    Returns:
        Dictionary with verification stats
    """
    stats = {
        'total_terms': len(enhanced_index),
        'avg_skip_interval': 0,
        'total_skip_pointers': 0,
        'total_postings': 0
    }
    
    skip_intervals = []
    
    for term, term_data in enhanced_index.items():
        postings = term_data['postings']
        skip_interval = term_data['skip_interval']
        
        stats['total_postings'] += len(postings)
        skip_intervals.append(skip_interval)
        
        # Count skip pointers
        skip_count = sum(1 for p in postings if p['skip'] is not None)
        stats['total_skip_pointers'] += skip_count
    
    if skip_intervals:
        stats['avg_skip_interval'] = sum(skip_intervals) / len(skip_intervals)
    
    return stats
