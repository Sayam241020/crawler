"""
Skip Pointers Implementation
Runtime optimization for faster Boolean query processing

Skip pointers allow faster intersection/union of postings lists by
enabling "jumping" over irrelevant documents.

Strategy: Compute on index load (not stored on disk)
- Avoids rebuilding indices
- Small memory overhead
- Fast generation (<1 second per index)

Usage:
    skip_index = generate_skip_pointers(inverted_index, skip_interval=None)
    # Then use skip_index for faster query processing
"""
import math
from typing import Dict, List, Tuple, Optional
from collections import defaultdict


def generate_skip_pointers(
    inverted_index: Dict[str, List], 
    skip_interval: Optional[int] = None
) -> Dict[str, List[Tuple[int, int, int]]]:
    """
    Generate skip pointers for all terms in inverted index.
    
    Skip pointer format: (doc_id, position_in_list, next_skip_index)
    
    Args:
        inverted_index: Dictionary mapping terms to postings lists
                       Format: {term: [[doc_id, positions], ...]}
        skip_interval: Distance between skip pointers (default: sqrt(list_length))
        
    Returns:
        Dictionary with skip pointers: {term: [(doc_id, pos, next_skip), ...]}
        
    Example:
        Original postings: [1, 3, 5, 7, 9, 11, 13, 15]
        Skip interval: 3
        Skip pointers: [(1, 0, 3), (7, 3, 6), (13, 6, None)]
    """
    skip_index = {}
    
    for term, postings_list in inverted_index.items():
        if not postings_list:
            skip_index[term] = []
            continue
            
        list_len = len(postings_list)
        
        # Calculate optimal skip interval (sqrt of list length)
        if skip_interval is None:
            interval = max(2, int(math.sqrt(list_len)))
        else:
            interval = skip_interval
        
        # Generate skip pointers
        skip_pointers = []
        for i in range(0, list_len, interval):
            doc_id = postings_list[i][0]  # First element is doc_id
            next_skip = i + interval if (i + interval) < list_len else None
            skip_pointers.append((doc_id, i, next_skip))
        
        skip_index[term] = skip_pointers
    
    return skip_index


def intersect_with_skips(
    postings1: List,
    postings2: List,
    skip_pointers1: List[Tuple[int, int, int]],
    skip_pointers2: List[Tuple[int, int, int]],
    debug: bool = False
) -> List[str]:
    """
    Intersect two postings lists using skip pointers for faster processing.
    
    Args:
        postings1: First postings list [[doc_id, positions], ...]
        postings2: Second postings list
        skip_pointers1: Skip pointers for first list [(doc_id, pos, next_skip_idx), ...]
        skip_pointers2: Skip pointers for second list
        debug: Enable debug output
        
    Returns:
        List of document IDs in intersection
    """
    if not postings1 or not postings2:
        return []
    
    result = []
    i, j = 0, 0
    
    # Build lookup maps for skip pointers by position
    skip_map1 = {pos: next_skip for _, pos, next_skip in skip_pointers1 if next_skip is not None}
    skip_map2 = {pos: next_skip for _, pos, next_skip in skip_pointers2 if next_skip is not None}
    
    if debug:
        print(f"Postings1 length: {len(postings1)}, Skip map1 size: {len(skip_map1)}")
        print(f"Postings2 length: {len(postings2)}, Skip map2 size: {len(skip_map2)}")
        print(f"First 5 skip map1: {dict(list(skip_map1.items())[:5])}")
        print(f"First 5 skip map2: {dict(list(skip_map2.items())[:5])}")
    
    iterations = 0
    while i < len(postings1) and j < len(postings2):
        iterations += 1
        doc1 = postings1[i][0]
        doc2 = postings2[j][0]
        
        if debug and iterations <= 20:
            print(f"Iter {iterations}: i={i}, j={j}, doc1={doc1}, doc2={doc2}")
        
        if doc1 == doc2:
            result.append(doc1)
            if debug and iterations <= 20:
                print(f"  MATCH! Added {doc1}")
            i += 1
            j += 1
        elif doc1 < doc2:
            # Try to skip ahead in list 1
            if i in skip_map1:
                skip_idx = skip_map1[i]
                if skip_idx < len(postings1) and postings1[skip_idx][0] < doc2:  # Changed from <=
                    if debug and iterations <= 20:
                        print(f"  Skip in list1: {i} -> {skip_idx} (doc {doc1} -> {postings1[skip_idx][0]})")
                    i = skip_idx
                else:
                    if debug and iterations <= 20:
                        print(f"  No skip (would overshoot): skip_idx={skip_idx}, would jump to {postings1[skip_idx][0] if skip_idx < len(postings1) else 'END'}")
                    i += 1
            else:
                if debug and iterations <= 20:
                    print(f"  No skip pointer at {i}, incrementing")
                i += 1
        else:  # doc1 > doc2
            # Try to skip ahead in list 2
            if j in skip_map2:
                skip_idx = skip_map2[j]
                if skip_idx < len(postings2) and postings2[skip_idx][0] < doc1:  # Changed from <=
                    if debug and iterations <= 20:
                        print(f"  Skip in list2: {j} -> {skip_idx} (doc {doc2} -> {postings2[skip_idx][0]})")
                    j = skip_idx
                else:
                    if debug and iterations <= 20:
                        print(f"  No skip (would overshoot): skip_idx={skip_idx}, would jump to {postings2[skip_idx][0] if skip_idx < len(postings2) else 'END'}")
                    j += 1
            else:
                if debug and iterations <= 20:
                    print(f"  No skip pointer at {j}, incrementing")
                j += 1
    
    if debug:
        print(f"Total iterations: {iterations}, Results: {len(result)}")
    
    return result


def union_with_skips(
    postings1: List,
    postings2: List,
    skip_pointers1: List[Tuple[int, int, int]],
    skip_pointers2: List[Tuple[int, int, int]]
) -> List[str]:
    """
    Union two postings lists using skip pointers.
    
    Args:
        postings1: First postings list [[doc_id, positions], ...]
        postings2: Second postings list
        skip_pointers1: Skip pointers for first list
        skip_pointers2: Skip pointers for second list
        
    Returns:
        List of document IDs in union (sorted, no duplicates)
    """
    result = []
    i, j = 0, 0
    
    while i < len(postings1) and j < len(postings2):
        doc1 = postings1[i][0]
        doc2 = postings2[j][0]
        
        if doc1 == doc2:
            result.append(doc1)
            i += 1
            j += 1
        elif doc1 < doc2:
            result.append(doc1)
            i += 1
        else:
            result.append(doc2)
            j += 1
    
    # Add remaining documents
    while i < len(postings1):
        result.append(postings1[i][0])
        i += 1
    
    while j < len(postings2):
        result.append(postings2[j][0])
        j += 1
    
    return result


class SkipPointerIndex:
    """
    Wrapper class for inverted index with skip pointers.
    Provides faster query processing for Boolean queries.
    """
    
    def __init__(self, inverted_index: Dict[str, List], skip_interval: Optional[int] = None):
        """
        Initialize skip pointer index.
        
        Args:
            inverted_index: Original inverted index
            skip_interval: Distance between skip pointers (default: sqrt(list_length))
        """
        # Sort all postings lists to ensure correct lexicographic order
        # (Skip pointers require sorted lists)
        print("Sorting postings lists...")
        self.inverted_index = {}
        for term, postings in inverted_index.items():
            self.inverted_index[term] = sorted(postings, key=lambda x: x[0])
        
        self.skip_pointers = generate_skip_pointers(self.inverted_index, skip_interval)
        
        # Statistics
        total_skips = sum(len(skips) for skips in self.skip_pointers.values())
        total_postings = sum(len(posts) for posts in self.inverted_index.values())
        
        print(f"Skip pointers generated:")
        print(f"  Total terms: {len(self.skip_pointers):,}")
        print(f"  Total skip pointers: {total_skips:,}")
        print(f"  Total postings: {total_postings:,}")
        print(f"  Average skips per term: {total_skips/len(self.skip_pointers):.1f}")
    
    def get_postings(self, term: str) -> List:
        """Get original postings list for a term"""
        return self.inverted_index.get(term, [])
    
    def get_skip_pointers(self, term: str) -> List[Tuple[int, int, int]]:
        """Get skip pointers for a term"""
        return self.skip_pointers.get(term, [])
    
    def intersect(self, term1: str, term2: str, debug: bool = False) -> List[str]:
        """
        Intersect postings for two terms using skip pointers.
        
        Args:
            term1: First term
            term2: Second term
            debug: Enable debug output
            
        Returns:
            List of document IDs in intersection
        """
        postings1 = self.get_postings(term1)
        postings2 = self.get_postings(term2)
        skips1 = self.get_skip_pointers(term1)
        skips2 = self.get_skip_pointers(term2)
        
        return intersect_with_skips(postings1, postings2, skips1, skips2, debug)
    
    def union(self, term1: str, term2: str) -> List[str]:
        """
        Union postings for two terms using skip pointers.
        
        Args:
            term1: First term
            term2: Second term
            
        Returns:
            List of document IDs in union
        """
        postings1 = self.get_postings(term1)
        postings2 = self.get_postings(term2)
        skips1 = self.get_skip_pointers(term1)
        skips2 = self.get_skip_pointers(term2)
        
        return union_with_skips(postings1, postings2, skips1, skips2)
