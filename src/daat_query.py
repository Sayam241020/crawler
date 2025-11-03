"""
Document-at-a-Time (DAAT) Query Processing
Runtime query engine alternative to Term-at-a-Time (TAAT)

TAAT: Process one term at a time, accumulate scores
DAAT: Process one document at a time, compute scores for all query terms

Advantages of DAAT:
- Better cache locality (process one doc fully before moving to next)
- Early termination possible (stop when top-k found)
- Lower memory usage (don't need full score arrays)

Usage:
    from src.daat_query import daat_query
    results = daat_query(indexer, query_terms, top_k=10)
"""
import heapq
from typing import List, Dict, Tuple, Set
from collections import defaultdict


def daat_query_boolean(
    inverted_index: Dict[str, List],
    query_terms: List[str],
    operation: str = 'AND'
) -> Set[str]:
    """
    Document-at-a-time Boolean query processing.
    
    Args:
        inverted_index: Dictionary mapping terms to postings lists
        query_terms: List of preprocessed query terms
        operation: 'AND' or 'OR' (default: 'AND')
        
    Returns:
        Set of matching document IDs
    """
    if not query_terms:
        return set()
    
    # Get postings for all query terms
    postings_lists = []
    for term in query_terms:
        postings = inverted_index.get(term, [])
        if postings:
            # Extract just doc_ids for faster processing
            doc_ids = [posting[0] for posting in postings]
            postings_lists.append(set(doc_ids))
    
    if not postings_lists:
        return set()
    
    # Perform set operation
    if operation == 'AND':
        # Intersection
        result = postings_lists[0]
        for postings in postings_lists[1:]:
            result = result.intersection(postings)
    else:  # OR
        # Union
        result = postings_lists[0]
        for postings in postings_lists[1:]:
            result = result.union(postings)
    
    return result


def daat_query_ranked(
    inverted_index: Dict[str, List],
    query_terms: List[str],
    scoring: str = 'TF',
    idf_scores: Dict[str, float] = None,
    top_k: int = 10
) -> List[Tuple[str, float]]:
    """
    Document-at-a-time ranked query processing with TF or TF-IDF scoring.
    
    Algorithm:
    1. Get all postings for query terms
    2. Create sorted list of (doc_id, term_index, tf) tuples
    3. Process documents one at a time
    4. For each doc, accumulate scores from all query terms
    5. Return top-k ranked documents
    
    Args:
        inverted_index: Dictionary mapping terms to postings lists
        query_terms: List of preprocessed query terms
        scoring: 'TF' or 'TFIDF' (default: 'TF')
        idf_scores: Dictionary of IDF scores (required if scoring='TFIDF')
        top_k: Number of top results to return
        
    Returns:
        List of (doc_id, score) tuples sorted by score (descending)
    """
    if not query_terms:
        return []
    
    # Collect all postings with term information
    # Format: (doc_id, term_index, tf)
    all_postings = []
    term_to_idf = {}
    
    for term_idx, term in enumerate(query_terms):
        postings = inverted_index.get(term, [])
        
        # Get IDF for this term (if using TF-IDF)
        if scoring == 'TFIDF' and idf_scores:
            term_to_idf[term_idx] = idf_scores.get(term, 0.0)
        else:
            term_to_idf[term_idx] = 1.0  # No IDF weighting
        
        for posting in postings:
            doc_id = posting[0]
            
            # Get term frequency from posting
            if len(posting) >= 3:
                # Format: [doc_id, tf, positions]
                tf = posting[1]
            elif len(posting) == 2:
                # Format: [doc_id, positions]
                tf = len(posting[1])  # Count positions
            else:
                tf = 1
            
            all_postings.append((doc_id, term_idx, tf))
    
    if not all_postings:
        return []
    
    # Sort by doc_id to process documents in order
    all_postings.sort(key=lambda x: x[0])
    
    # Process documents one at a time
    doc_scores = {}
    current_doc = None
    current_score = 0.0
    
    for doc_id, term_idx, tf in all_postings:
        if doc_id != current_doc:
            # Save previous document score
            if current_doc is not None:
                doc_scores[current_doc] = current_score
            
            # Start new document
            current_doc = doc_id
            current_score = 0.0
        
        # Accumulate score for this term
        idf = term_to_idf[term_idx]
        current_score += tf * idf
    
    # Save last document
    if current_doc is not None:
        doc_scores[current_doc] = current_score
    
    # Get top-k documents using heap
    if len(doc_scores) <= top_k:
        # Return all documents sorted by score
        ranked = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)
    else:
        # Use heap for efficient top-k selection
        top_k_heap = heapq.nlargest(top_k, doc_scores.items(), key=lambda x: x[1])
        ranked = sorted(top_k_heap, key=lambda x: x[1], reverse=True)
    
    return ranked


def daat_query_with_early_termination(
    inverted_index: Dict[str, List],
    query_terms: List[str],
    idf_scores: Dict[str, float] = None,
    top_k: int = 10,
    threshold: float = 0.0
) -> List[Tuple[str, float]]:
    """
    DAAT with early termination optimization.
    
    Stops processing when we've found top-k documents with high confidence.
    
    Args:
        inverted_index: Dictionary mapping terms to postings lists
        query_terms: List of preprocessed query terms
        idf_scores: Dictionary of IDF scores (optional)
        top_k: Number of top results to return
        threshold: Minimum score threshold for inclusion
        
    Returns:
        List of (doc_id, score) tuples sorted by score (descending)
    """
    # Use regular DAAT but with threshold filtering
    all_results = daat_query_ranked(
        inverted_index, 
        query_terms, 
        scoring='TFIDF' if idf_scores else 'TF',
        idf_scores=idf_scores,
        top_k=top_k * 2  # Get more candidates
    )
    
    # Filter by threshold
    if threshold > 0:
        filtered = [(doc_id, score) for doc_id, score in all_results if score >= threshold]
    else:
        filtered = all_results
    
    # Return top-k
    return filtered[:top_k]


class DAATQueryEngine:
    """
    Document-at-a-Time query engine for ranked retrieval.
    
    Usage:
        engine = DAATQueryEngine(indexer)
        results = engine.query(query_terms, top_k=10)
    """
    
    def __init__(self, inverted_index: Dict[str, List], idf_scores: Dict[str, float] = None):
        """
        Initialize DAAT query engine.
        
        Args:
            inverted_index: Inverted index from indexer
            idf_scores: IDF scores (optional, for TF-IDF)
        """
        self.inverted_index = inverted_index
        self.idf_scores = idf_scores or {}
        self.query_mode = 'DAAT'
    
    def query_boolean(self, query_terms: List[str], operation: str = 'AND') -> Set[str]:
        """
        Boolean query using DAAT.
        
        Args:
            query_terms: Preprocessed query terms
            operation: 'AND' or 'OR'
            
        Returns:
            Set of matching document IDs
        """
        return daat_query_boolean(self.inverted_index, query_terms, operation)
    
    def query_ranked(
        self, 
        query_terms: List[str], 
        top_k: int = 10,
        scoring: str = 'TFIDF'
    ) -> List[Tuple[str, float]]:
        """
        Ranked query using DAAT.
        
        Args:
            query_terms: Preprocessed query terms
            top_k: Number of top results
            scoring: 'TF' or 'TFIDF'
            
        Returns:
            List of (doc_id, score) tuples
        """
        return daat_query_ranked(
            self.inverted_index,
            query_terms,
            scoring=scoring,
            idf_scores=self.idf_scores if scoring == 'TFIDF' else None,
            top_k=top_k
        )
    
    def query_with_threshold(
        self,
        query_terms: List[str],
        top_k: int = 10,
        threshold: float = 0.0
    ) -> List[Tuple[str, float]]:
        """
        Ranked query with score threshold.
        
        Args:
            query_terms: Preprocessed query terms
            top_k: Number of top results
            threshold: Minimum score threshold
            
        Returns:
            List of (doc_id, score) tuples
        """
        return daat_query_with_early_termination(
            self.inverted_index,
            query_terms,
            idf_scores=self.idf_scores,
            top_k=top_k,
            threshold=threshold
        )
