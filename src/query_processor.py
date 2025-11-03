# src/query_processor.py
"""
Query processing algorithms for TAAT (Term-at-a-Time) and DAAT (Document-at-a-Time).

TAAT (Term-at-a-Time):
- Process one term at a time
- Accumulate document scores incrementally
- Good for Boolean queries and when terms have short posting lists

DAAT (Document-at-a-Time):
- Process all terms for each document
- Compute final score for each document in one pass
- Better for ranked retrieval with many query terms
- Can use early termination optimizations
"""

from typing import List, Set, Dict, Tuple
from collections import defaultdict
import heapq


class QueryProcessor:
    """
    Implements TAAT and DAAT query processing algorithms.
    """
    
    @staticmethod
    def taat_boolean(postings_lists: List[Set[str]], operation: str = 'AND') -> Set[str]:
        """
        Term-at-a-Time processing for Boolean queries.
        
        Args:
            postings_lists: List of sets containing doc_ids for each query term
            operation: 'AND', 'OR', or 'NOT'
            
        Returns:
            Set of doc_ids that satisfy the Boolean query
        """
        if not postings_lists:
            return set()
        
        if operation == 'AND':
            result = postings_lists[0]
            for postings in postings_lists[1:]:
                result = result.intersection(postings)
            return result
        
        elif operation == 'OR':
            result = set()
            for postings in postings_lists:
                result = result.union(postings)
            return result
        
        elif operation == 'NOT':
            # NOT is unary - complement of first list
            if postings_lists:
                return postings_lists[0]  # Complement done elsewhere
            return set()
        
        return set()
    
    @staticmethod
    def taat_ranked(term_postings: Dict[str, List[Tuple]], scoring_fn, top_k: int = 10) -> List[Tuple[str, float]]:
        """
        Term-at-a-Time processing for ranked retrieval.
        
        Args:
            term_postings: Dict mapping term -> list of (doc_id, tf, positions) tuples
            scoring_fn: Function to compute score contribution for a term in a document
            top_k: Number of top results to return
            
        Returns:
            List of (doc_id, score) tuples sorted by score descending
        """
        scores = defaultdict(float)
        
        # Process each term sequentially
        for term, postings in term_postings.items():
            for posting in postings:
                doc_id = posting[0]
                tf = posting[1] if len(posting) > 1 else 1
                
                # Accumulate score for this document
                scores[doc_id] += scoring_fn(term, doc_id, tf)
        
        # Sort by score and return top-K
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return ranked[:top_k]
    
    @staticmethod
    def daat_boolean(postings_lists: List[Set[str]], operation: str = 'AND') -> Set[str]:
        """
        Document-at-a-Time processing for Boolean queries.
        More efficient than TAAT when posting lists are sorted.
        
        Args:
            postings_lists: List of sets containing doc_ids for each query term
            operation: 'AND' or 'OR'
            
        Returns:
            Set of doc_ids that satisfy the Boolean query
        """
        if not postings_lists:
            return set()
        
        if operation == 'AND':
            # Convert to sorted lists for efficient intersection
            sorted_lists = [sorted(list(postings)) for postings in postings_lists]
            
            if not all(sorted_lists):
                return set()
            
            # Use multi-way merge with AND logic
            result = set()
            pointers = [0] * len(sorted_lists)
            
            while all(pointers[i] < len(sorted_lists[i]) for i in range(len(sorted_lists))):
                # Get current doc_ids at each pointer
                current_docs = [sorted_lists[i][pointers[i]] for i in range(len(sorted_lists))]
                
                # If all doc_ids match, we have an AND match
                if len(set(current_docs)) == 1:
                    result.add(current_docs[0])
                    # Advance all pointers
                    for i in range(len(pointers)):
                        pointers[i] += 1
                else:
                    # Advance pointer(s) pointing to smallest doc_id
                    min_doc = min(current_docs)
                    for i in range(len(pointers)):
                        if current_docs[i] == min_doc:
                            pointers[i] += 1
            
            return result
        
        elif operation == 'OR':
            # Simple union for OR
            result = set()
            for postings in postings_lists:
                result = result.union(postings)
            return result
        
        return set()
    
    @staticmethod
    def daat_ranked(term_postings: Dict[str, List[Tuple]], scoring_fn, top_k: int = 10, 
                   early_termination: bool = False) -> List[Tuple[str, float]]:
        """
        Document-at-a-Time processing for ranked retrieval.
        More cache-friendly and allows early termination.
        
        Args:
            term_postings: Dict mapping term -> list of (doc_id, tf, positions) tuples
            scoring_fn: Function to compute score contribution for a term in a document
            top_k: Number of top results to return
            early_termination: If True, use early termination optimization
            
        Returns:
            List of (doc_id, score) tuples sorted by score descending
        """
        # Build a mapping of doc_id -> list of (term, tf) pairs
        doc_terms = defaultdict(list)
        for term, postings in term_postings.items():
            for posting in postings:
                doc_id = posting[0]
                tf = posting[1] if len(posting) > 1 else 1
                doc_terms[doc_id].append((term, tf))
        
        scores = []
        
        # Process each document completely before moving to next
        for doc_id, term_tf_pairs in doc_terms.items():
            score = 0.0
            
            # Compute complete score for this document
            for term, tf in term_tf_pairs:
                score += scoring_fn(term, doc_id, tf)
            
            if early_termination and len(scores) >= top_k:
                # Use heap to maintain top-K
                if score > scores[0][0]:  # Better than worst in top-K
                    heapq.heapreplace(scores, (score, doc_id))
            else:
                scores.append((score, doc_id))
        
        # Convert to proper format and sort
        if early_termination and len(scores) > top_k:
            # Heap is a min-heap, so we have top-K but need to sort descending
            top_scores = heapq.nlargest(top_k, scores)
            return [(doc_id, score) for score, doc_id in top_scores]
        else:
            # Sort all scores
            scores.sort(reverse=True)
            return [(doc_id, score) for score, doc_id in scores[:top_k]]
    
    @staticmethod
    def compare_algorithms(term_postings: Dict[str, List[Tuple]], scoring_fn, top_k: int = 10):
        """
        Compare TAAT vs DAAT performance on the same query.
        
        Returns:
            Dictionary with timing and result comparison
        """
        import time
        
        # Run TAAT
        start = time.time()
        taat_results = QueryProcessor.taat_ranked(term_postings, scoring_fn, top_k)
        taat_time = time.time() - start
        
        # Run DAAT
        start = time.time()
        daat_results = QueryProcessor.daat_ranked(term_postings, scoring_fn, top_k)
        daat_time = time.time() - start
        
        # Run DAAT with early termination
        start = time.time()
        daat_et_results = QueryProcessor.daat_ranked(term_postings, scoring_fn, top_k, early_termination=True)
        daat_et_time = time.time() - start
        
        return {
            'taat': {'time': taat_time, 'results': taat_results},
            'daat': {'time': daat_time, 'results': daat_results},
            'daat_early_term': {'time': daat_et_time, 'results': daat_et_results},
            'speedup_daat': taat_time / daat_time if daat_time > 0 else 0,
            'speedup_daat_et': taat_time / daat_et_time if daat_et_time > 0 else 0
        }
