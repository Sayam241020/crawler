"""
SelfIndexer_x2: TF (Term Frequency) Ranking Indexer

Implements x=2 from the assignment requirements:
- Enables ranking with word counts (TF scores)
- Supports compression (z=1: none, z=2: Elias, z=3: Zlib)
- Supports both datastores (y=1: JSON, y=2: SQLite via compressed_indexer)
- Implements TAAT and DAAT query processing

Index structure:
{
  "term": [
    [doc_id, tf, [positions]],
    [doc_id, tf, [positions]],
    ...
  ]
}
"""

import json
import os
from collections import defaultdict
from .index_base import IndexBase
from .preprocessor import preprocess_text
from typing import Iterable, Dict, List
import math


class SelfIndexer_x2(IndexBase):
    """
    TF (Term Frequency) ranking indexer
    
    Assignment requirement: x=2 - Enable ranking with word counts
    """
    
    def __init__(self, dstore='CUSTOM', compr='NONE', optim='Null'):
        super().__init__(core='SelfIndex', info='WORDCOUNT', dstore=dstore, 
                         qproc='TERMatat', compr=compr, optim=optim)
        self.inverted_index = defaultdict(list)
        self.documents = {}
        self.compression_type = compr
        self.optim = optim  # Store optimization type
        
    def create_index(self, index_id: str, documents: Iterable[Dict]):
        print(f"--- Building TF index '{self.identifier_short}' ---")
        
        doc_count = 0
        for doc in documents:
            doc_id = doc['doc_id']
            self.documents[doc_id] = {'title': doc.get('title', 'No Title')}
            
            # Get preprocessed tokens
            if 'tokens' in doc:
                tokens = doc['tokens']
            else:
                content = doc.get('content', '')
                tokens = preprocess_text(content)
            
            # Count term frequencies and track positions
            term_data = defaultdict(lambda: {'count': 0, 'positions': []})
            for i, token in enumerate(tokens):
                term_data[token]['count'] += 1
                term_data[token]['positions'].append(i)
            
            # Build inverted index with TF scores
            for term, data in term_data.items():
                tf = data['count']
                positions = data['positions']
                self.inverted_index[term].append([doc_id, tf, positions])
            
            doc_count += 1
            if doc_count % 10000 == 0:
                print(f"  Processed {doc_count} documents...")

        print(f"TF index complete. Processed {doc_count} documents.")
        print(f"Total unique terms: {len(self.inverted_index)}")
        
        # BUILD-TIME OPTIMIZATION: Add skip pointers if requested
        # Note: Skip pointers for ranked retrieval are less common but can help with phrase queries
        if self.optim == 'Skipping':
            print("\n--- Building Skip Pointers (Build-Time Optimization) ---")
            print("Note: Skip pointers are primarily beneficial for Boolean queries.")
            print("For ranked retrieval, benefits are limited to phrase query processing.\n")
            # For now, skip pointer building is deferred for ranked indexers
            # to focus on core functionality
            print("Skip pointer integration for ranked indexers: NOT YET IMPLEMENTED")
            print("Continuing with standard TF index...\n")
        
        self._save_index(index_id)

    def _save_index(self, index_id: str):
        """Save index to disk (uncompressed)"""
        # Ensure indices directory exists
        os.makedirs('indices', exist_ok=True)
        
        # Save in indices/ folder
        filename = f"indices/{self.identifier_short}.json"
        index_data = {
            "identifier": self.identifier_short,
            "inverted_index": dict(self.inverted_index),
            "documents": self.documents,
            "compression": "NONE"
        }
        with open(filename, 'w') as f:
            json.dump(index_data, f)
        print(f"TF index saved to {filename}")

    def load_index(self, index_id: str):
        """Load index from disk with compression support"""
        # Try indices/ folder first, then root
        filename = f"indices/{index_id}.json"
        if not os.path.exists(filename):
            filename = f"{index_id}.json"
        
        print(f"--- Loading TF index from {filename} ---")
        
        with open(filename, 'r') as f:
            index_data = json.load(f)
        
        # Check compression
        compression_type = index_data.get("compression", "NONE")
        
        if compression_type != "NONE" and compression_type in ["CODE", "CLIB"]:
            print(f"Detected compression: {compression_type}")
            
            # Import appropriate compressor
            if compression_type == "CODE":
                from src.compression.elias import EliasCompressor
                compressor = EliasCompressor
            elif compression_type == "CLIB":
                from src.compression.zlib_compressor import ZlibCompressor
                compressor = ZlibCompressor
            
            # Decompress inverted index
            compressed_index = index_data["inverted_index"]
            self.inverted_index = defaultdict(
                list,
                compressor.decompress_inverted_index(compressed_index)
            )
        else:
            # Uncompressed
            self.inverted_index = defaultdict(list, index_data["inverted_index"])
        
        self.documents = index_data["documents"]
        print(f"Loaded {len(self.inverted_index)} terms, {len(self.documents)} documents")

    def query(self, query_str: str, mode: str = 'TAAT', top_k: int = 10) -> List[str]:
        """
        TF-based ranked retrieval with selectable query mode
        
        Args:
            query_str: Query string
            mode: 'TAAT' (Term-at-a-Time) or 'DAAT' (Document-at-a-Time)
            top_k: Number of top results
        
        Returns:
            List of document IDs ranked by TF scores
        """
        query_terms = preprocess_text(query_str)
        
        if mode == 'DAAT':
            return self._ranked_query_daat(query_terms, top_k)
        else:  # Default to TAAT
            return self._ranked_query_taat(query_terms, top_k)
    
    def query_daat(self, query_str: str, top_k: int = 10) -> List[str]:
        """
        DAAT (Document-at-a-Time) query processing - DEPRECATED
        Use query(query_str, mode='DAAT') instead
        """
        query_terms = preprocess_text(query_str)
        return self._ranked_query_daat(query_terms, top_k)
    
    def _ranked_query_daat(self, query_terms: List[str], top_k: int = 10) -> List[str]:
        """
        DAAT (Document-at-a-Time) query processing with optimizations
        
        Supports:
        - Thresholding (i=th): Filter results by minimum score
        - EarlyStopping (i=es): Stop when top-k stable results found
        """
        
        # Get postings for all query terms
        term_postings = {}
        for term in query_terms:
            if term in self.inverted_index:
                term_postings[term] = self.inverted_index[term]
        
        if not term_postings:
            return []
        
        # Thresholding: calculate minimum score threshold
        threshold = 0.0
        if self.optim == 'Thresholding':
            max_possible_score = len(query_terms) * 10
            threshold = max_possible_score * 0.1
        
        # Build doc -> term -> tf mapping
        doc_term_tf = defaultdict(dict)
        for term, postings in term_postings.items():
            for posting in postings:
                doc_id = posting[0]
                tf = posting[1]
                doc_term_tf[doc_id][term] = tf
        
        # Score each document
        doc_scores = []
        processed = 0
        for doc_id, term_tfs in doc_term_tf.items():
            score = sum(term_tfs.values())
            
            # Skip if below threshold
            if self.optim == 'Thresholding' and threshold > 0 and score < threshold:
                continue
            
            doc_scores.append((doc_id, score))
            processed += 1
            
            # Early stopping: if we have enough high-quality results, stop
            if self.optim == 'EarlyStopping' and processed >= top_k * 3:
                break
        
        # Sort and return top-K
        doc_scores.sort(key=lambda x: x[1], reverse=True)
        return [doc_id for doc_id, score in doc_scores[:top_k]]
    
    def _ranked_query_taat(self, query_terms: List[str], top_k: int = 10) -> List[str]:
        """
        TAAT (Term-at-a-Time) query processing with optimizations
        
        Supports:
        - Thresholding (i=th): Filter results by minimum score
        - EarlyStopping (i=es): Stop accumulating when threshold is met
        """
        doc_scores = defaultdict(float)
        
        # Thresholding: calculate minimum score threshold
        threshold = 0.0
        if self.optim == 'Thresholding':
            # Use a simple threshold: 10% of max possible score
            max_possible_score = len(query_terms) * 10  # Rough estimate
            threshold = max_possible_score * 0.1
        
        # Process each term
        for term in query_terms:
            if term not in self.inverted_index:
                continue
            
            postings = self.inverted_index[term]
            
            # Accumulate TF scores for all documents
            for posting in postings:
                doc_id = posting[0]
                tf = posting[1]
                doc_scores[doc_id] += tf
            
            # Early stopping optimization
            if self.optim == 'EarlyStopping' and len(doc_scores) >= top_k * 2:
                # Stop processing if we have enough candidates
                break
        
        # Filter by threshold if enabled
        if self.optim == 'Thresholding' and threshold > 0:
            doc_scores = {doc_id: score for doc_id, score in doc_scores.items() if score >= threshold}
        
        # Sort documents by score
        sorted_docs = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)
        
        # Return top-K document IDs
        return [doc_id for doc_id, score in sorted_docs[:top_k]]

    def update_index(self, index_id: str, remove_docs: Iterable[Dict], add_docs: Iterable[Dict]):
        """Update index by removing and adding documents"""
        # Remove documents
        for doc in remove_docs:
            doc_id = doc['doc_id']
            if doc_id in self.documents:
                del self.documents[doc_id]
                # Remove from inverted index
                for term in self.inverted_index:
                    self.inverted_index[term] = [
                        posting for posting in self.inverted_index[term] 
                        if posting[0] != doc_id
                    ]
        
        # Add new documents
        for doc in add_docs:
            doc_id = doc['doc_id']
            self.documents[doc_id] = {'title': doc.get('title', 'No Title')}
            
            if 'tokens' in doc:
                tokens = doc['tokens']
            else:
                tokens = preprocess_text(doc.get('content', ''))
            
            term_data = defaultdict(lambda: {'count': 0, 'positions': []})
            for i, token in enumerate(tokens):
                term_data[token]['count'] += 1
                term_data[token]['positions'].append(i)
            
            for term, data in term_data.items():
                tf = data['count']
                positions = data['positions']
                self.inverted_index[term].append([doc_id, tf, positions])
        
        self._save_index(index_id)

    def delete_index(self, index_id: str):
        """Delete index from disk"""
        filename = f"{self.identifier_short}.json"
        if os.path.exists(filename):
            os.remove(filename)
            print(f"Deleted index: {filename}")

    def list_indices(self) -> Iterable[str]:
        """List all available indices"""
        indices = []
        for file in os.listdir('.'):
            if file.startswith('SelfIndex_i2') and file.endswith('.json'):
                indices.append(file[:-5])
        return indices

    def list_indexed_files(self, index_id: str) -> Iterable[str]:
        """List all documents in the index"""
        return list(self.documents.keys())
