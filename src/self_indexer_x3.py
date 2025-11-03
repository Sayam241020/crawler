"""
SelfIndexer_x3: TF-IDF Ranking Indexer

Implements x=3 from the assignment requirements:
- TF-IDF scoring for better relevance ranking
- Supports compression (z=1: none, z=2: Elias, z=3: Zlib)
- Supports both datastores (y=1: JSON, y=2: SQLite via compressed_indexer)
- Implements TAAT and DAAT query processing

Index structure:
{
  "term": [
    [doc_id, tf, [positions]],
    [doc_id, tf, [positions]],
    ...
  ],
  "idf_scores": {
    "term": idf_value,
    ...
  }
}
"""

import json
import os
import math
from collections import defaultdict
from .index_base import IndexBase
from .preprocessor import preprocess_text
from typing import Iterable, Dict, List


class SelfIndexer_x3(IndexBase):
    """
    TF-IDF ranking indexer
    
    Assignment requirement: x=3 - Evaluate gains from TF-IDF scores
    """
    
    def __init__(self, dstore='CUSTOM', compr='NONE', optim='Null'):
        super().__init__(core='SelfIndex', info='TFIDF', dstore=dstore, 
                         qproc='TERMatat', compr=compr, optim=optim)
        self.inverted_index = defaultdict(list)
        self.documents = {}
        self.idf_scores = {}
        self.num_documents = 0
        self.compression_type = compr
        self.optim = optim  # Store optimization type
        
    def create_index(self, index_id: str, documents: Iterable[Dict]):
        print(f"--- Building TF-IDF index '{self.identifier_short}' ---")
        
        # First pass: build TF index
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

        self.num_documents = doc_count
        
        # Second pass: compute IDF scores
        print(f"Computing IDF scores for {len(self.inverted_index)} terms...")
        for term, postings in self.inverted_index.items():
            df = len(postings)  # Document frequency
            idf = math.log(self.num_documents / df) if df > 0 else 0
            self.idf_scores[term] = idf
        
        print(f"TF-IDF index complete. Processed {doc_count} documents.")
        print(f"Total unique terms: {len(self.inverted_index)}")
        
        # BUILD-TIME OPTIMIZATION: Add skip pointers if requested
        if self.optim == 'Skipping':
            print("\n--- Building Skip Pointers (Build-Time Optimization) ---")
            print("Note: Skip pointers are primarily beneficial for Boolean queries.")
            print("For TF-IDF ranked retrieval, benefits are limited.\n")
            # For now, skip pointer building is deferred for ranked indexers
            print("Skip pointer integration for ranked indexers: NOT YET IMPLEMENTED")
            print("Continuing with standard TF-IDF index...\n")
        
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
            "idf_scores": self.idf_scores,
            "num_documents": self.num_documents,
            "compression": "NONE"
        }
        with open(filename, 'w') as f:
            json.dump(index_data, f)
        print(f"TF-IDF index saved to {filename}")

    def load_index(self, index_id: str):
        """Load index from disk with compression support"""
        # Try indices/ folder first, then root
        filename = f"indices/{index_id}.json"
        if not os.path.exists(filename):
            filename = f"{index_id}.json"
        
        print(f"--- Loading TF-IDF index from {filename} ---")
        
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
        self.idf_scores = index_data.get("idf_scores", {})
        self.num_documents = index_data.get("num_documents", len(self.documents))
        
        print(f"Loaded {len(self.inverted_index)} terms, {len(self.documents)} documents")
        print(f"IDF scores computed for {len(self.idf_scores)} terms")

    def query(self, query_str: str, mode: str = 'TAAT', top_k: int = 10) -> List[str]:
        """
        TF-IDF ranked retrieval with selectable query mode
        
        Args:
            query_str: Query string
            mode: 'TAAT' (Term-at-a-Time) or 'DAAT' (Document-at-a-Time)
            top_k: Number of top results
        
        Returns:
            List of document IDs ranked by TF-IDF scores
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
        DAAT (Document-at-a-Time) query processing with TF-IDF and optimizations
        
        Supports:
        - Thresholding (i=th): Filter results by minimum TF-IDF score
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
        if self.optim == 'Thresholding' and self.idf_scores:
            avg_idf = sum(self.idf_scores.values()) / len(self.idf_scores)
            threshold = avg_idf * len(query_terms) * 0.05
        
        # Build doc -> term -> tf mapping
        doc_term_tf = defaultdict(dict)
        for term, postings in term_postings.items():
            for posting in postings:
                doc_id = posting[0]
                tf = posting[1]
                doc_term_tf[doc_id][term] = tf
        
        # Score each document with TF-IDF
        doc_scores = []
        processed = 0
        for doc_id, term_tfs in doc_term_tf.items():
            score = 0
            for term, tf in term_tfs.items():
                idf = self.idf_scores.get(term, 0)
                score += tf * idf
            
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
        TAAT (Term-at-a-Time) query processing with TF-IDF and optimizations
        
        Supports:
        - Thresholding (i=th): Filter results by minimum TF-IDF score
        - EarlyStopping (i=es): Stop accumulating when threshold is met
        """
        doc_scores = defaultdict(float)
        
        # Thresholding: calculate minimum score threshold
        threshold = 0.0
        if self.optim == 'Thresholding':
            # For TF-IDF, use 5% of average IDF * query length
            if self.idf_scores:
                avg_idf = sum(self.idf_scores.values()) / len(self.idf_scores)
                threshold = avg_idf * len(query_terms) * 0.05
        
        # Process each term
        for term in query_terms:
            if term not in self.inverted_index:
                continue
            
            postings = self.inverted_index[term]
            idf = self.idf_scores.get(term, 0)
            
            # Accumulate TF-IDF scores for all documents
            for posting in postings:
                doc_id = posting[0]
                tf = posting[1]
                tf_idf = tf * idf
                doc_scores[doc_id] += tf_idf
            
            # Early stopping optimization
            if self.optim == 'EarlyStopping' and len(doc_scores) >= top_k * 2:
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
                self.num_documents -= 1
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
            self.num_documents += 1
            
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
        
        # Recompute IDF scores for all terms
        print("Recomputing IDF scores...")
        for term, postings in self.inverted_index.items():
            df = len(postings)
            idf = math.log(self.num_documents / df) if df > 0 else 0
            self.idf_scores[term] = idf
        
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
            if file.startswith('SelfIndex_i3') and file.endswith('.json'):
                indices.append(file[:-5])
        return indices

    def list_indexed_files(self, index_id: str) -> Iterable[str]:
        """List all documents in the index"""
        return list(self.documents.keys())
