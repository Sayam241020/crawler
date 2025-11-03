import os
import zipfile
import json
import pandas as pd
import pyarrow.parquet as pq
import itertools
from typing import Iterable, Dict, Optional, List

def load_news_data(folder_path: str, limit: Optional[int] = None) -> Iterable[Dict]:
    doc_id_counter = 0
    limit_reached = False
    print(f"Loading News data from: {folder_path}")
    if not os.path.isdir(folder_path):
        print(f"Warning: News data directory not found at {folder_path}")
        return

    for filename in os.listdir(folder_path):
        if filename.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(folder_path, filename), 'r') as zf:
                for json_file in zf.namelist():
                    with zf.open(json_file) as f:
                        try:
                            data = json.load(f)
                            if data.get('language', '').lower() == 'english' and data.get('text'):
                                yield {
                                    "doc_id": f"news_{doc_id_counter}",
                                    "original_id": data.get('uuid'),
                                    "content": data.get('text', ''),
                                    "title": data.get('title', ''),
                                    "author": data.get('author', ''),
                                    "published_date": data.get('published', None),
                                    "source": "news"
                                }
                                doc_id_counter += 1
                                
                                # Progress indicator
                                if doc_id_counter % 5000 == 0:
                                    print(f"  Loaded {doc_id_counter:,} News documents...")
                                
                                if limit and doc_id_counter >= limit:
                                    print(f"✓ Reached news document limit of {limit:,}.")
                                    limit_reached = True
                                    break
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            pass
                    if limit_reached:
                        break
            if limit_reached:
                break

def load_wiki_data(folder_path: str, limit: Optional[int] = None) -> Iterable[Dict]:
    doc_id_counter = 0
    limit_reached = False
    print(f"Loading Wikipedia data from: {folder_path}")
    if not os.path.isdir(folder_path):
        raise FileNotFoundError(f"Error: Directory not found at {folder_path}")
        
    parquet_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]
    
    for file_num, file_path in enumerate(parquet_files, 1):
        # Read parquet file
        df = pq.read_table(file_path).to_pandas()
        
        # FASTER: Use itertuples() instead of iterrows() (10x faster!)
        for row in df.itertuples():
            yield {
                "doc_id": f"wiki_{row.id}",
                "original_id": row.id,
                "content": row.text,
                "title": row.title,
                "url": row.url,
                "source": "wikipedia"
            }
            doc_id_counter += 1
            
            # Progress indicator
            if doc_id_counter % 5000 == 0:
                print(f"  Loaded {doc_id_counter:,} Wikipedia documents...")
            
            if limit and doc_id_counter >= limit:
                print(f"✓ Reached wiki document limit of {limit:,}.")
                limit_reached = True
                break
        
        if limit_reached:
            break
        else:
            print(f"  Completed file {file_num}/{len(parquet_files)}")

def load_preprocessed_jsonl(filepath: str) -> Iterable[Dict]:
    """
    Load preprocessed documents from JSONL file (FAST - no tokenization needed!)
    
    Each line is: {"doc_id": "wiki_123", "title": "...", "tokens": ["term1", "term2", ...]}
    
    Args:
        filepath: Path to preprocessed JSONL file
        
    Yields:
        Dictionary with doc_id, title, and preprocessed tokens
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Preprocessed file not found: {filepath}")
    
    print(f"Loading preprocessed documents from: {filepath}")
    doc_count = 0
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            doc = json.loads(line.strip())
            yield doc
            doc_count += 1
            
            if doc_count % 10000 == 0:
                print(f"  Loaded {doc_count:,} preprocessed documents...")
    
    print(f"Loaded {doc_count:,} preprocessed documents from cache")


def load_documents_preprocessed(limit: int = 50000) -> Iterable[Dict]:
    """
    Load preprocessed documents from JSONL cache (MUCH FASTER!)
    
    This is the RECOMMENDED way to load documents for indexing.
    Uses cached tokenized documents to avoid repeated preprocessing.
    
    Args:
        limit: Number of documents per source (default: 50000)
        
    Returns:
        Iterator of preprocessed document dictionaries
    """
    wiki_file = f"preprocessed/wiki_{limit}.jsonl"
    news_file = f"preprocessed/news_{limit}.jsonl"
    
    # Check if preprocessed files exist
    if os.path.exists(wiki_file) and os.path.exists(news_file):
        print(f"\n{'='*70}")
        print(f"USING PREPROCESSED CACHE (FAST MODE!)")
        print(f"{'='*70}\n")
        
        wiki_docs = load_preprocessed_jsonl(wiki_file)
        news_docs = load_preprocessed_jsonl(news_file)
        return itertools.chain(wiki_docs, news_docs)
    else:
        print(f"\n{'='*70}")
        print(f"⚠️  Preprocessed files not found. Run preprocessing first:")
        print(f"   python scripts/preprocess_corpus.py --limit {limit}")
        print(f"{'='*70}\n")
        raise FileNotFoundError(f"Preprocessed files not found: {wiki_file}, {news_file}")


def load_documents(limit_per_source: Optional[int] = None) -> Iterable[Dict]:
    """
    Load documents from both Wikipedia and News datasets (OLD SLOW METHOD)
    
    NOTE: Use load_documents_preprocessed() instead for much faster loading!
    
    Args:
        limit_per_source: Optional limit on documents per source
        
    Returns:
        Iterator of document dictionaries
    """
    print(f"\n  Using OLD slow method (re-preprocessing every time)")
    print(f"   Consider using load_documents_preprocessed() instead!\n")
    
    wiki_docs = load_wiki_data("data/wiki/", limit=limit_per_source)
    news_docs = load_news_data("data/News_Datasets/", limit=limit_per_source)
    return itertools.chain(wiki_docs, news_docs)