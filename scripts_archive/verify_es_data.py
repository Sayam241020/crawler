"""
Verify that we're loading the correct data for Elasticsearch
"""
import sys
sys.path.insert(0, '.')

from src.data_loader import load_wiki_data, load_news_data
import itertools

print("=" * 70)
print("VERIFYING DATA FOR ELASTICSEARCH")
print("=" * 70)
print()

# Load one wiki document
print("Loading 1 Wikipedia document...")
wiki_gen = load_wiki_data('data/wiki', limit=1)
wiki_doc = next(wiki_gen)

print("\nWikipedia Document Sample:")
print(f"  doc_id: {wiki_doc.get('doc_id')}")
print(f"  title: {wiki_doc.get('title')}")
print(f"  content (first 200 chars): {wiki_doc.get('content', '')[:200]}...")
print(f"  Has 'content' field: {'content' in wiki_doc}")
print(f"  Has 'tokens' field: {'tokens' in wiki_doc}")

# Load one news document
print("\nLoading 1 News document...")
news_gen = load_news_data('data/News_Datasets', limit=1)
news_doc = next(news_gen)

print("\nNews Document Sample:")
print(f"  doc_id: {news_doc.get('doc_id')}")
print(f"  title: {wiki_doc.get('title')}")
print(f"  content (first 200 chars): {news_doc.get('content', '')[:200]}...")
print(f"  Has 'content' field: {'content' in news_doc}")
print(f"  Has 'tokens' field: {'tokens' in news_doc}")

print()
print("=" * 70)
if 'content' in wiki_doc and 'content' in news_doc:
    if 'tokens' not in wiki_doc and 'tokens' not in news_doc:
        print("✓ CORRECT: Loading ORIGINAL text (not preprocessed tokens)")
        print("✓ Elasticsearch will apply its own preprocessing")
    else:
        print("⚠ WARNING: Data has 'tokens' field - might be preprocessed")
else:
    print("✗ ERROR: Data missing 'content' field!")
print("=" * 70)
