# test_assignment_example.py
"""Test the exact example from the assignment."""

from src.self_indexer import SelfIndexer
from src.preprocessor import preprocess_text
from collections import defaultdict

def create_example_index():
    """Creates test index matching the assignment example."""
    indexer = SelfIndexer()
    
    # Create documents with fruits mentioned in the assignment example
    test_docs = [
        {"doc_id": "doc1", "content": "Apple and Banana are good fruits", "title": "Fruits 1"},
        {"doc_id": "doc2", "content": "Orange is citrus fruit", "title": "Citrus"},
        {"doc_id": "doc3", "content": "Grape wine is popular", "title": "Wine"},
        {"doc_id": "doc4", "content": "Apple pie with Orange", "title": "Recipe 1"},
        {"doc_id": "doc5", "content": "Banana split dessert with Grape", "title": "Recipe 2"},
        {"doc_id": "doc6", "content": "Apple Banana smoothie", "title": "Smoothie"},
        {"doc_id": "doc7", "content": "Orange juice and Grape juice", "title": "Juices"},
    ]
    
    # Build index using proper preprocessing
    indexer.documents = {}
    indexer.inverted_index = defaultdict(list)
    
    for doc in test_docs:
        doc_id = doc['doc_id']
        content = doc.get('content', '')
        indexer.documents[doc_id] = {'title': doc.get('title', 'No Title')}
        
        tokens = preprocess_text(content)
        term_positions = defaultdict(list)
        for i, token in enumerate(tokens):
            term_positions[token].append(i)
        
        for term, positions in term_positions.items():
            indexer.inverted_index[term].append([doc_id, positions])
    
    return indexer

def main():
    indexer = create_example_index()
    
    print("=" * 70)
    print("TESTING ASSIGNMENT EXAMPLE QUERY")
    print("=" * 70)
    
    # Display index
    print("\nTest Documents:")
    for doc_id in sorted(indexer.documents.keys()):
        print(f"  {doc_id}: {indexer.documents[doc_id]['title']}")
    
    print("\nInverted Index (stemmed terms):")
    for term in sorted(indexer.inverted_index.keys()):
        doc_ids = [p[0] for p in indexer.inverted_index[term]]
        print(f"  {term}: {doc_ids}")
    
    # Test the assignment's example query
    assignment_query = '("Apple" AND "Banana") OR ("Orange" AND NOT "Grape")'
    
    print("\n" + "=" * 70)
    print(f"Query: {assignment_query}")
    print("=" * 70)
    
    results = indexer.query(assignment_query)
    
    print(f"\nMatching Documents: {results}")
    print(f"Total matches: {len(results)}")
    
    print("\nDocument Details:")
    for doc_id in results:
        print(f"  {doc_id}: {indexer.documents[doc_id]['title']}")
    
    # Explain the logic
    print("\n" + "=" * 70)
    print("QUERY LOGIC BREAKDOWN:")
    print("=" * 70)
    
    apple_docs = indexer._get_postings('"Apple"')
    banana_docs = indexer._get_postings('"Banana"')
    orange_docs = indexer._get_postings('"Orange"')
    grape_docs = indexer._get_postings('"Grape"')
    
    print(f"\n1. Documents with 'Apple': {sorted(apple_docs)}")
    print(f"2. Documents with 'Banana': {sorted(banana_docs)}")
    print(f"3. 'Apple' AND 'Banana': {sorted(apple_docs & banana_docs)}")
    
    print(f"\n4. Documents with 'Orange': {sorted(orange_docs)}")
    print(f"5. Documents with 'Grape': {sorted(grape_docs)}")
    all_docs = set(indexer.documents.keys())
    print(f"6. NOT 'Grape' (complement): {sorted(all_docs - grape_docs)}")
    print(f"7. 'Orange' AND NOT 'Grape': {sorted(orange_docs & (all_docs - grape_docs))}")
    
    print(f"\n8. Final: ('Apple' AND 'Banana') OR ('Orange' AND NOT 'Grape')")
    print(f"   = {sorted(apple_docs & banana_docs)} OR {sorted(orange_docs & (all_docs - grape_docs))}")
    print(f"   = {sorted((apple_docs & banana_docs) | (orange_docs & (all_docs - grape_docs)))}")

if __name__ == "__main__":
    main()
