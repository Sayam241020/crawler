# test_boolean_parser.py
"""
Comprehensive test suite for the boolean query parser.
Tests various query patterns to ensure correct behavior.
"""

from src.self_indexer import SelfIndexer

def create_test_index():
    """Creates a small test index with known documents using proper preprocessing."""
    from src.preprocessor import preprocess_text
    from collections import defaultdict
    
    indexer = SelfIndexer()
    
    # Create test documents with simple content
    test_docs = [
        {"doc_id": "doc1", "content": "apple banana", "title": "Doc 1"},
        {"doc_id": "doc2", "content": "apple orange", "title": "Doc 2"},
        {"doc_id": "doc3", "content": "banana orange", "title": "Doc 3"},
        {"doc_id": "doc4", "content": "grape apple", "title": "Doc 4"},
        {"doc_id": "doc5", "content": "grape banana orange", "title": "Doc 5"},
    ]
    
    # Build index using the SAME preprocessing as create_index()
    indexer.documents = {}
    indexer.inverted_index = defaultdict(list)
    
    for doc in test_docs:
        doc_id = doc['doc_id']
        content = doc.get('content', '')
        indexer.documents[doc_id] = {'title': doc.get('title', 'No Title')}
        
        # Apply preprocessing exactly as in create_index()
        tokens = preprocess_text(content)
        
        # Build term positions
        term_positions = defaultdict(list)
        for i, token in enumerate(tokens):
            term_positions[token].append(i)
        
        # Add to inverted index
        for term, positions in term_positions.items():
            indexer.inverted_index[term].append([doc_id, positions])
    
    return indexer

def run_tests():
    """Runs comprehensive boolean query tests."""
    indexer = create_test_index()
    
    # Display the test index
    print("=" * 60)
    print("TEST INDEX CONTENTS:")
    print("=" * 60)
    for doc_id, info in sorted(indexer.documents.items()):
        print(f"{doc_id}: {info['title']}")
    print("\nINVERTED INDEX:")
    for term, postings in sorted(indexer.inverted_index.items()):
        doc_ids = [p[0] for p in postings]
        print(f"  {term}: {doc_ids}")
    print("=" * 60)
    
    # Test cases: (query, expected_docs, description)
    test_cases = [
        # Single term queries
        ('"apple"', ['doc1', 'doc2', 'doc4'], "Single term: apple"),
        ('"banana"', ['doc1', 'doc3', 'doc5'], "Single term: banana"),
        ('"grape"', ['doc4', 'doc5'], "Single term: grape"),
        ('"mango"', [], "Single term: non-existent"),
        
        # AND queries
        ('"apple" AND "banana"', ['doc1'], "AND: apple AND banana"),
        ('"apple" AND "orange"', ['doc2'], "AND: apple AND orange"),
        ('"banana" AND "orange"', ['doc3', 'doc5'], "AND: banana AND orange"),
        ('"apple" AND "grape"', ['doc4'], "AND: apple AND grape"),
        
        # OR queries
        ('"apple" OR "grape"', ['doc1', 'doc2', 'doc4', 'doc5'], "OR: apple OR grape"),
        ('"banana" OR "grape"', ['doc1', 'doc3', 'doc4', 'doc5'], "OR: banana OR grape"),
        
        # NOT queries (unary complement)
        ('NOT "apple"', ['doc3', 'doc5'], "NOT: NOT apple"),
        ('NOT "banana"', ['doc2', 'doc4'], "NOT: NOT banana"),
        
        # Combined AND NOT (using NOT as complement, then AND)
        ('"apple" AND NOT "banana"', ['doc2', 'doc4'], "AND NOT: apple AND NOT banana"),
        ('"orange" AND NOT "apple"', ['doc3', 'doc5'], "AND NOT: orange AND NOT apple"),
        
        # Parentheses
        ('("apple" AND "banana") OR "grape"', ['doc1', 'doc4', 'doc5'], "Parentheses: (apple AND banana) OR grape"),
        ('"apple" AND ("banana" OR "orange")', ['doc1', 'doc2'], "Parentheses: apple AND (banana OR orange)"),
        
        # Complex queries
        # (apple AND banana) = {doc1}
        # orange = {doc2, doc3, doc5}, NOT grape = {doc1, doc2, doc3}
        # (orange AND NOT grape) = {doc2, doc3}
        # Result: {doc1} OR {doc2, doc3} = {doc1, doc2, doc3}
        ('("apple" AND "banana") OR ("orange" AND NOT "grape")', ['doc1', 'doc2', 'doc3'], 
         "Complex: (apple AND banana) OR (orange AND NOT grape)"),
        
        ('("apple" OR "banana") AND "orange"', ['doc2', 'doc3', 'doc5'], 
         "Complex: (apple OR banana) AND orange"),
        
        # Precedence tests (NOT > AND > OR)
        ('"apple" OR "banana" AND "orange"', ['doc1', 'doc2', 'doc3', 'doc4', 'doc5'], 
         "Precedence: apple OR (banana AND orange)"),
        
        ('"apple" AND "banana" OR "grape"', ['doc1', 'doc4', 'doc5'], 
         "Precedence: (apple AND banana) OR grape"),
    ]
    
    print("\n" + "=" * 60)
    print("RUNNING TEST CASES:")
    print("=" * 60)
    
    passed = 0
    failed = 0
    
    for query, expected, description in test_cases:
        result = indexer.query(query)
        result_sorted = sorted(result)
        expected_sorted = sorted(expected)
        
        status = "✓ PASS" if result_sorted == expected_sorted else "✗ FAIL"
        
        if result_sorted == expected_sorted:
            passed += 1
            print(f"\n{status}: {description}")
            print(f"  Query: {query}")
            print(f"  Result: {result_sorted}")
        else:
            failed += 1
            print(f"\n{status}: {description}")
            print(f"  Query: {query}")
            print(f"  Expected: {expected_sorted}")
            print(f"  Got:      {result_sorted}")
    
    print("\n" + "=" * 60)
    print(f"SUMMARY: {passed} passed, {failed} failed out of {len(test_cases)} tests")
    print("=" * 60)

if __name__ == "__main__":
    run_tests()
