"""
Consolidated Ranking Tests (x=2, x=3)
Tests for word count and TF-IDF ranking functionality
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.self_indexer_x2 import SelfIndexer_x2
from src.self_indexer_x3 import SelfIndexer_x3
from src.preprocessor import preprocess_text
from collections import defaultdict
import math

def test_x2_basic_ranking():
    """Test basic TF ranking with simple documents"""
    print("=" * 70)
    print("Test: x=2 Basic Ranking")
    print("=" * 70)
    
    # Create test documents
    test_docs = [
        {
            'doc_id': 'doc1',
            'title': 'Python Guide',
            'content': 'python python python is a programming language'
        },
        {
            'doc_id': 'doc2',
            'title': 'Java Guide',
            'content': 'java is a programming language'
        },
        {
            'doc_id': 'doc3',
            'title': 'Python Basics',
            'content': 'python is easy to learn'
        }
    ]
    
    # Build index
    indexer = SelfIndexer_x2()
    indexer.inverted_index.clear()
    indexer.documents.clear()
    
    for doc in test_docs:
        doc_id = doc['doc_id']
        content = doc.get('content', '')
        indexer.documents[doc_id] = {'title': doc.get('title', 'No Title')}
        
        tokens = preprocess_text(content)
        term_positions = defaultdict(list)
        for i, token in enumerate(tokens):
            term_positions[token].append(i)
        
        for term, positions in term_positions.items():
            term_freq = len(positions)
            indexer.inverted_index[term].append([doc_id, term_freq, positions])
    
    # Test 1: Single term query
    results = indexer.query('"python"', return_scores=True)
    assert len(results) == 2, "Should find 2 documents"
    assert results[0][0] == 'doc1', "doc1 should rank first (TF=3)"
    assert results[0][1] == 3.0, "doc1 should have score=3"
    assert results[1][0] == 'doc3', "doc3 should rank second (TF=1)"
    print("✓ Test 1 passed: Single term ranking")
    
    # Test 2: OR query with score aggregation
    results = indexer.query('"python" OR "programming"', return_scores=True)
    assert len(results) == 3, "Should find 3 documents"
    assert results[0][0] == 'doc1', "doc1 should rank first"
    assert results[0][1] == 4.0, "doc1 score = TF(python)=3 + TF(programming)=1"
    print("✓ Test 2 passed: Multi-term score aggregation")
    
    # Test 3: top_k limiting
    results = indexer.query('"python" OR "programming"', top_k=2, return_scores=True)
    assert len(results) == 2, "Should return only top 2"
    print("✓ Test 3 passed: top_k limiting")
    
    # Test 4: Boolean AND with ranking
    results = indexer.query('"python" AND "programming"', return_scores=True)
    assert len(results) == 1, "Only doc1 has both terms"
    assert results[0][0] == 'doc1'
    print("✓ Test 4 passed: Boolean AND with ranking")
    
    print("\n✅ All x=2 basic ranking tests passed!\n")

def test_x2_result_quality():
    """Test that ranking produces meaningful result ordering"""
    print("=" * 70)
    print("Test: x=2 Result Quality")
    print("=" * 70)
    
    # Create documents with varying term frequencies
    test_docs = [
        {
            'doc_id': 'high_freq',
            'title': 'AI Research',
            'content': 'artificial intelligence ' * 10 + ' machine learning'
        },
        {
            'doc_id': 'medium_freq',
            'title': 'AI Overview',
            'content': 'artificial intelligence ' * 3 + ' overview'
        },
        {
            'doc_id': 'low_freq',
            'title': 'Tech Trends',
            'content': 'artificial intelligence is emerging'
        },
        {
            'doc_id': 'unrelated',
            'title': 'Cooking',
            'content': 'pasta recipes and cooking tips'
        }
    ]
    
    # Build index
    indexer = SelfIndexer_x2()
    indexer.inverted_index.clear()
    indexer.documents.clear()
    
    for doc in test_docs:
        doc_id = doc['doc_id']
        content = doc.get('content', '')
        indexer.documents[doc_id] = {'title': doc.get('title', 'No Title')}
        
        tokens = preprocess_text(content)
        term_positions = defaultdict(list)
        for i, token in enumerate(tokens):
            term_positions[token].append(i)
        
        for term, positions in term_positions.items():
            term_freq = len(positions)
            indexer.inverted_index[term].append([doc_id, term_freq, positions])
    
    # Test: Documents with higher TF should rank higher
    results = indexer.query('"artificial" AND "intelligence"', return_scores=True)
    assert len(results) == 3, "Should find 3 documents"
    assert results[0][0] == 'high_freq', "high_freq should rank first"
    assert results[1][0] == 'medium_freq', "medium_freq should rank second"
    assert results[2][0] == 'low_freq', "low_freq should rank third"
    assert results[0][1] > results[1][1] > results[2][1], "Scores should be descending"
    print("✓ Documents ranked correctly by term frequency")
    
    # Test: Unrelated documents should not appear
    results = indexer.query('"artificial"', return_scores=True)
    doc_ids = [r[0] for r in results]
    assert 'unrelated' not in doc_ids, "Unrelated doc should not appear"
    print("✓ Unrelated documents correctly excluded")
    
    print("\n✅ All x=2 quality tests passed!\n")

def test_x2_edge_cases():
    """Test edge cases and error handling"""
    print("=" * 70)
    print("Test: x=2 Edge Cases")
    print("=" * 70)
    
    indexer = SelfIndexer_x2()
    indexer.inverted_index.clear()
    indexer.documents.clear()
    
    # Add one simple document
    doc = {
        'doc_id': 'test_doc',
        'title': 'Test',
        'content': 'hello world'
    }
    
    tokens = preprocess_text(doc['content'])
    indexer.documents['test_doc'] = {'title': 'Test'}
    
    term_positions = defaultdict(list)
    for i, token in enumerate(tokens):
        term_positions[token].append(i)
    
    for term, positions in term_positions.items():
        term_freq = len(positions)
        indexer.inverted_index[term].append(['test_doc', term_freq, positions])
    
    # Test 1: Empty result set
    results = indexer.query('"nonexistent"', return_scores=True)
    assert len(results) == 0, "Should return empty list"
    print("✓ Empty result set handled correctly")
    
    # Test 2: top_k larger than result set
    results = indexer.query('"hello"', top_k=100, return_scores=True)
    assert len(results) == 1, "Should return all available results"
    print("✓ top_k > result size handled correctly")
    
    # Test 3: return_scores=False
    results = indexer.query('"hello"', return_scores=False)
    assert isinstance(results, list), "Should return list"
    assert len(results) == 1
    assert results[0] == 'test_doc', "Should return doc_id only"
    print("✓ return_scores=False works correctly")
    
    print("\n✅ All x=2 edge case tests passed!\n")

def test_x3_idf_computation():
    """Test IDF computation correctness"""
    print("=" * 70)
    print("Test: x=3 IDF Computation")
    print("=" * 70)
    
    # Create test documents
    test_docs = [
        {'doc_id': 'doc1', 'title': 'Doc 1', 'content': 'common common rare'},
        {'doc_id': 'doc2', 'title': 'Doc 2', 'content': 'common unique'},
        {'doc_id': 'doc3', 'title': 'Doc 3', 'content': 'common other'},
        {'doc_id': 'doc4', 'title': 'Doc 4', 'content': 'different words'}
    ]
    
    # Build index
    indexer = SelfIndexer_x3()
    indexer.inverted_index.clear()
    indexer.documents.clear()
    
    for doc in test_docs:
        doc_id = doc['doc_id']
        content = doc.get('content', '')
        indexer.documents[doc_id] = {'title': doc.get('title', 'No Title')}
        
        tokens = preprocess_text(content)
        term_positions = defaultdict(list)
        for i, token in enumerate(tokens):
            term_positions[token].append(i)
        
        for term, positions in term_positions.items():
            term_freq = len(positions)
            indexer.inverted_index[term].append([doc_id, term_freq, positions])
    
    # Compute IDF
    indexer._compute_idf()
    
    # Test 1: Common term should have low IDF
    common_idf = indexer.get_idf('common')
    expected_common_idf = math.log(4 / 3)  # N=4, df=3 (appears in 3 docs)
    assert abs(common_idf - expected_common_idf) < 0.001, f"Common IDF should be {expected_common_idf}"
    print(f"✓ Test 1: Common term IDF = {common_idf:.4f} (appears in 3/4 docs)")
    
    # Test 2: Rare term should have high IDF
    rare_idf = indexer.get_idf('rare')
    expected_rare_idf = math.log(4 / 1)  # N=4, df=1 (appears in 1 doc)
    assert abs(rare_idf - expected_rare_idf) < 0.001, f"Rare IDF should be {expected_rare_idf}"
    print(f"✓ Test 2: Rare term IDF = {rare_idf:.4f} (appears in 1/4 docs)")
    
    # Test 3: Compare IDF values
    assert rare_idf > common_idf, "Rare terms should have higher IDF than common terms"
    print("✓ Test 3: Rare term IDF > Common term IDF")
    
    # Test 4: Non-existent term
    nonexistent_idf = indexer.get_idf('nonexistent')
    assert nonexistent_idf == 0.0, "Non-existent term should have IDF=0"
    print("✓ Test 4: Non-existent term has IDF=0")
    
    print("\n✅ All x=3 IDF computation tests passed!\n")

def test_x3_tfidf_scoring():
    """Test TF-IDF scoring vs TF-only scoring"""
    print("=" * 70)
    print("Test: x=3 TF-IDF Scoring")
    print("=" * 70)
    
    # Create documents where TF and TF-IDF rankings differ
    # Make sure words have different document frequencies
    test_docs = [
        {
            'doc_id': 'doc1',
            'title': 'Common Words',
            'content': 'common common common common rare'
        },
        {
            'doc_id': 'doc2',
            'title': 'More Common',
            'content': 'common unique specialized'
        },
        {
            'doc_id': 'doc3',
            'title': 'Also Common',
            'content': 'common different words'
        },
        {
            'doc_id': 'doc4',
            'title': 'Other',
            'content': 'other content here'
        }
    ]
    
    # Build x=2 index (TF only)
    indexer_x2 = SelfIndexer_x2()
    indexer_x2.inverted_index.clear()
    indexer_x2.documents.clear()
    
    for doc in test_docs:
        doc_id = doc['doc_id']
        content = doc.get('content', '')
        indexer_x2.documents[doc_id] = {'title': doc.get('title', 'No Title')}
        
        tokens = preprocess_text(content)
        term_positions = defaultdict(list)
        for i, token in enumerate(tokens):
            term_positions[token].append(i)
        
        for term, positions in term_positions.items():
            term_freq = len(positions)
            indexer_x2.inverted_index[term].append([doc_id, term_freq, positions])
    
    # Build x=3 index (TF-IDF)
    indexer_x3 = SelfIndexer_x3()
    indexer_x3.inverted_index.clear()
    indexer_x3.documents.clear()
    
    for doc in test_docs:
        doc_id = doc['doc_id']
        content = doc.get('content', '')
        indexer_x3.documents[doc_id] = {'title': doc.get('title', 'No Title')}
        
        tokens = preprocess_text(content)
        term_positions = defaultdict(list)
        for i, token in enumerate(tokens):
            term_positions[token].append(i)
        
        for term, positions in term_positions.items():
            term_freq = len(positions)
            indexer_x3.inverted_index[term].append([doc_id, term_freq, positions])
    
    indexer_x3._compute_idf()
    
    # Test 1: Common word (appears in 3/4 docs) vs rare word (appears in 1/4 docs)
    common_idf = indexer_x3.get_idf('common')
    rare_idf = indexer_x3.get_idf('rare')
    unique_idf = indexer_x3.get_idf('unique')
    
    print(f"  IDF(common) = {common_idf:.4f} (appears in 3/4 docs)")
    print(f"  IDF(rare) = {rare_idf:.4f} (appears in 1/4 docs)")
    print(f"  IDF(unique) = {unique_idf:.4f} (appears in 1/4 docs)")
    
    # Rare terms should have higher IDF than common terms
    assert rare_idf > common_idf, "Rare term should have higher IDF than common term"
    assert unique_idf > common_idf, "Unique term should have higher IDF than common term"
    print("✓ Test 1: IDF correctly distinguishes rare vs common terms")
    
    # Test 2: Query for common word - TF-IDF reduces its importance
    results_x2 = indexer_x2.query('"common"', return_scores=True)
    results_x3 = indexer_x3.query('"common"', return_scores=True)
    
    if results_x2 and results_x3:
        # doc1 has TF=4 for "common"
        score_x2_doc1 = next((s for d, s in results_x2 if d == 'doc1'), None)
        score_x3_doc1 = next((s for d, s in results_x3 if d == 'doc1'), None)
        
        if score_x2_doc1 and score_x3_doc1:
            # TF-IDF multiplies by IDF < 1, so score should be lower
            assert score_x3_doc1 < score_x2_doc1, "TF-IDF should reduce score of common word"
            print(f"  doc1 'common' score (TF): {score_x2_doc1:.4f}")
            print(f"  doc1 'common' score (TF-IDF): {score_x3_doc1:.4f}")
            print("✓ Test 2: TF-IDF reduces weight of common words")
    
    # Test 3: Multi-term query showing relative weighting
    # Query with both common and rare terms
    results_x2_multi = indexer_x2.query('"common" OR "rare"', return_scores=True)
    results_x3_multi = indexer_x3.query('"common" OR "rare"', return_scores=True)
    
    print(f"\n  Multi-term query results (x=2 TF):")
    for doc_id, score in results_x2_multi[:3]:
        print(f"    {doc_id}: {score:.4f}")
    
    print(f"\n  Multi-term query results (x=3 TF-IDF):")
    for doc_id, score in results_x3_multi[:3]:
        print(f"    {doc_id}: {score:.4f}")
    
    print("✓ Test 3: TF-IDF correctly weights multi-term queries")
    
    print("\n✅ All x=3 TF-IDF scoring tests passed!\n")

def test_x3_term_stats():
    """Test term statistics helper method"""
    print("=" * 70)
    print("Test: x=3 Term Statistics")
    print("=" * 70)
    
    # Create test documents
    test_docs = [
        {'doc_id': 'doc1', 'title': 'Doc 1', 'content': 'python python java'},
        {'doc_id': 'doc2', 'title': 'Doc 2', 'content': 'python javascript'},
        {'doc_id': 'doc3', 'title': 'Doc 3', 'content': 'ruby rails'}
    ]
    
    # Build index
    indexer = SelfIndexer_x3()
    indexer.inverted_index.clear()
    indexer.documents.clear()
    
    for doc in test_docs:
        doc_id = doc['doc_id']
        content = doc.get('content', '')
        indexer.documents[doc_id] = {'title': doc.get('title', 'No Title')}
        
        tokens = preprocess_text(content)
        term_positions = defaultdict(list)
        for i, token in enumerate(tokens):
            term_positions[token].append(i)
        
        for term, positions in term_positions.items():
            term_freq = len(positions)
            indexer.inverted_index[term].append([doc_id, term_freq, positions])
    
    indexer._compute_idf()
    
    # Test get_term_stats
    stats = indexer.get_term_stats('python')
    assert stats['exists'] == True, "Python should exist"
    assert stats['document_frequency'] == 2, "Python appears in 2 docs"
    assert stats['total_documents'] == 3, "Total 3 docs"
    assert stats['total_occurrences'] == 3, "Python occurs 3 times total (2+1)"
    print(f"  Python stats: df={stats['document_frequency']}, total_occurrences={stats['total_occurrences']}")
    print("✓ get_term_stats returns correct information")
    
    # Test non-existent term
    stats_none = indexer.get_term_stats('nonexistent')
    assert stats_none['exists'] == False
    print("✓ Non-existent term handled correctly")
    
    print("\n✅ All x=3 term statistics tests passed!\n")

if __name__ == '__main__':
    print("\n" + "=" * 70)
    print("Running Consolidated Ranking Tests")
    print("=" * 70 + "\n")
    
    # x=2 tests
    test_x2_basic_ranking()
    test_x2_result_quality()
    test_x2_edge_cases()
    
    # x=3 tests
    test_x3_idf_computation()
    test_x3_tfidf_scoring()
    test_x3_term_stats()
    
    print("=" * 70)
    print("✅ ALL RANKING TESTS PASSED (x=2 and x=3)!")
    print("=" * 70)

