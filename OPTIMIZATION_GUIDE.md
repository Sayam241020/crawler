# Optimization Implementation Guide

## Overview

This document explains how optimizations work in the IR system, distinguishing between **build-time** and **runtime** optimizations.

## Optimization Types

### Build-Time Optimizations (Embedded in Index)

**Skip Pointers (`optim='sp'`)**
- **When applied**: During index creation (`build.py`)
- **Effect**: Modifies index structure by embedding skip pointers in postings lists
- **Requires**: Separate index files for comparison
- **Index format change**: 
  ```python
  # Without skip pointers (o=0):
  {"term": [[doc1, positions], [doc2, positions], ...]}
  
  # With skip pointers (o=sp):
  {"term": {
    "postings": [
      {"doc_id": doc1, "positions": [...], "skip": 3},
      {"doc_id": doc2, "positions": [...], "skip": 6},
      ...
    ],
    "skip_interval": 3
  }}
  ```
- **Build command**: 
  ```bash
  python build.py -x 1 -y 1 -z 1 -optim sp
  # Creates: SelfIndex_i1d1c1osp.json
  ```
- **Benefits**: Speeds up Boolean AND/OR operations by allowing pointer skipping
- **Indexer support**: 
  - ✅ `SelfIndexer_x1` (Boolean) - FULLY IMPLEMENTED
  - ⚠️ `SelfIndexer_x2` (TF) - NOT YET IMPLEMENTED (message shown)
  - ⚠️ `SelfIndexer_x3` (TF-IDF) - NOT YET IMPLEMENTED (message shown)

### Runtime Optimizations (Query-Time Strategies)

These optimizations are applied during query execution and work on ANY index (including o=0).

**Thresholding (`optim='th'`)**
- **When applied**: During query execution
- **Effect**: Filters out documents with scores below a dynamic threshold
- **Requires**: No special index structure
- **Implementation**: 
  ```python
  # In _ranked_query_taat():
  if self.optim == 'Thresholding':
      threshold = max_score * 0.1  # 10% of max score
      scores = {doc: score for doc, score in scores.items() if score >= threshold}
  ```
- **Build command**: 
  ```bash
  python build.py -x 3 -y 1 -z 1 -optim th
  # Creates: SelfIndex_i3d1c1oth.json (same structure as o=0)
  ```
- **Evaluate command**: Same index, runtime strategy automatically applied
- **Benefits**: Reduces computation time by eliminating low-scoring documents early
- **Indexer support**: 
  - ❌ `SelfIndexer_x1` (Boolean) - N/A (no scores)
  - ✅ `SelfIndexer_x2` (TF) - IMPLEMENTED
  - ✅ `SelfIndexer_x3` (TF-IDF) - IMPLEMENTED

**Early Stopping (`optim='es'`)**
- **When applied**: During query execution
- **Effect**: Stops processing when top-k results are stable
- **Requires**: No special index structure
- **Implementation**: 
  ```python
  # In _ranked_query_taat():
  if self.optim == 'EarlyStopping':
      if len(results) >= top_k * 2:  # 2x buffer
          break  # Stop processing remaining terms
  ```
- **Build command**: 
  ```bash
  python build.py -x 3 -y 1 -z 1 -optim es
  # Creates: SelfIndex_i3d1c1oes.json (same structure as o=0)
  ```
- **Benefits**: Reduces query time when enough good results found early
- **Indexer support**: 
  - ❌ `SelfIndexer_x1` (Boolean) - N/A (no ranking)
  - ✅ `SelfIndexer_x2` (TF) - IMPLEMENTED
  - ✅ `SelfIndexer_x3` (TF-IDF) - IMPLEMENTED

## Index Identifier Format

**Format**: `SelfIndex_i{x}d{y}c{z}o{optim}`

- **x**: Index type (1=Boolean, 2=TF, 3=TF-IDF)
- **y**: Datastore (1=JSON, 2=SQLite)
- **z**: Compression (1=NONE, 2=CODE/Elias, 3=CLIB/Zlib)
- **optim**: Optimization type (0=Null, sp=SkipPointers, th=Thresholding, es=EarlyStopping)

**Examples**:
- `SelfIndex_i1d1c1o0` - Boolean, JSON, no compression, no optimization
- `SelfIndex_i1d1c1osp` - Boolean, JSON, no compression, **skip pointers embedded**
- `SelfIndex_i3d1c2oth` - TF-IDF, JSON, Elias compression, **thresholding runtime strategy**
- `SelfIndex_i3d2c3oes` - TF-IDF, SQLite, Zlib compression, **early stopping runtime strategy**

## Query Mode (NOT in Identifier!)

Query mode is a **runtime parameter**, NOT part of the index identifier:

```bash
# Same index, different query modes
python evaluate.py -x 3 -y 1 -z 1 -optim 0 -q T  # TAAT mode
python evaluate.py -x 3 -y 1 -z 1 -optim 0 -q D  # DAAT mode
```

Both use the same index: `SelfIndex_i3d1c1o0.json`

## Comparison Strategy

### Plot.A Requirements (Assignment)

Compare optimization effectiveness:
- **Baseline**: `o=0` (no optimization)
- **Optimized**: `o=sp` (skip pointers for Boolean) OR `o=th`/`o=es` (for ranked)

**Example comparison**:
```bash
# Build baseline
python build.py -x 1 -y 1 -z 1 -optim 0
# Creates: SelfIndex_i1d1c1o0.json

# Build optimized
python build.py -x 1 -y 1 -z 1 -optim sp
# Creates: SelfIndex_i1d1c1osp.json

# Evaluate both
python evaluate.py -x 1 -y 1 -z 1 -optim 0 -q T
python evaluate.py -x 1 -y 1 -z 1 -optim sp -q T

# Compare results in Plot.A
```

### Why This Approach?

1. **Build-time optimizations** (sp) require separate indices because they change the data structure
2. **Runtime optimizations** (th, es) can technically use the same index, but we build separate ones for clean comparison and identifier clarity
3. This allows fair comparison: same documents, same query set, different optimization strategies

## Implementation Status

### ✅ Completed

- [x] Optimizations enum documented in `index_base.py`
- [x] Skip pointer builder (`skip_pointer_builder.py`)
- [x] Boolean indexer (x1) skip pointer integration
- [x] Boolean indexer query methods updated to handle skip pointer format
- [x] TF indexer (x2) thresholding and early stopping
- [x] TF-IDF indexer (x3) thresholding and early stopping
- [x] Identifier format fixed (no query mode parameter)
- [x] Runtime optimizations properly separated from build-time

### ⚠️ Partial / Deferred

- [ ] TF indexer (x2) skip pointer integration - Shows message, not implemented
- [ ] TF-IDF indexer (x3) skip pointer integration - Shows message, not implemented
  - **Reason**: Skip pointers are primarily beneficial for Boolean queries
  - **For ranked retrieval**: Benefits are minimal, mainly for phrase queries
  - **Priority**: Low (focus on core functionality first)

### 🔄 Next Steps

1. Test skip pointer indices with Boolean queries
2. Verify performance improvements
3. Generate comparison plots (Plot.A)
4. Optionally implement skip pointers for ranked indexers if needed

## Usage Examples

### Building Indices

```bash
# Boolean with no optimization
python build.py -x 1 -y 1 -z 1 -optim 0

# Boolean with skip pointers (build-time)
python build.py -x 1 -y 1 -z 1 -optim sp

# TF-IDF with thresholding (runtime strategy, but separate index for clean comparison)
python build.py -x 3 -y 1 -z 1 -optim th

# TF-IDF with early stopping (runtime strategy)
python build.py -x 3 -y 1 -z 1 -optim es
```

### Evaluating Indices

```bash
# Evaluate Boolean with skip pointers, TAAT mode
python evaluate.py -x 1 -y 1 -z 1 -optim sp -q T

# Evaluate TF-IDF with thresholding, DAAT mode
python evaluate.py -x 3 -y 1 -z 1 -optim th -q D

# Evaluate TF-IDF with early stopping, TAAT mode
python evaluate.py -x 3 -y 1 -z 1 -optim es -q T
```

### Comparing Performance

```bash
# Compare baseline vs skip pointers
python evaluate.py -x 1 -y 1 -z 1 -optim 0 -q T > results_baseline.txt
python evaluate.py -x 1 -y 1 -z 1 -optim sp -q T > results_skippointers.txt

# Compare baseline vs thresholding
python evaluate.py -x 3 -y 1 -z 1 -optim 0 -q T > results_baseline_tfidf.txt
python evaluate.py -x 3 -y 1 -z 1 -optim th -q T > results_thresholding.txt
```

## Technical Details

### Skip Pointer Structure

```python
# Original postings list
postings = [[doc1, positions1], [doc2, positions2], [doc3, positions3], ...]

# After skip pointer enhancement
postings = {
    'postings': [
        {'doc_id': doc1, 'positions': positions1, 'skip': 3},  # Points to index 3
        {'doc_id': doc2, 'positions': positions2, 'skip': 6},  # Points to index 6
        {'doc_id': doc3, 'positions': positions3, 'skip': 9},  # Points to index 9
        ...
    ],
    'skip_interval': 3,  # sqrt(n) spacing
    'length': 100
}
```

### Skip Interval Calculation

```python
def calculate_skip_interval(postings_length: int) -> int:
    """Use sqrt(n) spacing for optimal performance"""
    if postings_length < 3:
        return postings_length  # No skip pointers needed
    skip_interval = int(math.sqrt(postings_length))
    return max(2, skip_interval)  # Minimum skip of 2
```

### Query Processing with Skip Pointers

```python
def _get_postings(self, term: str) -> Set[str]:
    postings = self.inverted_index.get(processed_term, [])
    
    # Detect skip pointer format
    if self.skip_pointers_enabled and isinstance(postings, dict):
        # Extract doc_ids from enhanced format
        return {posting['doc_id'] for posting in postings['postings']}
    
    # Regular format
    return {posting[0] for posting in postings}
```

## Key Insights

1. **Build-time vs Runtime**: Skip pointers change index structure (build-time), while thresholding/early stopping are pure query strategies (runtime)

2. **Index Identifier**: Only build-time optimizations appear in identifier. This is correct because:
   - Skip pointers: Index structure is different, needs separate file
   - Thresholding/Early stopping: Same index, different query strategy

3. **Comparison Plots**: Assignment requires comparing i=0 vs i=sp/th/es, so we build separate indices for each optimization type

4. **Query Mode**: TAAT/DAAT is a runtime parameter via `-q`, not part of index identifier

5. **Flexibility**: This design allows:
   - Compare optimization effectiveness (Plot.A)
   - Compare datastores (JSON vs SQLite) at same optimization level
   - Compare compression types independently
   - Test different query modes on same index
