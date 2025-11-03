# Phase 1 Index Reference Guide

## Overview
This document describes the 7 core indices built in Phase 1 and how they map to assignment requirements.

## Dataset
- **Source**: Wikipedia + News articles
- **Size**: 100,000 documents (50K each)
- **Preprocessed**: Yes (stemming, stopword removal)
- **Location**: `preprocessed_data/`

## Index Identifier Format
`SelfIndex_i{x}d{y}c{z}o{optim}`

- **x**: Index type (1=Boolean, 2=TF, 3=TF-IDF)
- **y**: Datastore (1=JSON, 2=SQLite)
- **z**: Compression (1=None, 2=Elias, 3=Zlib)
- **optim**: Optimization (0=None, sp=SkipPointers, th=Thresholding, es=EarlyStopping)

## The 7 Core Indices

### 1. SelfIndex_i1d1c1o0 (Boolean Baseline)
```
Index Type:     Boolean (x=1)
Datastore:      JSON (y=1)
Compression:    None (z=1)
Optimization:   None (o=0)
```
**Purpose**: 
- Baseline for Boolean queries
- Plot.C comparison (index type)
- Plot.A comparison (optimization baseline)

**File Size**: ~50-100 MB
**Build Time**: ~5 minutes
**Query Time**: 0.5-2 ms

---

### 2. SelfIndex_i2d1c1o0 (TF Baseline)
```
Index Type:     TF/Word Count (x=2)
Datastore:      JSON (y=1)
Compression:    None (z=1)
Optimization:   None (o=0)
```
**Purpose**: 
- Plot.C comparison (index type)
- Demonstrates ranking with word counts

**File Size**: ~80-150 MB
**Build Time**: ~7 minutes
**Query Time**: 1-3 ms

---

### 3. SelfIndex_i3d1c1o0 (TF-IDF Baseline)
```
Index Type:     TF-IDF (x=3)
Datastore:      JSON (y=1)
Compression:    None (z=1)
Optimization:   None (o=0)
```
**Purpose**: 
- Plot.C comparison (index type)
- Plot.A comparison (datastore baseline)
- Plot.AB comparison (compression baseline)
- Plot.AC comparison (query mode baseline)
- **Most important baseline index**

**File Size**: ~100-200 MB
**Build Time**: ~10 minutes
**Query Time**: 2-5 ms

---

### 4. SelfIndex_i3d2c1o0 (TF-IDF with SQLite)
```
Index Type:     TF-IDF (x=3)
Datastore:      SQLite (y=2)
Compression:    None (z=1)
Optimization:   None (o=0)
```
**Purpose**: 
- Plot.A comparison (datastore: JSON vs SQLite)

**File Size**: ~120-250 MB (SQLite overhead)
**Build Time**: ~12 minutes
**Query Time**: 3-7 ms (disk I/O)

**Comparison Notes**:
- SQLite: Better for concurrent access, ACID guarantees
- JSON: Faster single-threaded reads, simpler

---

### 5. SelfIndex_i3d1c2o0 (TF-IDF with Elias Compression)
```
Index Type:     TF-IDF (x=3)
Datastore:      JSON (y=1)
Compression:    Elias Gamma/Delta (z=2)
Optimization:   None (o=0)
```
**Purpose**: 
- Plot.AB comparison (compression: custom code)

**File Size**: ~30-60 MB (3-4x compression ratio)
**Build Time**: ~8 minutes (compression overhead)
**Query Time**: 3-6 ms (decompression overhead)

**Compression Details**:
- Algorithm: Elias Gamma/Delta encoding
- Best for: Small integers (doc IDs, positions)
- Custom implementation in `src/compression/elias.py`

---

### 6. SelfIndex_i3d1c3o0 (TF-IDF with Zlib Compression)
```
Index Type:     TF-IDF (x=3)
Datastore:      JSON (y=1)
Compression:    Zlib (z=3)
Optimization:   None (o=0)
```
**Purpose**: 
- Plot.AB comparison (compression: library-based)

**File Size**: ~20-50 MB (4-5x compression ratio)
**Build Time**: ~8 minutes
**Query Time**: 2-5 ms (faster decompression than Elias)

**Compression Details**:
- Algorithm: Zlib (DEFLATE algorithm)
- Best for: General-purpose compression
- Library: Python's `zlib` module

---

### 7. SelfIndex_i1d1c1osp (Boolean with Skip Pointers)
```
Index Type:     Boolean (x=1)
Datastore:      JSON (y=1)
Compression:    None (z=1)
Optimization:   Skip Pointers (o=sp)
```
**Purpose**: 
- Plot.A comparison (optimization: i=0 vs i=sp)

**File Size**: ~60-120 MB (skip pointer overhead)
**Build Time**: ~6 minutes
**Query Time**: 0.3-1.5 ms (faster Boolean operations)

**Optimization Details**:
- Skip pointers embedded during build
- Sqrt(n) spacing for optimal performance
- Most effective for Boolean AND/OR queries

---

## Assignment Plot Mapping

### Plot.C: Memory Footprint (Index Types)
**Compare**: x=1, x=2, x=3 (all with y=1, z=1, o=0)

| Index | Type | File Size | RAM Usage |
|-------|------|-----------|-----------|
| i1d1c1o0 | Boolean | ~50-100 MB | ~200-300 MB |
| i2d1c1o0 | TF | ~80-150 MB | ~300-400 MB |
| i3d1c1o0 | TF-IDF | ~100-200 MB | ~400-500 MB |

**Expected Result**: TF-IDF > TF > Boolean

---

### Plot.A: Datastore Comparison
**Compare**: y=1 vs y=2 (both with x=3, z=1, o=0)

| Index | Datastore | Latency (p50) | Throughput |
|-------|-----------|---------------|------------|
| i3d1c1o0 | JSON | ~2-3 ms | ~300-400 q/s |
| i3d2c1o0 | SQLite | ~3-5 ms | ~200-300 q/s |

**Expected Result**: JSON faster for single-threaded, SQLite better for concurrent

---

### Plot.AB: Compression Comparison
**Compare**: z=1, z=2, z=3 (all with x=3, y=1, o=0)

| Index | Compression | File Size | Query Time | Compression Ratio |
|-------|-------------|-----------|------------|-------------------|
| i3d1c1o0 | None | ~150 MB | ~2-3 ms | 1.0x |
| i3d1c2o0 | Elias | ~40 MB | ~4-5 ms | 3.7x |
| i3d1c3o0 | Zlib | ~30 MB | ~3-4 ms | 5.0x |

**Expected Result**: Better compression = smaller size but slightly slower queries

---

### Plot.A: Optimization Comparison
**Compare**: o=0 vs o=sp (both with x=1, y=1, z=1)

| Index | Optimization | Query Time | Speedup |
|-------|--------------|------------|---------|
| i1d1c1o0 | None | ~1.5 ms | Baseline |
| i1d1c1osp | Skip Pointers | ~0.8 ms | ~1.9x |

**Expected Result**: Skip pointers speed up Boolean queries

---

### Plot.AC: Query Mode Comparison
**Compare**: q=T (TAAT) vs q=D (DAAT) - same index i3d1c1o0

| Query Mode | Latency (p50) | Throughput |
|------------|---------------|------------|
| TAAT (-q T) | ~2-3 ms | ~350 q/s |
| DAAT (-q D) | ~2-4 ms | ~300 q/s |

**Expected Result**: Similar performance, DAAT might be slightly faster for top-k

---

## Build Commands

```bash
# PowerShell
.\build_phase1.ps1

# Bash (Linux/Mac)
chmod +x build_phase1.sh
./build_phase1.sh
```

## Evaluation Commands

```bash
# PowerShell
.\evaluate_phase1.ps1

# Individual evaluations
python evaluate.py -x 1 -y 1 -z 1 -optim 0 -q T
python evaluate.py -x 2 -y 1 -z 1 -optim 0 -q T
python evaluate.py -x 3 -y 1 -z 1 -optim 0 -q T
python evaluate.py -x 3 -y 2 -z 1 -optim 0 -q T
python evaluate.py -x 3 -y 1 -z 2 -optim 0 -q T
python evaluate.py -x 3 -y 1 -z 3 -optim 0 -q T
python evaluate.py -x 1 -y 1 -z 1 -optim sp -q T

# Query mode comparison (same index, different mode)
python evaluate.py -x 3 -y 1 -z 1 -optim 0 -q D
```

## Expected Outputs

After building all indices:
```
SelfIndex_i1d1c1o0.json    (~50-100 MB)
SelfIndex_i2d1c1o0.json    (~80-150 MB)
SelfIndex_i3d1c1o0.json    (~100-200 MB)
SelfIndex_i3d2c1o0.db      (~120-250 MB - SQLite)
SelfIndex_i3d1c2o0.json    (~30-60 MB - compressed)
SelfIndex_i3d1c3o0.json    (~20-50 MB - compressed)
SelfIndex_i1d1c1osp.json   (~60-120 MB - with skip pointers)
```

After evaluations:
```
results_SelfIndex_i1d1c1o0.json
results_SelfIndex_i2d1c1o0.json
results_SelfIndex_i3d1c1o0.json
results_SelfIndex_i3d2c1o0.json
results_SelfIndex_i3d1c2o0.json
results_SelfIndex_i3d1c3o0.json
results_SelfIndex_i1d1c1osp.json
```

## Next Steps

1. **Build indices**: Run `build_phase1.ps1` (~1 hour)
2. **Evaluate indices**: Run `evaluate_phase1.ps1` (~10 minutes)
3. **Generate plots**: Create plotting scripts for each comparison
4. **Build Elasticsearch**: Set up ESIndex-v1.0 for final comparison
5. **Word frequency plots**: Run `data_analysis.ipynb`
6. **Ground truth**: Create precision/recall test data

## Troubleshooting

**Memory issues?**
- Use lazy loading for compressed indices
- Reduce dataset size (--limit 50000)

**Build too slow?**
- Reduce document limit in preprocessed data
- Build indices in parallel (not recommended, may crash)

**Evaluation errors?**
- Check index files exist
- Verify queries.txt has valid queries
- Check Python environment activated

## References

- Assignment requirements: See main assignment document
- Optimization guide: `OPTIMIZATION_GUIDE.md`
- Quick reference: `QUICK_REFERENCE.md`
