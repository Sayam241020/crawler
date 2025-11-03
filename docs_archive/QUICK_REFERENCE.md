# 🚀 QUICK REFERENCE GUIDE

**Last Updated:** October 25, 2025  
**Status:** ✅ Production Ready

---

## 📂 File Navigation

### Want to...

| Task | File | Command |
|------|------|---------|
| **Build an index** | `build.py` | `python build.py -x 1 -y 1 -z 1 -q T -optim 0` |
| **Evaluate performance** | `evaluate.py` | `python evaluate.py` |
| **Query an index** | `query.py` | `python query.py -x 3 -y 1 -z 1 -q T -optim 0 --interactive` |
| **Generate plots** | `generate_plots.py` | `python generate_plots.py` |
| **Verify plots** | `verify_plots.py` | `python verify_plots.py` |
| **Check structure** | `verify_structure.py` | `python verify_structure.py` |
| **Understand indexing** | `src/self_indexer.py` | Read the code |
| **See grammar** | `src/query_processor.py` | Read the code |
| **View test cases** | `tests/test_assignment_example.py` | Read the code |

---

## 📁 Directory Quick Reference

```
IRE_Assignment1/
├── 📄 *.py files        → Main scripts (7 files)
├── 📁 src/              → Implementation (12 files)
├── 📁 tests/            → Essential tests (3 files)
├── 📁 indices/          → Built indices (18 files)
├── 📁 plots/            → Visualizations (8 files)
├── 📁 results/          → Evaluation results
├── 📁 archive/          → Historical docs
├── 📁 data/             → Source datasets
└── 📄 *.md files        → Documentation (4 files)
```

---

## 🎯 Common Commands

### Build Indices
```powershell
# Boolean index
python build.py -x 1 -y 1 -z 1 -q T -optim 0

# TF index with compression
python build.py -x 2 -y 1 -z 2 -q T -optim 0

# TF-IDF with SQLite
python build.py -x 3 -y 3 -z 1 -q T -optim 0
```

### Evaluate
```powershell
# Run full evaluation
python evaluate.py

# Evaluate specific index
python evaluate.py -x 3 -y 1 -z 1 -q T -optim 0
```

### Generate Plots
```powershell
# All plots
python generate_plots.py

# Missing plots (skip pointers, datastore)
python generate_missing_plots.py

# Verify plots
python verify_plots.py
```

### Query
```powershell
# Interactive mode
python query.py -x 3 -y 1 -z 1 -q T -optim 0 --interactive

# Single query
python query.py -x 3 -y 1 -z 1 -q T -optim 0 --query "machine learning"
```

### Tests
```powershell
# Run all tests
pytest tests/ -v

# Run specific test
pytest tests/test_assignment_example.py -v
```

---

## 📊 Index Configurations

| x (Ranking) | y (Datastore) | z (Compression) | File |
|-------------|---------------|-----------------|------|
| 1 (Boolean) | 1 (JSON) | 1 (None) | `SelfIndex_i1d1c1qTo0.json` |
| 1 (Boolean) | 1 (JSON) | 2 (VByte) | `SelfIndex_i1d1c2qTo0.json` |
| 1 (Boolean) | 1 (JSON) | 3 (Zlib) | `SelfIndex_i1d1c3qTo0.json` |
| 1 (Boolean) | 3 (SQLite) | 1 (None) | `SelfIndex_i1d3c1qTo0.db` |
| 1 (Boolean) | 3 (SQLite) | 2 (VByte) | `SelfIndex_i1d3c2qTo0.db` |
| 1 (Boolean) | 3 (SQLite) | 3 (Zlib) | `SelfIndex_i1d3c3qTo0.db` |
| 2 (TF) | 1 (JSON) | 1 (None) | `SelfIndex_i2d1c1qTo0.json` |
| 2 (TF) | 1 (JSON) | 2 (VByte) | `SelfIndex_i2d1c2qTo0.json` |
| 2 (TF) | 1 (JSON) | 3 (Zlib) | `SelfIndex_i2d1c3qTo0.json` |
| 2 (TF) | 3 (SQLite) | 1 (None) | `SelfIndex_i2d3c1qTo0.db` |
| 2 (TF) | 3 (SQLite) | 2 (VByte) | `SelfIndex_i2d3c2qTo0.db` |
| 2 (TF) | 3 (SQLite) | 3 (Zlib) | `SelfIndex_i2d3c3qTo0.db` |
| 3 (TF-IDF) | 1 (JSON) | 1 (None) | `SelfIndex_i3d1c1qTo0.json` |
| 3 (TF-IDF) | 1 (JSON) | 2 (VByte) | `SelfIndex_i3d1c2qTo0.json` |
| 3 (TF-IDF) | 1 (JSON) | 3 (Zlib) | `SelfIndex_i3d1c3qTo0.json` |
| 3 (TF-IDF) | 3 (SQLite) | 1 (None) | `SelfIndex_i3d3c1qTo0.db` |
| 3 (TF-IDF) | 3 (SQLite) | 2 (VByte) | `SelfIndex_i3d3c2qTo0.db` |
| 3 (TF-IDF) | 3 (SQLite) | 3 (Zlib) | `SelfIndex_i3d3c3qTo0.db` |

**Total:** 18 indices (3 × 2 × 3)

---

## 🎨 Generated Plots

| File | Description | Assignment Requirement |
|------|-------------|------------------------|
| `PlotA_datastore_y1_vs_y3.png` | JSON vs SQLite comparison | Plot.A (y=1 vs y=3) |
| `PlotA_skip_pointers_i0_vs_isp.png` | Skip pointers optimization | Plot.A (i=0 vs i=sp) |
| `PlotA_skip_pointers_detailed.png` | Skip pointers details | Plot.A (detailed) |
| `PlotB_compression.png` | Compression impact | Plot.AB (z=1,2,3) |
| `PlotC_index_types.png` | Index types comparison | Plot.C (x=1,2,3) |
| `PlotD_taat_vs_daat.png` | TAAT vs DAAT | Plot.AC (q=T vs q=D) |
| `PlotF_size_speed.png` | Size-speed tradeoff | Bonus |
| `summary_table.png` | Complete metrics | Bonus |

**Total:** 8 plots (6 required + 2 bonus)

---

## 📖 Documentation Files

| File | Purpose |
|------|---------|
| `README.md` | Main documentation (start here!) |
| `PROJECT_STRUCTURE.md` | Detailed file structure |
| `ASSIGNMENT_PLOTS_COMPLETE.md` | Plot completion status |
| `CLEANUP_REPORT.md` | What was removed and why |
| `QUICK_REFERENCE.md` | This file |

---

## 🔑 Key Results

### Skip Pointers Optimization
- **Speedup:** 5.72x average for Boolean AND queries
- **Baseline:** 10.499ms → **Optimized:** 1.834ms
- **Correctness:** 100% verified

### Compression Impact
- **VByte (z=2):** 2-3x size reduction, minimal speed impact
- **Zlib (z=3):** 5-8x size reduction, moderate overhead

### TAAT vs DAAT
- **TAAT:** Better for simple queries
- **DAAT:** Better for complex multi-term queries

### Index Types
- **Boolean (x=1):** Fastest, no ranking
- **TF (x=2):** Moderate speed, basic relevance
- **TF-IDF (x=3):** Best relevance, slightly slower

---

## 🎓 Tips

1. **Always activate environment first:**
   ```powershell
   .\env\Scripts\Activate.ps1
   ```

2. **Check plots before submitting:**
   ```powershell
   python verify_plots.py
   ```

3. **Verify structure is clean:**
   ```powershell
   python verify_structure.py
   ```

4. **Read the documentation:**
   - Start with `README.md`
   - Check `PROJECT_STRUCTURE.md` for organization
   - See `ASSIGNMENT_PLOTS_COMPLETE.md` for plot details

5. **Understand the format:**
   - `SelfIndex_i{x}d{y}c{z}q{q}o{optim}`
   - Defined in `src/index_base.py`

---

## ✅ Assignment Checklist

- [x] 18 indices built (all configurations)
- [x] Full evaluation (256 queries × 18 indices × 2 modes)
- [x] All required plots generated
- [x] Skip pointers implemented and verified
- [x] Clean, organized codebase
- [x] Documentation complete

**Status:** ✅ **READY FOR SUBMISSION**

---

**Quick Ref Version:** 1.0  
**Date:** October 25, 2025  
**Status:** Current & Complete
