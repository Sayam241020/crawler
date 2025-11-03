# File Cleanup Plan - Audit & Organization

## 📋 Current Inventory

### ✅ KEEP - Essential Files

#### **Evaluation Results (17 files):**
**SelfIndex Evaluations (12 files - REQUIRED for Phase 1):**
- eval_SelfIndex_i1d1c1o0_qTAAT.json ✅
- eval_SelfIndex_i1d1c1osp_qTAAT.json ✅
- eval_SelfIndex_i2d1c1o0_qDAAT.json ✅
- eval_SelfIndex_i2d1c1o0_qTAAT.json ✅
- eval_SelfIndex_i3d1c1o0_qDAAT.json ✅
- eval_SelfIndex_i3d1c1o0_qTAAT.json ✅
- eval_SelfIndex_i3d1c2o0_qDAAT.json ✅
- eval_SelfIndex_i3d1c2o0_qTAAT.json ✅
- eval_SelfIndex_i3d1c3o0_qDAAT.json ✅
- eval_SelfIndex_i3d1c3o0_qTAAT.json ✅
- eval_SelfIndex_i3d2c1o0_qDAAT.json ✅
- eval_SelfIndex_i3d2c1o0_qTAAT.json ✅

**Elasticsearch Evaluations (4 files - BONUS for Phase 2):**
- eval_esindex-v1.0_before_optimization.json ✅ (historical reference)
- eval_esindex-v1.0_COLD.json ✅ (primary ES comparison)
- eval_esindex-v1.0_MIXED.json ✅ (realistic scenario)
- eval_esindex-v1.0_WARM.json ✅ (best-case)

**Duplicate to Consider:**
- eval_esindex-v1.0.json ❓ (seems to be duplicate of old eval)

---

#### **Index Files (6 files - REQUIRED):**
- SelfIndex_i1d1c1o0.json ✅ (575 MB - Boolean)
- SelfIndex_i1d1c1osp.json ✅ (1322 MB - Boolean with skip pointers)
- SelfIndex_i2d1c1o0.json ✅ (634 MB - TF)
- SelfIndex_i3d1c1o0.json ✅ (651 MB - TF-IDF baseline)
- SelfIndex_i3d1c2o0.json ✅ (164 MB - TF-IDF + Elias)
- SelfIndex_i3d1c3o0.json ✅ (263 MB - TF-IDF + Zlib)

**Note:** ES index stored separately at D:\ElasticStack\ (not in project)

---

#### **Core Scripts (KEEP):**
- build.py ✅ (builds SelfIndex indices)
- evaluate.py ✅ (evaluates SelfIndex)
- query.py ✅ (query execution)
- build_es.py ✅ (builds ES index)

---

#### **Documentation (KEEP):**
- README.md ✅ (main readme)
- ELASTICSEARCH_EVALUATION_GUIDE.md ✅ (ES evaluation guide - NEW)
- PHASE1_REFERENCE.md ✅ (Phase 1 reference)
- OPTIMIZATION_GUIDE.md ✅ (optimization guide)
- QUICK_REFERENCE.md ✅ (quick reference)

---

### ❓ REVIEW - Potential Cleanup Candidates

#### **ES Scripts (Diagnostic/Testing - Can Archive):**
- evaluate_es.py ❓ (old version, superseded by evaluate_es_all_scenarios.py)
- evaluate_es_all_scenarios.py ✅ (KEEP - comprehensive ES evaluation)
- compare_es_evaluations.py ✅ (KEEP - useful comparison tool)
- test_es_connection.py ❓ (diagnostic only - can archive)
- verify_es_data.py ❓ (diagnostic only - can archive)

#### **ES Documentation (Diagnostic - Can Archive):**
- ES_STORAGE_EXPLAINED.md ❓ (useful but covered in ELASTICSEARCH_EVALUATION_GUIDE.md)
- ES_PERFORMANCE_ANALYSIS.md ❓ (useful but covered in ELASTICSEARCH_EVALUATION_GUIDE.md)

#### **Duplicate Evaluation File:**
- eval_esindex-v1.0.json ❓ (check if duplicate of MIXED or old version)

---

### ❌ DELETE - Truly Unnecessary Files

*To be determined after review*

---

## 🎯 Cleanup Actions

### Action 1: Identify Duplicate ES Evaluation
Check if `eval_esindex-v1.0.json` is duplicate:
```powershell
Compare-Object (Get-Content results/eval_esindex-v1.0.json) (Get-Content results/eval_esindex-v1.0_MIXED.json)
```

**If duplicate:** Delete eval_esindex-v1.0.json
**If unique:** Rename to indicate what it is

---

### Action 2: Archive Diagnostic Scripts
Create `scripts_archive/` folder and move:
- test_es_connection.py → scripts_archive/
- verify_es_data.py → scripts_archive/
- evaluate_es.py (old version) → scripts_archive/

**Rationale:** Keep them for reference but not cluttering main directory

---

### Action 3: Consolidate ES Documentation
Options:
A) Keep all 3 separate (current state)
B) Archive ES_STORAGE_EXPLAINED.md and ES_PERFORMANCE_ANALYSIS.md (info in main guide)

**Recommendation:** Option B - archive to docs_archive/

---

### Action 4: Organize Directory Structure
**Proposed structure:**
```
IRE_Assignment1/
├── results/                    # Keep all 17 eval files
├── indices/                    # Keep all 6 index files
├── src/                        # Core source code
├── scripts/                    # Active scripts
│   ├── build.py               ✅
│   ├── evaluate.py            ✅
│   ├── query.py               ✅
│   ├── build_es.py            ✅
│   ├── evaluate_es_all_scenarios.py ✅
│   └── compare_es_evaluations.py ✅
├── scripts_archive/            # Diagnostic scripts
│   ├── test_es_connection.py
│   ├── verify_es_data.py
│   └── evaluate_es.py (old)
├── docs/                       # Main documentation
│   ├── README.md              ✅
│   ├── ELASTICSEARCH_EVALUATION_GUIDE.md ✅
│   ├── PHASE1_REFERENCE.md    ✅
│   ├── OPTIMIZATION_GUIDE.md  ✅
│   └── QUICK_REFERENCE.md     ✅
└── docs_archive/               # Historical docs
    ├── ES_STORAGE_EXPLAINED.md
    └── ES_PERFORMANCE_ANALYSIS.md
```

---

## 📊 Summary

### Files to Keep (Core Assignment):
- ✅ 17 evaluation result files
- ✅ 6 index files (7 total including skip pointers)
- ✅ 4 core scripts (build, evaluate, query, build_es)
- ✅ 2 ES analysis scripts (all_scenarios, compare)
- ✅ 5 main documentation files

### Files to Archive (Not Delete):
- 📦 3 diagnostic ES scripts
- 📦 2 supplementary ES docs (merged into main guide)
- 📦 1 potential duplicate eval file

### Files to Delete:
- ❌ None (archive instead for transparency)

### Total Cleanup:
- Before: ~30 files in root
- After: ~20 files in root (organized)
- Archived: ~6 files (preserved but not cluttering)

---

## ✅ Execution Plan

1. Check for duplicate eval file
2. Create archive folders
3. Move diagnostic files to archive
4. Move old docs to archive
5. Verify nothing broken
6. Update README with new structure

**Safe approach:** Archive, don't delete!
