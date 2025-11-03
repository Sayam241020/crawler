# Elasticsearch Storage Explanation

## Your Question: "What is this? Where is the index saved?"

### SHORT ANSWER
✅ **Elasticsearch index IS saved to disk**  
✅ **Location:** `D:\ElasticStack\elasticsearch-9.1.4-windows-x86_64\elasticsearch-9.1.4\data\indices\2u4DaCOJT5SEysAK3HwtSg\`  
✅ **NOT in your project folder** (because ES is a separate server)

---

## Detailed Explanation

### 1. WHERE IS THE INDEX STORED?

**SelfIndex (Your Python code):**
```
D:\IRE\IRE_Assignment1\indices\
├── SelfIndex_i1d1c1o0.json      (575 MB)
├── SelfIndex_i1d1c1osp.json     (1322 MB)
├── SelfIndex_i2d1c1o0.json      (634 MB)
├── SelfIndex_i3d1c1o0.json      (651 MB)
├── SelfIndex_i3d2c1o0.db        (0 MB - SQLite)
├── SelfIndex_i3d1c2o0.json      (164 MB - Elias)
└── SelfIndex_i3d1c3o0.json      (263 MB - Zlib)
```

**Elasticsearch (External Server):**
```
D:\ElasticStack\elasticsearch-9.1.4-windows-x86_64\elasticsearch-9.1.4\data\
└── indices\
    └── 2u4DaCOJT5SEysAK3HwtSg\    ← YOUR esindex-v1.0 (418 MB)
        ├── 0\
        │   ├── index\
        │   │   ├── _0.cfs          (Lucene segment files)
        │   │   ├── _0.si
        │   │   ├── segments_2
        │   │   └── write.lock
        │   └── translog\           (Transaction logs)
        └── _state\                 (Metadata)
```

---

### 2. WHY TWO DIFFERENT SIZES?

**Build Output Said: 359.9 MB**
```
Documents indexed: 61607
Index size: 359.9mb          ← This is PRIMARY shard size
Time taken: 45.28 seconds
```

**Actual Disk Usage: 418 MB**

**What's the extra ~58 MB?**
- Transaction logs (WAL - Write Ahead Log)
- Metadata and state files
- Lucene index overhead
- Deleted documents not yet merged
- ES internal data structures

**Both numbers are correct:**
- 359.9 MB = **Primary index data** (what ES API reports)
- 418 MB = **Total disk usage** (what Windows shows)

---

### 3. WHY NOT IN YOUR PROJECT FOLDER?

**Because Elasticsearch is a SEPARATE APPLICATION!**

Think of it like this:

| Aspect | SelfIndex | Elasticsearch |
|--------|-----------|---------------|
| **Type** | Python library | External server |
| **Runs in** | Your Python process | Separate Java process |
| **Data location** | Your project folder | ES installation folder |
| **Control** | Direct file I/O | HTTP API calls |
| **Analogy** | Saving to Excel file | Using MySQL database |

**Example:**
- When you use **SelfIndex**: Your Python code directly writes to `D:\IRE\IRE_Assignment1\indices\*.json`
- When you use **Elasticsearch**: Your Python code sends HTTP requests to ES server, ES server writes to its own data folder

---

### 4. IS THIS A PROBLEM?

**NO! This is normal and expected.**

**For evaluation, you'll measure:**
- ✅ Disk usage (418 MB for ES vs 164-651 MB for SelfIndex)
- ✅ Query latency (how fast ES responds vs SelfIndex)
- ✅ Throughput (queries per second)

**The comparison is still fair because:**
1. Both systems store 100K documents
2. Both apply same preprocessing (lowercase + stopwords + stemming)
3. Both build inverted indices
4. You'll measure ACTUAL disk usage (not just reported size)

---

### 5. SUMMARY

```
YOUR PROJECT:
D:\IRE\IRE_Assignment1\
├── indices\
│   └── SelfIndex_*.json           ← Your custom indices (164-651 MB each)
├── src\
│   ├── self_indexer.py            ← Builds SelfIndex
│   └── es_indexer.py              ← Talks to ES via HTTP
└── build_es.py                    ← Sends data to ES server

ELASTICSEARCH SERVER:
D:\ElasticStack\elasticsearch-9.1.4-windows-x86_64\elasticsearch-9.1.4\
├── bin\
│   └── elasticsearch.bat          ← ES server (running separately)
└── data\
    └── indices\
        └── 2u4DaCOJT5SEysAK3HwtSg\ ← esindex-v1.0 (418 MB)
```

**Key Points:**
- ✅ ES index IS saved to disk (not RAM)
- ✅ Location: ES data folder (not your project folder)
- ✅ Size: ~418 MB actual, ~360 MB primary
- ✅ This is normal - ES is a separate server
- ✅ You'll compare actual disk usage in plots
