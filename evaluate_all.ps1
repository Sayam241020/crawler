# Batch Evaluation Script for Phase 1 Indices
# Evaluates all 7 built indices with both TAAT and DAAT query modes

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "PHASE 1: BATCH EVALUATION OF ALL INDICES" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

$pythonExe = "d:\IRE\IRE_Assignment1\env\Scripts\python.exe"
$queryFile = "queries/test_queries.txt"
$startTime = Get-Date

# Check if query file exists
if (-not (Test-Path $queryFile)) {
    Write-Host "ERROR: Query file not found: $queryFile" -ForegroundColor Red
    Write-Host "Run: python scripts/generate_queries.py" -ForegroundColor Yellow
    exit 1
}

# Count queries
$queryCount = (Get-Content $queryFile | Measure-Object -Line).Lines
Write-Host "Query file: $queryFile" -ForegroundColor Cyan
Write-Host "Total queries: $queryCount" -ForegroundColor Cyan
Write-Host ""

# Function to evaluate an index
function Evaluate-Index {
    param (
        [int]$x,
        [int]$y,
        [int]$z,
        [string]$optim,
        [string]$queryMode,
        [string]$description
    )
    
    Write-Host ""
    Write-Host "-----------------------------------------------------------" -ForegroundColor Yellow
    Write-Host "Evaluating: $description" -ForegroundColor Yellow
    Write-Host "Parameters: x=$x y=$y z=$z optim=$optim q=$queryMode" -ForegroundColor Yellow
    Write-Host "-----------------------------------------------------------" -ForegroundColor Yellow
    
    $evalStart = Get-Date
    
    & $pythonExe evaluate.py -x $x -y $y -z $z -optim $optim -q $queryMode --queries $queryFile
    
    $evalEnd = Get-Date
    $duration = $evalEnd - $evalStart
    
    if ($LASTEXITCODE -eq 0) {
        $seconds = [math]::Round($duration.TotalSeconds, 2)
        Write-Host "SUCCESS - Evaluation completed in $seconds seconds" -ForegroundColor Green
    } else {
        Write-Host "FAILED - Evaluation failed with exit code $LASTEXITCODE" -ForegroundColor Red
        # Continue with other evaluations
    }
}

Write-Host "Evaluating 7 indices x 2 query modes = 14 evaluations total" -ForegroundColor Cyan
Write-Host ""

$evalCount = 0
$totalEvals = 14

# ============================================================
# INDEX 1: Boolean Baseline (TAAT only - no ranking)
# ============================================================
$evalCount++
Write-Host "[$evalCount/$totalEvals]" -ForegroundColor Magenta
Evaluate-Index -x 1 -y 1 -z 1 -optim "0" -queryMode "T" -description "Boolean Index (Baseline) - TAAT"

# ============================================================
# INDEX 2: TF Baseline (TAAT and DAAT)
# ============================================================
$evalCount++
Write-Host "[$evalCount/$totalEvals]" -ForegroundColor Magenta
Evaluate-Index -x 2 -y 1 -z 1 -optim "0" -queryMode "T" -description "TF Index (Baseline) - TAAT"

$evalCount++
Write-Host "[$evalCount/$totalEvals]" -ForegroundColor Magenta
Evaluate-Index -x 2 -y 1 -z 1 -optim "0" -queryMode "D" -description "TF Index (Baseline) - DAAT"

# ============================================================
# INDEX 3: TF-IDF Baseline (TAAT and DAAT)
# ============================================================
$evalCount++
Write-Host "[$evalCount/$totalEvals]" -ForegroundColor Magenta
Evaluate-Index -x 3 -y 1 -z 1 -optim "0" -queryMode "T" -description "TF-IDF Index (Baseline) - TAAT"

$evalCount++
Write-Host "[$evalCount/$totalEvals]" -ForegroundColor Magenta
Evaluate-Index -x 3 -y 1 -z 1 -optim "0" -queryMode "D" -description "TF-IDF Index (Baseline) - DAAT"

# ============================================================
# INDEX 4: TF-IDF with SQLite (TAAT and DAAT)
# ============================================================
$evalCount++
Write-Host "[$evalCount/$totalEvals]" -ForegroundColor Magenta
Evaluate-Index -x 3 -y 2 -z 1 -optim "0" -queryMode "T" -description "TF-IDF Index with SQLite - TAAT"

$evalCount++
Write-Host "[$evalCount/$totalEvals]" -ForegroundColor Magenta
Evaluate-Index -x 3 -y 2 -z 1 -optim "0" -queryMode "D" -description "TF-IDF Index with SQLite - DAAT"

# ============================================================
# INDEX 5: TF-IDF with Elias Compression (TAAT and DAAT)
# ============================================================
$evalCount++
Write-Host "[$evalCount/$totalEvals]" -ForegroundColor Magenta
Evaluate-Index -x 3 -y 1 -z 2 -optim "0" -queryMode "T" -description "TF-IDF Index with Elias Compression - TAAT"

$evalCount++
Write-Host "[$evalCount/$totalEvals]" -ForegroundColor Magenta
Evaluate-Index -x 3 -y 1 -z 2 -optim "0" -queryMode "D" -description "TF-IDF Index with Elias Compression - DAAT"

# ============================================================
# INDEX 6: TF-IDF with Zlib Compression (TAAT and DAAT)
# ============================================================
$evalCount++
Write-Host "[$evalCount/$totalEvals]" -ForegroundColor Magenta
Evaluate-Index -x 3 -y 1 -z 3 -optim "0" -queryMode "T" -description "TF-IDF Index with Zlib Compression - TAAT"

$evalCount++
Write-Host "[$evalCount/$totalEvals]" -ForegroundColor Magenta
Evaluate-Index -x 3 -y 1 -z 3 -optim "0" -queryMode "D" -description "TF-IDF Index with Zlib Compression - DAAT"

# ============================================================
# INDEX 7: Boolean with Skip Pointers (TAAT only)
# ============================================================
$evalCount++
Write-Host "[$evalCount/$totalEvals]" -ForegroundColor Magenta
Evaluate-Index -x 1 -y 1 -z 1 -optim "sp" -queryMode "T" -description "Boolean Index with Skip Pointers - TAAT"

# ============================================================
# SUMMARY
# ============================================================
$endTime = Get-Date
$totalDuration = $endTime - $startTime
$totalMinutes = [math]::Round($totalDuration.TotalMinutes, 2)

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "EVALUATION COMPLETE!" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Total time: $totalMinutes minutes" -ForegroundColor Green
Write-Host "Total evaluations: $evalCount" -ForegroundColor Green
Write-Host ""

# List result files
Write-Host "Result files:" -ForegroundColor Cyan
Get-ChildItem results\eval_*.json | ForEach-Object {
    $size = [math]::Round($_.Length / 1KB, 2)
    Write-Host "  $($_.Name) ($size KB)" -ForegroundColor White
}

Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Generate plots: python scripts/plot_metrics.py" -ForegroundColor White
Write-Host "  2. Compare results: python scripts/compare_indices.py" -ForegroundColor White
Write-Host "  3. Setup Elasticsearch: python scripts/es_indexer.py" -ForegroundColor White
Write-Host ""
