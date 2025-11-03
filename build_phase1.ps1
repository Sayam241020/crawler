# Phase 1: Core Index Building Script
# Builds 7 essential indices for all required assignment plots
# Dataset: 100K documents (50K Wiki + 50K News)
# Estimated time: ~1 hour total

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "PHASE 1: BUILDING CORE INDICES FOR ASSIGNMENT" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

$pythonExe = "d:\IRE\IRE_Assignment1\env\Scripts\python.exe"
$startTime = Get-Date

# Function to build an index with timing
function Build-Index {
    param (
        [int]$x,
        [int]$y,
        [int]$z,
        [string]$optim,
        [string]$description
    )
    
    Write-Host ""
    Write-Host "-----------------------------------------------------------" -ForegroundColor Yellow
    Write-Host "Building: $description" -ForegroundColor Yellow
    Write-Host "Parameters: x=$x y=$y z=$z optim=$optim" -ForegroundColor Yellow
    Write-Host "-----------------------------------------------------------" -ForegroundColor Yellow
    
    $buildStart = Get-Date
    
    & $pythonExe build.py -x $x -y $y -z $z -optim $optim
    
    $buildEnd = Get-Date
    $duration = $buildEnd - $buildStart
    
    if ($LASTEXITCODE -eq 0) {
        $minutes = [math]::Round($duration.TotalMinutes, 2)
        Write-Host "SUCCESS - Build completed in $minutes minutes" -ForegroundColor Green
    } else {
        Write-Host "FAILED - Build failed with exit code $LASTEXITCODE" -ForegroundColor Red
        exit $LASTEXITCODE
    }
}

Write-Host "Total indices to build: 7" -ForegroundColor Cyan
Write-Host "Dataset size: 100,000 documents (50K Wiki + 50K News)" -ForegroundColor Cyan
Write-Host ""

# ============================================================
# INDEX 1: Boolean Baseline (for Plot.C - index type comparison)
# ============================================================
Build-Index -x 1 -y 1 -z 1 -optim "0" -description "Boolean Index (Baseline)"

# ============================================================
# INDEX 2: TF Baseline (for Plot.C - index type comparison)
# ============================================================
Build-Index -x 2 -y 1 -z 1 -optim "0" -description "TF Index (Baseline)"

# ============================================================
# INDEX 3: TF-IDF Baseline (for Plot.C - index type comparison)
# ============================================================
Build-Index -x 3 -y 1 -z 1 -optim "0" -description "TF-IDF Index (Baseline)"

# ============================================================
# INDEX 4: TF-IDF with SQLite (for Plot.A - datastore comparison)
# ============================================================
Build-Index -x 3 -y 2 -z 1 -optim "0" -description "TF-IDF Index with SQLite (Datastore Comparison)"

# ============================================================
# INDEX 5: TF-IDF with Elias Compression (for Plot.AB - compression comparison)
# ============================================================
Build-Index -x 3 -y 1 -z 2 -optim "0" -description "TF-IDF Index with Elias Compression"

# ============================================================
# INDEX 6: TF-IDF with Zlib Compression (for Plot.AB - compression comparison)
# ============================================================
Build-Index -x 3 -y 1 -z 3 -optim "0" -description "TF-IDF Index with Zlib Compression"

# ============================================================
# INDEX 7: Boolean with Skip Pointers (for Plot.A - optimization comparison)
# ============================================================
Build-Index -x 1 -y 1 -z 1 -optim "sp" -description "Boolean Index with Skip Pointers (Optimization)"

# ============================================================
# SUMMARY
# ============================================================
$endTime = Get-Date
$totalDuration = $endTime - $startTime
$totalMinutes = [math]::Round($totalDuration.TotalMinutes, 2)

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "PHASE 1 BUILD COMPLETE!" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Total time: $totalMinutes minutes" -ForegroundColor Green
Write-Host ""
Write-Host "Built indices:" -ForegroundColor Cyan
Write-Host "  1. SelfIndex_i1d1c1o0   - Boolean baseline" -ForegroundColor White
Write-Host "  2. SelfIndex_i2d1c1o0   - TF baseline" -ForegroundColor White
Write-Host "  3. SelfIndex_i3d1c1o0   - TF-IDF baseline" -ForegroundColor White
Write-Host "  4. SelfIndex_i3d2c1o0   - TF-IDF with SQLite" -ForegroundColor White
Write-Host "  5. SelfIndex_i3d1c2o0   - TF-IDF with Elias compression" -ForegroundColor White
Write-Host "  6. SelfIndex_i3d1c3o0   - TF-IDF with Zlib compression" -ForegroundColor White
Write-Host "  7. SelfIndex_i1d1c1osp  - Boolean with skip pointers" -ForegroundColor White
Write-Host ""
Write-Host "What you can do now:" -ForegroundColor Yellow
Write-Host "  1. Run evaluations: .\evaluate_phase1.ps1" -ForegroundColor White
Write-Host "  2. Generate plots: python scripts/generate_plots.py" -ForegroundColor White
Write-Host "  3. Compare indices: python scripts/compare_indices.py" -ForegroundColor White
Write-Host ""
Write-Host "Assignment plots covered:" -ForegroundColor Yellow
Write-Host "  [OK] Plot.C  - Memory footprint across index types (x=1,2,3)" -ForegroundColor Green
Write-Host "  [OK] Plot.A  - Datastore comparison (y=1 vs y=2)" -ForegroundColor Green
Write-Host "  [OK] Plot.AB - Compression comparison (z=1,2,3)" -ForegroundColor Green
Write-Host "  [OK] Plot.A  - Optimization comparison (i=0 vs i=sp)" -ForegroundColor Green
Write-Host "  [OK] Plot.AC - Query mode comparison (runtime: -q T vs -q D)" -ForegroundColor Green
Write-Host ""
