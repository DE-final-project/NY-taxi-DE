# Performance tuning (measured)

| # | What | Before | After | Why it helps |
|---|------|--------|-------|--------------|
| 1 | fact_trips: full rebuild → incremental (`unique_key=trip_id`, watermark `_loaded_at`) | 73 s | 5 s | Only new rows (by `_loaded_at`) are merged, not all 10M |
| 2 | loader: file-level incremental via `raw.load_log` (rerun with no new files) | reload everything | 0 new files | Files already in `load_log` are skipped, so a rerun costs seconds |
| 3 | loader: row inserts → `bulk_copy` (TABLOCK, batch 102,400) | not measured | ~13.9k rows/s | One bulk round-trip per 102,400 rows instead of one INSERT per row |
| 4 | raw tables: rowstore → clustered columnstore | not measured | not measured | ~10x compression, fast scans |
| 5 | ingest: 4 → 2 workers (batch-api ran out of memory) | crashed | succeeded (ingest 55 s) | stability over raw speed |