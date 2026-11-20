# Performance tuning (measured)

| # | What | Before | After | Why it helps |
|---|------|--------|-------|--------------|
| 1 | fact_trips: full rebuild → incremental (`unique_key=trip_id`, watermark `_loaded_at`) | 73 s | 5 s | Only new rows (by `_loaded_at`) are merged, not all 10M |
| 2 | loader: file-level incremental via `raw.load_log` (rerun with no new files) | reload everything | 0 new files | Files already in `load_log` are skipped, so a rerun costs seconds |
| 3 | loader: row inserts → `bulk_copy` (TABLOCK, batch 102,400) | not measured | ~13.9k rows/s | One bulk round-trip per 102,400 rows instead of one INSERT per row |
| 4 | raw tables: rowstore → clustered columnstore | not measured | not measured | ~10x compression, fast scans |
| 5 | ingest: 4 → 2 workers (batch-api ran out of memory) | crashed | succeeded (ingest 55 s) | stability over raw speed |
## Earlier iteration: ingest into a database (measured)
Before the lake existed, `ingest-image` wrote API pages straight into database tables. Tuning steps, each measured:

| Step | Throughput / effect |
|---|---|
| pandas `to_sql` default (one row per INSERT) | ~30 rows/s |
| `method="multi"` (multi-row INSERT) | ~6,900 rows/s |
| 500K-row pages without chunking | worse: ~90–100 s per page (statement compilation pegged one core) |
| `INGEST_CHUNK_SIZE` + 100K pages | ~18 s per 100K-row page |
| Threads → processes (CPU-bound, GIL) | ~100% → ~375% CPU (real multi-core) |
| `batch-api` 1 → 4 uvicorn workers | API stopped being the bottleneck |
