# Dataset Inspection (Initial)

Inspection date: 2026-03-17

## Current state
- `data/raw/` exists but contains no files yet.
- No schema inference can be finalized until raw files are added.

## Planned detection once files are available
- file types
- column names and frequencies
- candidate timestamp fields
- identifier fields (`bus_id`-like)
- route fields
- GPS fields (`lat/lon`-like)
- schema inconsistencies across files
- quality risks (nulls, duplicates, invalid coords)

## Command
```bash
make dataset-inspect
```
