---
name: infino-data
description: Use this skill when the user wants to create an infino table or change its data from the terminal — create-table, ingest, update, delete, optimize, gc. Covers schema definition, indexes, and row loading.
version: 0.1.0
---

# infino data lifecycle

All commands take `--uri` (or `INFINO_URI`).

## Create a table (and seed it)

A table is durable only after its first commit, so `create-table` loads initial
rows too:

```
# Schema + data from one Parquet file:
infino create-table docs <uri> --from-parquet seed.parquet --fts body

# Or a YAML schema plus a data file:
infino create-table docs --uri <uri> --schema schema.yaml --file seed.ndjson \
    --fts body --vector embedding:384:256:cosine
```

- `--fts <col>` — full-text (BM25) index (the column must be `large_utf8`).
- `--vector <col:dim:n_cent:metric>` — vector index; metric is `cosine`,
  `l2sq`, or `negdot`.

## Load more rows

```
infino ingest docs --uri <uri> --file data.parquet                 # parquet
cat rows.ndjson | infino ingest docs --uri <uri> --format ndjson   # ndjson via stdin
```

## Change rows

```
infino update docs --where "id = 42" --set-file new.ndjson --uri <uri>
infino delete docs --where "ts < '2026-01-01'" --uri <uri>
```

`--where` is a SQL predicate resolved against the table schema. `update` replaces
matched rows with the values in `--set-file` (Parquet or NDJSON).

## Compact

```
infino optimize docs --uri <uri> [--max-memory-mb N] [--min-fill-percent P] \
    [--target-superfile-size-mb S]
```

## Reclaim storage (gc)

Delete orphaned objects left by compaction or interrupted writes. Requires
durable storage. `--older-than-secs` is a safety window (default `0`) so a
concurrent reader or writer is never raced.

```
infino gc docs --uri <uri> [--older-than-secs N]
```

See [references/SCHEMA.md](references/SCHEMA.md) for the YAML schema format and
column types.
