# infino CLI — end-to-end workflows

All commands take `--uri` (or `INFINO_URI`). Examples use `file://./data`.

## Build a searchable table and query it

```
# 1. Create + seed (a table is durable only after its first commit).
infino create-table docs --uri file://./data \
    --schema schema.yaml --fts body --file seed.ndjson

# 2. Append more rows later.
infino ingest docs --uri file://./data --file more.ndjson --format ndjson

# 3. Search.
infino bm25-search docs body "object storage" -k 10 --uri file://./data
infino query "SELECT id, body FROM docs LIMIT 5" --uri file://./data
```

`schema.yaml` is a list of columns; full-text columns must be `large_utf8`:

```yaml
- {name: id, type: int64}
- {name: body, type: large_utf8}
```

## Semantic (vector) search

The CLI does **not** embed text — embed the query with your own model, write the
vector as a JSON array, then pass it:

```
echo '[0.12, -0.04, ...]' > q.json
infino vector-search docs embedding --vector-file q.json -k 10 --uri file://./data
# or stream it:  ... --vector-file -   (reads the array from stdin)
```

Create the table with a vector index first:
`--vector embedding:384:256:cosine` (column:dim:n_cent:metric).

## Change data

```
infino update docs --where "id = 42" --set-file new.ndjson --uri file://./data
infino delete docs --where "ts < '2026-01-01'" --uri file://./data
infino optimize docs --uri file://./data
```
