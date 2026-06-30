---
name: infino
description: Use this skill when the user wants to work with an infino dataset from the terminal — connect to local disk or object storage, then search (BM25 / vector / SQL), inspect, or load data. Start here; see infino-search and infino-data for specifics.
version: 0.1.0
---

# infino CLI

`infino` runs SQL, full-text (BM25), and vector search over a single copy of data
on object storage. It is a thin shell over the infino engine.

## Connecting

Every command targets a storage location with `--uri` (or the `INFINO_URI`
environment variable):

- `memory://` — ephemeral, in-process
- `file://<path>` — local disk
- `s3://<bucket>/<prefix>` — Amazon S3 (or S3-compatible)
- `az://<container>/<prefix>` — Azure Blob

S3/Azure credentials come from the ambient environment (`AWS_*`, `AZURE_*`).

## Inspect

```
infino tables --uri <uri>
infino describe <table> --uri <uri>
```

## Output

`--output table` (default, aligned), `json` (one object per line, jq-friendly),
or `csv`. Applies to every row-returning command.

## Where to go next

- **Searching** a table (BM25, vector, token, exact, SQL) → use the
  `infino-search` skill. See [references/WORKFLOWS.md](references/WORKFLOWS.md)
  for end-to-end flows.
- **Creating tables and loading/changing data** (create-table, ingest, update,
  delete, optimize) → use the `infino-data` skill.

## Installing these skills

```
infino skills install        # writes them into ~/.claude/skills
infino skills status
```
