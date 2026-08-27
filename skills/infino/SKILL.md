---
name: infino
description: Use this skill when the user wants to work with an infino dataset from the terminal — connect to local disk, object storage, or the hosted service, then search (BM25 / vector / SQL), inspect, or load data. Start here; see infino-search and infino-data for specifics.
version: 0.2.0
---

# infino CLI

`infino` runs SQL, full-text (BM25), and vector search over a single copy of data
on object storage. It is a thin shell over the infino engine.

## Connecting

Every command targets a storage location with `--uri` (or the `INFINO_URI`
environment variable):

- `memory://` — ephemeral, in-process
- `file://<path>` — local disk
- `s3://<bucket>/<prefix>` — Amazon S3 (or S3-compatible: MinIO, Ceph, RustFS)
- `az://<container>/<prefix>` — Azure Blob
- `gs://<bucket>/<prefix>` — Google Cloud Storage
- `https://<host>/<database>` — Infino hosted service

Cloud credentials are passed explicitly with `--storage-option KEY=VALUE`
(repeatable), keyed by object_store's config strings — the same
`storage_options` map the language bindings take. Nothing is read from `AWS_*` /
`AZURE_*`; omit them to use ambient cloud identity (IAM role / managed identity).

```
infino table ls --uri s3://bucket \
  --storage-option aws_access_key_id=... \
  --storage-option aws_secret_access_key=... \
  --storage-option aws_region=us-east-1 \
  --storage-option aws_endpoint=https://minio.internal:9000   # S3-compatible
```

## Hosted service

For the hosted service, pass an API key with `--api-key` (or the
`INFINO_API_KEY` env var), and provision the database once with
`database create`:

```
export INFINO_API_KEY=sk-...
infino database create --uri https://<host>/<database>
infino table ls --uri https://<host>/<database>
```

Every other command is identical to a local connection — only the `--uri`
changes. Add `--validate` to any command to fail fast at connect on bad
credentials or an unreachable endpoint instead of on the first query.

## Inspect

```
infino table ls --uri <uri>
infino table describe <table> --uri <uri>
```

## Output

`--output table` (default, aligned), `json` (one object per line, jq-friendly),
or `csv`. Applies to every row-returning command.

## Where to go next

- **Searching** a table (BM25, vector, token, exact, SQL) → use the
  `infino-search` skill. See [references/WORKFLOWS.md](references/WORKFLOWS.md)
  for end-to-end flows.
- **Creating tables and loading/changing data** (table create, row insert, row update,
  delete, optimize) → use the `infino-data` skill.

## Installing these skills

```
infino skills install        # writes them into ~/.claude/skills
infino skills status
```
