# infino CLI

`infino` is the command-line interface to the
[infino](https://github.com/infino-ai/infino) retrieval engine — **SQL,
full-text (BM25), and vector search over a single copy of your data on object
storage**, from your terminal or a coding agent. No server, no daemon.

## Install

```sh
# Homebrew (macOS / Linux)
brew install infino-ai/tap/infino-cli

# npm
npm install -g @infino-ai/infino-cli     # or: npx @infino-ai/infino-cli

# shell installer
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/infino-ai/infino-cli/releases/latest/download/infino-cli-installer.sh | sh

# cargo
cargo install infino-cli
```

All install the `infino` binary. (Or build from source: `cargo build --release`.)

## Quickstart

Every command targets a storage location with `--uri` (or the `INFINO_URI`
environment variable): `memory://`, `file://<path>`, `s3://<bucket>/<prefix>`,
or `az://<container>/<prefix>`.

```sh
# Create a table and load its first rows (schema from YAML, body full-text indexed)
infino create-table docs --uri file://./data --schema schema.yaml --fts body --file seed.ndjson

# Add more rows
infino ingest docs --uri file://./data --file more.ndjson --format ndjson

# Search
infino bm25-search docs body "object storage" -k 10 --uri file://./data
infino query "SELECT id, body FROM docs LIMIT 10" --uri file://./data --output json
```

## Cloud storage and credentials

Credentials, region, and endpoint are passed with `--storage-option KEY=VALUE`
(repeatable), keyed by [`object_store`](https://docs.rs/object_store)'s config
strings. This is the CLI's equivalent of the `storage_options` map on the Node
(`connect(uri, { storageOptions })`) and Python (`connect(uri,
storage_options=...)`) bindings. As with the engine and the bindings, nothing
is read from `AWS_*` / `AZURE_*` environment variables; omit the options to use
ambient cloud identity (IAM instance role / managed identity / workload-identity
ADC).

Common S3 keys (the same keys work for S3-compatible services like MinIO, Ceph,
and RustFS):

| Key | Purpose |
|---|---|
| `aws_access_key_id` / `aws_secret_access_key` | Static credentials |
| `aws_session_token` | Temporary-credential session token |
| `aws_region` | Signing region |
| `aws_endpoint` | Custom endpoint for a non-AWS S3 service |
| `aws_allow_http` | Set `true` to permit a plain-HTTP endpoint |

A custom `aws_endpoint` automatically switches to path-style addressing, which
is what most S3-compatible servers expect.

```sh
infino tables --uri s3://my-bucket \
  --storage-option aws_access_key_id=... \
  --storage-option aws_secret_access_key=... \
  --storage-option aws_region=us-east-1 \
  --storage-option aws_endpoint=https://minio.internal:9000 \
  --storage-option aws_allow_http=true
```

Azure uses `azure_storage_account_name` / `azure_storage_account_key`; GCS uses
`google_service_account` (key-file path) or `google_service_account_key`
(inline JSON). `file://` and `memory://` need no credentials.

To see the underlying storage requests when a connection misbehaves, set
`RUST_LOG`:

```sh
RUST_LOG=debug,object_store=trace infino tables --uri s3://my-bucket
```

## Commands

| Command | What it does |
|---|---|
| `create-table` | Create a table and load initial rows (`--from-parquet`, or `--schema` + `--file`; `--fts` / `--vector` indexes) |
| `ingest` | Append rows from Parquet or NDJSON (file or stdin) |
| `bm25-search` | Ranked keyword (BM25) search |
| `vector-search` | Vector similarity (kNN) search — bring your own query vector |
| `hybrid-search` | Hybrid BM25 + vector search, fused with reciprocal-rank fusion |
| `token-match` / `exact-match` | Unranked token / exact-value match |
| `count` | Count rows matching a keyword query, without fetching them |
| `query` | Run SQL (incl. the `bm25_search()` / `vector_search()` table functions) |
| `tables` / `describe` | List tables / show a table's schema |
| `update` / `delete` | Change rows matching a `--where` SQL predicate |
| `optimize` | Compact a table |
| `gc` | Reclaim orphaned storage objects (maintenance; requires durable storage) |
| `skills install` | Install the bundled agent skills for Claude Code / Cursor |

Run `infino <command> --help` for full flags. Output format is `--output
table` (default), `json`, or `csv`.

## Vectors

The CLI does **not** embed text — embed your query with your own model, then pass
the vector as a JSON array:

```sh
infino vector-search docs embedding --vector-file query.json -k 10 --uri file://./data
```

## Agent skills

`infino skills install` writes skill files into `~/.claude/skills` so agents
(Claude Code, Cursor) can drive the CLI in natural language:

```sh
infino skills install
infino skills status
```

## License

Apache-2.0. Part of the [infino](https://github.com/infino-ai/infino) project.
