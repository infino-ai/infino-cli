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
`az://<container>/<prefix>`, `gs://<bucket>/<prefix>`, or a hosted
`https://<host>/<database>`.

```sh
# Create a table and load its first rows (schema from YAML, body full-text indexed)
infino table create docs --uri file://./data --schema schema.yaml --fts body --file seed.ndjson

# Add more rows
infino row insert docs --uri file://./data --file more.ndjson --format ndjson

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
infino table ls --uri s3://my-bucket \
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
RUST_LOG=debug,object_store=trace infino table ls --uri s3://my-bucket
```

## Hosted service

Connect to the Infino hosted service with an `https://<host>/<database>` URI and
an API key (`--api-key`, or the `INFINO_API_KEY` environment variable). Every
command works exactly as it does locally; only the `--uri` changes. Provision
the database once with `database create`:

```sh
export INFINO_API_KEY=sk-...
infino database create --uri https://<host>/<database>
infino row insert docs --uri https://<host>/<database> --file rows.ndjson --format ndjson
infino bm25-search docs body "object storage" -k 10 --uri https://<host>/<database>
```

Add `--validate` to any command to fail fast at connect on bad credentials or an
unreachable endpoint, rather than on the first query.

## Commands

| Command | What it does |
|---|---|
| `table create` | Create a table and load initial rows (`--from-parquet`, or `--schema` + `--file`; `--fts` / `--vector` indexes) |
| `row insert` | Append rows from Parquet or NDJSON (files, a directory, a glob, or stdin) |
| `bm25-search` | Ranked keyword (BM25) search |
| `vector-search` | Vector similarity (kNN) search — bring your own query vector |
| `hybrid-search` | Hybrid BM25 + vector search, fused with reciprocal-rank fusion |
| `token-match` / `exact-match` | Unranked token / exact-value match |
| `count` | Count rows matching a keyword query, without fetching them |
| `query` | Run SQL (incl. the `bm25_search()` / `vector_search()` table functions) |
| `table ls` / `table describe` | List tables / show a table's schema |
| `table rm` | Remove a table and reclaim its storage (`--keep-storage` to leave the bytes) |
| `database create` | Provision a hosted database (no-op for local / object-storage backends) |
| `row update` / `row delete` | Change or remove rows matching a `--where` SQL predicate |
| `table optimize` | Compact a table |
| `table gc` | Reclaim orphaned storage objects (maintenance; requires durable storage) |
| `skills install` | Install the bundled agent skills for Claude Code / Cursor |

Run `infino <command> --help` for full flags. Output format is `--output
table` (default), `json`, or `csv`.

## Bulk and multi-file ingest

`--from-parquet` and `ingest --file` accept a single file, a **directory** (all
`*.parquet` inside), a quoted **glob**, or several paths — so a dataset split
across many Parquet files loads directly, no pre-combining needed:

```sh
# a whole directory of parquet parts
infino table create wiki --from-parquet ./wikipedia/data/ --fts body --uri s3://bucket
# or a glob
infino row insert wiki --file './wikipedia/data/*.parquet' --format parquet --uri s3://bucket
```

Ingest is **streamed and committed in windows** (default 256 MiB, set with
`--batch-size-mb`), so peak memory is bounded by the window rather than the
input size — a file far larger than RAM loads fine.

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
