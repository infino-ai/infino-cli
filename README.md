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

## Commands

| Command | What it does |
|---|---|
| `create-table` | Create a table and load initial rows (`--from-parquet`, or `--schema` + `--file`; `--fts` / `--vector` indexes) |
| `ingest` | Append rows from Parquet or NDJSON (file or stdin) |
| `bm25-search` | Ranked keyword (BM25) search |
| `vector-search` | Vector similarity (kNN) search — bring your own query vector |
| `token-match` / `exact-match` | Unranked token / exact-value match |
| `query` | Run SQL (incl. the `bm25_search()` / `vector_search()` table functions) |
| `tables` / `describe` | List tables / show a table's schema |
| `update` / `delete` | Change rows matching a `--where` SQL predicate |
| `optimize` | Compact a table |
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
