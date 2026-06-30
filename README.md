# infino CLI

`infino` is the command-line interface to the
[infino](https://github.com/infino-ai/infino) retrieval engine: SQL, full-text
(BM25), and vector search over a single copy of your data on object storage.

## Build

```sh
cargo build --release
./target/release/infino --help
```

Depends on the published [`infino`](https://crates.io/crates/infino) crate.

## Usage

Every command targets a storage location with `--uri` (or the `INFINO_URI`
environment variable): `memory://`, `file://<path>`, `s3://<bucket>/<prefix>`,
or `az://<container>/<prefix>`.

```sh
infino tables --uri file://./data
infino describe docs --uri file://./data
infino query "SELECT * FROM docs LIMIT 10" --uri file://./data
```
