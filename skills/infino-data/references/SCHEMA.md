# infino schema & indexes — reference

## YAML schema (`--schema`)

A list of column definitions. Each has `name`, `type`, and optional `nullable`
(default false):

```yaml
- {name: id, type: int64}
- {name: body, type: large_utf8}
- {name: lang, type: utf8, nullable: true}
- {name: embedding, type: "fixed_size_list<float32,384>"}
```

Pair `--schema` with `--file <data>` so the table is seeded (and durable) on
creation.

### Supported types

- Integers: `int8` `int16` `int32` `int64` (`int`); `uint8` `uint16` `uint32` `uint64`
- Floats: `float32` (`float`), `float64` (`double`)
- Text: `utf8` (`string`), **`large_utf8`** — full-text (`--fts`) columns must be
  `large_utf8`
- `bool` (`boolean`), `date32`
- Vectors: `fixed_size_list<float32,N>` where `N` is the dimension

## Inferring the schema from Parquet (`--from-parquet`)

`--from-parquet seed.parquet` takes the schema from the file's Arrow schema **and**
loads that file as the initial rows — no YAML needed.

## Indexes

Indexes are declared with flags (they work with either schema source):

- `--fts <column>` — BM25 full-text index (repeatable). Column must be `large_utf8`.
- `--vector <column:dim:n_cent:metric>` — IVF vector index (repeatable).
  - `dim` — vector dimension (match the column's `fixed_size_list` length).
  - `n_cent` — IVF centroid count; size it to the table's scale.
  - `metric` — `cosine`, `l2sq` (`l2`), or `negdot` (`dot`).

Example: `--vector embedding:384:256:cosine`.
