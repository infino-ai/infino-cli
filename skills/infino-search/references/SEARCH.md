# infino search — reference

## Choosing a command

- **bm25-search** — ranked keyword relevance over a full-text column. The
  default search for "find documents about X".
- **vector-search** — nearest-neighbour over a vector column; for semantic
  similarity. Needs a precomputed query vector.
- **token-match** — every row whose column matches the query tokens, unranked
  (`score` = 0). Useful for filtering, not ranking.
- **exact-match** — rows whose column equals a value exactly.
- **query** — arbitrary SQL; can call `bm25_search(...)` / `vector_search(...)`
  as table functions and join/filter the results.

## Common flags

- `-k, --k <N>` — number of results (default 10).
- `--fields a,b,c` — columns to return. Omit to get just the id + score.
- `--mode or|and` — for multi-term keyword queries: `or` (any term, default) or
  `and` (all terms).
- `--output table|json|csv`.

## Vector search

- `--vector-file <path>` — a JSON array of numbers, e.g. `[0.1, -0.2, 0.3]`; `-`
  reads it from stdin. The dimension must match the column's vector dimension.
- `--nprobe <N>` — IVF probes; higher = more recall, slower.
- `--rerank-mult <M>` — over-fetch factor before reranking.
- **Pushdown filter** — restrict the kNN candidates to rows matching a keyword
  predicate first: `--filter-column <fts_col> --filter-query "<text>"
  [--filter-mode or|and]`. All three filter flags work together; the filter
  column must be full-text indexed.

## Notes

- The CLI never embeds text. Produce query vectors with your own embedding model
  and pass them via `--vector-file`. Use the **same** model that produced the
  indexed vectors.
- Results are Arrow rows; `--output json` is the most agent-friendly.
