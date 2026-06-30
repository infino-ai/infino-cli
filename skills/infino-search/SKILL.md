---
name: infino-search
description: Use this skill when the user wants to search an infino table — keyword (BM25), vector similarity, token match, exact match, or SQL. Covers the infino CLI's search commands and their flags.
version: 0.1.0
---

# infino search

All commands take `--uri` (or `INFINO_URI`) and `--output table|json|csv`. The
four search commands mirror the infino SDK methods one-to-one.

## Keyword (BM25), ranked

```
infino bm25-search <table> <column> "<query>" -k 10 [--mode or|and] [--fields a,b]
```

## Vector similarity (kNN)

Bring your own query vector (the CLI does not embed text). `--vector-file` is a
JSON array of numbers, or `-` for stdin.

```
infino vector-search <table> <column> --vector-file q.json -k 10 \
    [--nprobe N] [--rerank-mult M] [--fields a,b] \
    [--filter-column <col> --filter-query "<text>" --filter-mode or|and]
```

## Unranked matches

```
infino token-match <table> <column> "<query>" [--mode or|and] [--fields a,b]
infino exact-match <table> <column> "<value>" [--fields a,b]
```

## SQL (incl. search table functions)

```
infino query "SELECT title FROM bm25_search('<table>', '<query>', 10)" --output json
```

See [references/SEARCH.md](references/SEARCH.md) for projection, modes, filters,
and when to use each command.
