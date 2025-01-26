# Surviving the JSON Jungle

## Getting started

Ensure you have [uv][uv-install-docs] installed.

Once that's done, you can set up the project with:

```bash
make setup
```

Run the Polars script.

```bash
make run-polars
```

Run the DuckDB script.

```bash
make run-duckdb
```

## TODO

- [x] Add DuckDB processing scripts.
- [x] Refine transformations, keeping only what's necessary.
- [ ] Look into query plans?
- [ ] Add some testing for better checks that the transformations work well.

[uv-install-docs]: https://docs.astral.sh/uv/getting-started/installation/
