.PHONY: setup
setup:
	uv sync --all-extras
	uv run pre-commit install

.PHONY: pre-commit
pre-commit:
	uv run pre-commit

.PHONY: run-polars
run-polars:
	time uv run run_polars.py

.PHONY: run-duckdb
run-duckdb:
	time uv run run_duckdb.py
