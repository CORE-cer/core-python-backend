.PHONY: lint format typecheck check fix

lint:
	uv run ruff check .

format:
	uv run ruff format --check .

typecheck:
	uv run basedpyright .

check: lint format typecheck

fix:
	uv run ruff check --fix .
	uv run ruff format .
