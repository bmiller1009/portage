.PHONY: dev test lint

dev:
	pip install -e ".[dev]"

test:
	pytest tests/unit -v

lint:
	ruff check .
	pyright
