lint:
	@ruff check .
.PHONY: lint

format: lint
	@ruff format .
.PHONY: format
