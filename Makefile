lint:
	@ruff check . --fix
.PHONY: lint

format: lint
	@ruff format .
.PHONY: format

run:
	@python -m demo.app
.PHONY: run

test:
	@python -m pytest -q
.PHONY: test
