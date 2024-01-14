lint:
	@ruff check .
.PHONY: lint

format: lint
	@ruff format .
.PHONY: format

run:
	@python demo/app.py
.PHONY: run
