.PHONY: install
install:
	python -m pip install --upgrade pip
	pip install -e . --upgrade --upgrade-strategy eager

.PHONY: install-dev
install-dev:
	python -m pip install --upgrade pip
	pip install -e .[dev] --upgrade --upgrade-strategy eager

.PHONY: format
format:
	ruff format .
	ruff check . --fix
	mypy . --install-types --ignore-missing-imports --non-interactive

.PHONY: test-format
test-format:
	ruff format . --check
	ruff check .
	mypy . --install-types --ignore-missing-imports --non-interactive

.PHONY: test-integration
test-integration:
	pytest tests/ --dbt-target=duckdb

.PHONY: test-e2e
test-e2e:
	pytest tests/ -s --log-cli-level=INFO --dbt-target=trino

.PHONY: docker-image
docker-image:
	docker build -t pulse-analytics:latest .