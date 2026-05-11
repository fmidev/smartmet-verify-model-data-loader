.PHONY: test lint fix audit clean run container-build container-run

test:
	docker compose run --rm test

lint:
	docker compose run --rm lint

fix:
	docker compose run --rm fix

audit:
	docker compose run --rm audit

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; \
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null; \
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null; \
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null; \
	rm -rf htmlcov .coverage coverage.xml

run:
	docker compose run --rm app

container-build:
	docker build -t smartmet-verify-model-data-loader .

container-run:
	docker run --env-file .env smartmet-verify-model-data-loader
