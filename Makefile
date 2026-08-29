.PHONY: venv install run-all run-tenant test lint clean

PYTHON := python3.11
VENV := .venv
ACTIVATE := . $(VENV)/bin/activate

venv:
	$(PYTHON) -m venv $(VENV)
	$(ACTIVATE) && pip install --upgrade pip

install: venv
	$(ACTIVATE) && pip install -e ".[dev]"

# Ejecuta el pipeline completo (todas las capas) para todos los tenants, todo el rango de fechas.
run-all:
	$(ACTIVATE) && python -m saas_pipeline.cli \
		--env dev --tenant all \
		--start-date 2025-01-01 --end-date 2025-06-30

# Ejemplo: make run-tenant TENANT=sv START=2025-03-01 END=2025-03-31
run-tenant:
	$(ACTIVATE) && python -m saas_pipeline.cli \
		--env dev --tenant $(TENANT) \
		--start-date $(START) --end-date $(END)

test:
	$(ACTIVATE) && pytest tests/ -v

lint:
	$(ACTIVATE) && ruff check src/ tests/

clean:
	rm -rf data/bronze data/silver data/gold data/bronze_quarantine data/silver_quarantine data/shared
	rm -rf $(VENV) .pytest_cache **/__pycache__
