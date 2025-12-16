VENV_BIN = python3 -m venv
VENV_DIR ?= .venv
VENV_ACTIVATE = $(VENV_DIR)/bin/activate
VENV_RUN = . $(VENV_ACTIVATE)


venv: $(VENV_ACTIVATE)

$(VENV_ACTIVATE): pyproject.toml
	test -d $(VENV_DIR) || $(VENV_BIN) $(VENV_DIR)
	$(VENV_RUN); pip install -e ".[dev]"
	touch $(VENV_DIR)/bin/activate

clean:
	rm -rf build/
	rm -rf .eggs/
	rm -rf *.egg-info/
	rm -rf .venv

clean-dist: clean
	rm -rf dist/

lint: venv
	$(VENV_RUN); python -m ruff check .

format: venv
	$(VENV_RUN); python -m ruff format . && python -m ruff check . --fix

test: venv
	$(VENV_RUN); python -m pytest

test-coverage: venv
	$(VENV_RUN); coverage run --source=verdin -m pytest tests && coverage lcov -o .coverage.lcov

dist: venv
	$(VENV_RUN); python -m build

install: venv
	$(VENV_RUN); pip install -e .

upload: venv
	$(VENV_RUN); pip install --upgrade twine; twine upload dist/*

.PHONY: clean clean-dist format test test-coverage upload
