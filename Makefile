VENV_BIN = python3 -m venv
VENV_DIR ?= .venv
VENV_ACTIVATE = $(VENV_DIR)/bin/activate
VENV_RUN = . $(VENV_ACTIVATE)


venv: $(VENV_ACTIVATE)

$(VENV_ACTIVATE): setup.cfg setup.py pyproject.toml
	test -d $(VENV_DIR) || $(VENV_BIN) $(VENV_DIR)
	$(VENV_RUN); pip install --upgrade setuptools wheel
	$(VENV_RUN); pip install -e ".[dev]"
	touch $(VENV_DIR)/bin/activate

clean:
	rm -rf build/
	rm -rf .eggs/
	rm -rf *.egg-info/
	rm -rf dist/

clean-venv:
	rm -rf .venv

format:
	$(VENV_RUN); python -m isort .; python -m black .

test: venv
	$(VENV_RUN); python -m pytest tests/

install: venv

dist: venv
	$(VENV_RUN); python setup.py sdist bdist_wheel

upload: venv
	$(VENV_RUN); pip install --upgrade twine; twine upload dist/*

.PHONY: clean venv-clean format
