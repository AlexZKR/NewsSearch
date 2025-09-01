requirements:
	python -m pip install --upgrade pip setuptools wheel && python -m pip install -r requirements.txt

# Linting
lint: mypy ruff bandit

mypy:
	mypy .

ruff:
	ruff check --fix

bandit:
	bandit -c pyproject.toml -r . --quiet

# Formatting
format:
	ruff format .

start:
	docker compose up --build

# Testing
check: lint tests

tests:
	PYTHONPATH=$(PWD) \
	&& . venv/bin/activate \
	&& pytest --cov --cov-fail-under=100 --cov-report html

coverage:
	PYTHONPATH=$(PWD) \
	&& . venv/bin/activate \
	&& python -m webbrowser -t htmlcov/index.html
