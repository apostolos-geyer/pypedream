# Target for just running the tests
test:
	poetry run pytest

# Target for generating coverage reports
coverage:
	poetry run pytest --cov=pypedream --cov-report=html:_docsrc/_static/coverage

# Target for building Sphinx documentation, depends on the coverage target
docs: coverage
	poetry run python -m sphinx -b html _docsrc docs

build: test
	poetry build



.PHONY: test coverage docs
