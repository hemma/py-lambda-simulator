

test:
	poetry run pytest

test_coverage:
	poetry run coverage run --omit="*/test*" -m pytest
	poetry run coverage report -m

format:
	poetry run black py_lambda_simulator tests

publish:
	poetry build
	poetry publish
