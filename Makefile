.PHONY: lint

all: lint test

lint:
	flake8

test:
	@echo 'done'

release:
	-rm -rf dist
	python setup.py sdist bdist_wheel
	twine upload dist/*.tar.gz dist/*.whl --sign --verbose
