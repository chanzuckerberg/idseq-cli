
.PHONY: lint

all: lint test

lint:
	@flake8

test:
	@echo 'done'