# Makefile for StoreMy project
# Thin interface — all logic lives in makefile.py
# Usage: make <target>  or  python makefile.py <target>

.DEFAULT_GOAL := help

.PHONY: test test-tables test-coverage test-watch test-watch-tables \
        install-tools clean build fmt vet lint lint-fix \
        run run-fresh tidy coverage-html bench check examples help \
        docker-demo docker-import docker-fresh docker-test \
        docker-build docker-clean docker-stop quickstart

# ── Testing ────────────────────────────────────────────────────────────────────
test:
	python makefile.py test

test-tables:
	python makefile.py test-tables

test-coverage:
	python makefile.py test-coverage

test-watch:
	python makefile.py test-watch

test-watch-tables:
	python makefile.py test-watch-tables

# ── Tools ──────────────────────────────────────────────────────────────────────
install-tools:
	python makefile.py install-tools

clean:
	python makefile.py clean

# ── Build ──────────────────────────────────────────────────────────────────────
build:
	python makefile.py build

fmt:
	python makefile.py fmt

vet:
	python makefile.py vet

lint:
	python makefile.py lint

lint-fix:
	python makefile.py lint-fix

tidy:
	python makefile.py tidy

coverage-html:
	python makefile.py coverage-html

bench:
	python makefile.py bench

check:
	python makefile.py check

# ── Run ────────────────────────────────────────────────────────────────────────
run:
	python makefile.py run

run-fresh:
	python makefile.py run-fresh

examples:
	python makefile.py examples

# ── Docker ─────────────────────────────────────────────────────────────────────
docker-demo:
	python makefile.py docker-demo

docker-import:
	python makefile.py docker-import

docker-fresh:
	python makefile.py docker-fresh

docker-test:
	python makefile.py docker-test

docker-build:
	python makefile.py docker-build

docker-clean:
	python makefile.py docker-clean

docker-stop:
	python makefile.py docker-stop

quickstart:
	python makefile.py quickstart

# ── Help ───────────────────────────────────────────────────────────────────────
help:
	@python makefile.py help
