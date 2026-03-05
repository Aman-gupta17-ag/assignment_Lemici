# MoSPI Scraper + RAG
.PHONY: up down crawl parse report etl test lint

up:
	docker compose up -d

down:
	docker compose down

crawl:
	python -m scraper.crawl --seed-url https://mospi.gov.in/download-reports --max-pages 5

parse:
	python -m scraper.parse

report:
	python -m scraper.report

etl:
	python -m pipeline.etl

test:
	pytest tests/ scraper/test_crawl.py pipeline/test_etl.py -v

lint:
	black scraper pipeline rag tests
	mypy scraper pipeline rag --ignore-missing-imports
