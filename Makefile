include .env

notebook:
	poetry run jupyter notebook --notebook-dir=examples

neo:
	docker run --restart always \
		-p=7474:7474 -p=7687:7687 \
		--name neo4j \
		--env NEO4J_AUTH=neo4j/${NEO4J_PASSWORD} \
		--volume=$(shell pwd)/.data/graph:/data neo4j:5.19.0 || docker start neo4j

test:
	poetry run pytest -vv tests
