include .env

LOCAL_NEO4J_VERSION=5.19.0

notebook:
	poetry run jupyter notebook --notebook-dir=examples

neo:
	docker run \
		-p=7474:7474 -p=7687:7687 \
		--name neo4j \
		--env NEO4J_AUTH=neo4j/${NEO4J_PASSWORD} \
		--volume=$(shell pwd)/.data/graph:/data neo4j:${LOCAL_NEO4J_VERSION} 2>&1 || docker start neo4j

test-neo:
	docker run \
		-p=7484:7474 -p=7697:7687 \
		--name neo4j-test \
		--env NEO4J_AUTH=neo4j/${NEO4J_PASSWORD} \
		--volume=$(shell pwd)/.data/test-graph:/data neo4j:${LOCAL_NEO4J_VERSION} 2>&1 || docker start neo4j-test

test: test-neo
	poetry run pytest -vv tests
