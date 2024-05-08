from contextlib import contextmanager
import os

from neo4j import GraphDatabase


@contextmanager
def get_driver():
    uri = os.getenv("NEO4J_URI", "neo4j://localhost")
    username = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "neo4j")
    AUTH = (username, password)

    with GraphDatabase.driver(uri, auth=AUTH) as driver:
        driver.verify_connectivity()
        yield driver
