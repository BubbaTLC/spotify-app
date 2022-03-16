
from py2neo import Graph
import apache_beam as beam

import json

from config import NEO4J_DATABASE, NEO4J_HOST, NEO4J_PASSWORD, NEO4J_USER




class Neo4jLoader(beam.DoFn):
    def __init__(self, host, user, password, query, database='neo4j', batch_size=2000):
        super().__init__()
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.batch = []
        self.batch_size = batch_size
        self.query = query

    def process(self, element, *args, **kwargs):
        self.amount = len(element)
        self.batch.append(element)
        if self.amount >= self.batch_size:
            self._flush()
            self.amount = 0

    def start_bundle(self):
        self.amount = 0
    
    def finish_bundle(self):
        self._flush()
        self.batch = []
    
    def _flush(self) -> None:
        if len(self.batch) == 0:
            return None
        with Neo4jWriter(
                host=self.host,
                database=self.database, 
                user=self.user, 
                password=self.password,
                query=self.query
            ) as writer:
            writer.write_batch(self.batch)
        self.batch = []

class Neo4jWriter():
    def __init__(self, host, database, user, password, query) -> None:
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.query = query
        self.graph = None

    def write_batch(self, batch) -> None:
        g = self.graph
        try:
            tx = g.begin()
            for x in batch:
                tx.run(self.query, parameters={"props": x})
            g.commit(tx)
        except Exception as e:
            print(e)
        return None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.graph = None

    def __enter__(self):
        if self.graph is None:
            try:
                self.graph = Graph(self.host, name=self.database, auth=(self.user, self.password))
            except Exception as e:
                print(e)
            return self

def destroy() -> None:
    graph = Graph(NEO4J_HOST, name=NEO4J_DATABASE, auth=(NEO4J_USER, NEO4J_PASSWORD))
    res = graph.run("MATCH (n) DETACH DELETE n")
    print(res.stats())
    return None


def main() -> None:
    return None




if __name__ == '__main__':
    main()


