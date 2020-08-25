import os
from neo4j import GraphDatabase
from dataclasses import dataclass
from typing import List

# Constants
URI_KEY = "NEO4J_URI"
USER_KEY = "NEO4J_USER"
PASSWORD_KEY = "NEO4J_PASS"
CONTENT_ID = "content_id"
PHRASE = "phrase"


SENTENCE_QUERY_CONTENT_PAGING = \
    """
MATCH (:SERIES{name:{name}}) - [:HAS_CONTENT] -> (content:CONTENT)
RETURN content.phrase as phrase, content.id as content_id
SKIP {page}
LIMIT {max_items}
"""


@dataclass(frozen=True)
class Content():
    id: str
    phrase: str


class UtilNeo4j():

    def init(self):
        uri = os.environ.get(URI_KEY)
        user = os.environ.get(USER_KEY)
        password = os.environ.get(PASSWORD_KEY)
        self.driver_neo4j = GraphDatabase.driver(uri, auth=(user, password))

    def close(self) -> None:
        self.driver_neo4j.close()

    def get_content(self, name: str, page: int, max_items: int) -> List[Content]:
        self.init()
        with self.driver_neo4j.session() as session:
            records = session.read_transaction(
                self._get_content_neo4j, name, page, max_items)

            contents: List[Content] = []

            if(records):
                for record in records:
                    contents.append(
                        Content(
                            record[CONTENT_ID],
                            record[PHRASE]
                        )
                    )

        return contents

    @staticmethod
    def _get_content_neo4j(tx, name: str, page: int, max_items: int):
        return tx.run(SENTENCE_QUERY_CONTENT_PAGING, name=name, page=page*max_items, max_items=max_items)
