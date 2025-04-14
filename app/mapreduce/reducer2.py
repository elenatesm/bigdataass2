import sys
sys.path.insert(0, 'python_libs.zip')

from cassandra.cluster import Cluster
CASSANDRA_ENABLED = True

cluster = Cluster(['cassandra-server'])
session = cluster.connect('search_engine')

current_word = None
doc_ids = set()

for line in sys.stdin:
    word, doc_id = line.strip().split("\t")

    if word != current_word:
        if current_word is not None:
            df = len(doc_ids)
            if CASSANDRA_ENABLED:
                session.execute(
                    "INSERT INTO term_stats (term, df) VALUES (%s, %s)",
                    (current_word, df)
                )
            else:
                print(f"{current_word}\t{df}")  # For local debug
        current_word = word
        doc_ids = set()

    doc_ids.add(doc_id)

# Final term
if current_word is not None:
    df = len(doc_ids)
    if CASSANDRA_ENABLED:
        session.execute(
            "INSERT INTO term_stats (term, df) VALUES (%s, %s)",
            (current_word, df)
        )
    else:
        print(f"{current_word}\t{df}")

# Close Cassandra connection
if CASSANDRA_ENABLED:
    cluster.shutdown()
