import sys

sys.path.insert(0, 'python_libs.zip')

from cassandra.cluster import Cluster

# Set up Cassandra connection
CASSANDRA_ENABLED = True
cluster = Cluster(['cassandra-server'])
session = cluster.connect('search_engine')

current_word = None
doc_ids = set()

for line in sys.stdin:
    word, doc_id = line.strip().split("\t")

    # When encountering a new term, insert the previous term's DF into Cassandra
    if word != current_word:
        if current_word is not None:
            df = len(doc_ids)
            if CASSANDRA_ENABLED:
                session.execute(
                    "INSERT INTO term_stats (term, df) VALUES (%s, %s)",
                    (current_word, df)
                )
            else:
                print(f"{current_word}\t{df}")  # Print to stdout for debugging

        # Start tracking the new term
        current_word = word
        doc_ids = set()

    doc_ids.add(doc_id)

# Insert the last term's DF after the loop ends
if current_word is not None:
    df = len(doc_ids)
    if CASSANDRA_ENABLED:
        session.execute(
            "INSERT INTO term_stats (term, df) VALUES (%s, %s)",
            (current_word, df)
        )
    else:
        print(f"{current_word}\t{df}")


cluster.shutdown()
