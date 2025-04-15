#!/usr/bin/env python3
import sys

sys.path.insert(0, 'python_libs.zip')
from cassandra.cluster import Cluster

# Set up Cassandra connection
cassandra_available = True
cluster = Cluster(['cassandra-server'])
session = cluster.connect('search_engine')
insert_stmt = session.prepare("INSERT INTO term_docs (term, doc_id, tf) VALUES (?, ?, ?)")

current_doc_term = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    doc_id, term, count = line.split('\t')
    count = int(count)

    # Group by (doc_id, term) to aggregate term frequency
    key = (doc_id, term)
    if current_doc_term == key:
        # Accumulate count if it's the same term in the same doc
        current_count += count
    else:
        if current_doc_term:
            doc, word = current_doc_term
            if cassandra_available:
                session.execute(insert_stmt, (word, doc, current_count))

            else:
                print(f"{doc}\t{word}\t{current_count}")

        # Start counting for the new key
        current_doc_term = key
        current_count = count

# Insert the last key
if current_doc_term:
    doc, word = current_doc_term
    if cassandra_available:
        session.execute(insert_stmt, (word, doc, current_count))

    else:
        print(f"{doc}\t{word}\t{current_count}")


cluster.shutdown()
