#!/usr/bin/env python3
import sys
sys.path.insert(0, 'python_libs.zip')

from cassandra.cluster import Cluster

CASSANDRA_ENABLED = True

cluster = Cluster(['cassandra-server'])
session = cluster.connect('search_engine')

current_doc = None
term_count = 0

total_docs = 0
total_terms = 0

for line in sys.stdin:
    doc_id, count = line.strip().split("\t")
    count = int(count)


    if doc_id != current_doc:
        if current_doc is not None:
            if CASSANDRA_ENABLED:
                session.execute(
                    "INSERT INTO doc_stats (doc_id, dl) VALUES (%s, %s)",
                    (current_doc, term_count)
                )
            else:
                print(f"{current_doc}\t{term_count}")
            total_docs += 1
            total_terms += term_count

        current_doc = doc_id
        term_count = count
    else:
        term_count += count

if current_doc is not None:
    if CASSANDRA_ENABLED:
        session.execute(
            "INSERT INTO doc_stats (doc_id, dl) VALUES (%s, %s)",
            (current_doc, term_count)
        )
    else:
        print(f"{current_doc}\t{term_count}")
    total_docs += 1
    total_terms += term_count


dlavg = total_terms / total_docs if total_docs > 0 else 0.0
session.execute(
        "INSERT INTO corpus_stats (id, total_docs, avg_dl) VALUES (%s, %s, %s)",
        ("global", total_docs, dlavg)
    )
cluster.shutdown()