from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['cassandra-server'])
session = cluster.connect()

# Create keyspace
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_engine 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
session.set_keyspace('search_engine')

# Table 1: term_docs stores tf(t, d)
session.execute("""
    CREATE TABLE IF NOT EXISTS term_docs (
        term text,
        doc_id text,
        tf int,
        PRIMARY KEY (term, doc_id)
    )
""")

# Table 2: term_stats stores df(t)
session.execute("""
    CREATE TABLE IF NOT EXISTS term_stats (
        term text PRIMARY KEY,
        df int
    )
""")

# Table 3: doc_stats stores dl(d)
session.execute("""
    CREATE TABLE IF NOT EXISTS doc_stats (
        doc_id text PRIMARY KEY,
        dl int
    )
""")

# Table 4: corpus_stats stores global stats: N and dlavg
session.execute("""
    CREATE TABLE IF NOT EXISTS corpus_stats (
        id text PRIMARY KEY,
        total_docs int,
        avg_dl float
    )
""")

cluster.shutdown()
