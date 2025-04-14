from pyspark import SparkConf, SparkContext
from cassandra.cluster import Cluster
import sys
import math

# Function to connect to Cassandra and retrieve index data
def get_cassandra_data(keyspace, table):
    cluster = Cluster(['cassandra-server'])  # Replace with your Cassandra server
    session = cluster.connect(keyspace)
    rows = session.execute(f'SELECT * FROM {table}')
    return rows

# Function to calculate BM25 score
def bm25_score(doc_id, query, term_stats, term_docs, doc_stats, corpus_stats):
    k1 = 1.5
    b = 0.75
    N = corpus_stats['total_docs']  # Total number of documents in the corpus
    dlavg = corpus_stats['avg_dl']  # Average document length
    
    # Get the document length for this doc_id
    dl = doc_stats.get(doc_id, 0)
    
    score = 0
    for term in query:
        # Term Frequency (tf(t, d)) for this term in the document
        tf = next((t['tf'] for t in term_docs if t['term'] == term and t['doc_id'] == doc_id), 0)
        
        # Document Frequency (df(t)) for this term
        df = term_stats.get(term, 0)
        
        # Inverse Document Frequency (idf)
        idf = math.log((N - df + 0.5) / (df + 0.5) + 1.0)
        
        # BM25 score for the term in the document
        term_score = idf * ((tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (dl / dlavg))))
        score += term_score
    return score

# Function to rank documents using BM25
def rank_documents(query, term_stats, term_docs, doc_stats, corpus_stats, docs):
    scores = []
    for doc in docs:
        doc_id = doc['doc_id']
        score = bm25_score(doc_id, query, term_stats, term_docs, doc_stats, corpus_stats)
        scores.append((doc_id, score))
    return sorted(scores, key=lambda x: x[1], reverse=True)[:10]

# Main function to handle user input and output results
if __name__ == "__main__":
    # Initialize Spark
    conf = SparkConf().setAppName("BM25Ranker")
    sc = SparkContext(conf=conf)

    # Cassandra connection and data retrieval
    keyspace = 'search_engine'  # Cassandra keyspace
    term_stats_rows = get_cassandra_data(keyspace, 'term_stats')
    term_docs_rows = get_cassandra_data(keyspace, 'term_docs')
    doc_stats_rows = get_cassandra_data(keyspace, 'doc_stats')
    corpus_stats_rows = get_cassandra_data(keyspace, 'corpus_stats')

    # Convert rows to dictionaries for easy lookup
    term_stats = {row['term']: row['df'] for row in term_stats_rows}
    term_docs = [{'term': row['term'], 'doc_id': row['doc_id'], 'tf': row['tf']} for row in term_docs_rows]
    doc_stats = {row['doc_id']: row['dl'] for row in doc_stats_rows}
    corpus_stats = corpus_stats_rows[0]  # Assuming only one row for corpus stats (total_docs, avg_dl)

    # Get query from stdin
    query = sys.stdin.read().strip().split()

    # List of documents (assuming we can get them from somewhere, or you can load them manually)
    docs = [{"doc_id": doc['doc_id']} for doc in doc_stats_rows]  # Use doc_ids from doc_stats

    # Rank documents based on the query
    top_docs = rank_documents(query, term_stats, term_docs, doc_stats, corpus_stats, docs)

    # Print the top 10 documents (ID and score)
    for doc_id, score in top_docs:
        print(f"Doc ID: {doc_id}, Score: {score}")

    sc.stop()
