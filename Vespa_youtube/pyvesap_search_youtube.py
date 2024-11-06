# Import necessary libraries
import pandas as pd
from vespa.application import Vespa
from vespa.io import VespaResponse, VespaQueryResponse

# Function to display hits as a DataFrame
def display_hits_as_df(response: VespaQueryResponse, fields) -> pd.DataFrame:
    records = []
    for hit in response.hits:
        record = {}
        for field in fields:
            record[field] = hit["fields"].get(field, None)
        records.append(record)
    return pd.DataFrame(records)

# Keyword search function using the BM25 ranking profile
def keyword_search(app, search_query):
    query = {
        "yql": "select * from sources * where userQuery() limit 5",
        "query": search_query,
        "ranking": "bm25",
    }
    response = app.query(query)
    return display_hits_as_df(response, ["doc_id", "title", "text", "views", "likes", "comment_count"])

# Semantic search function using embedding
def semantic_search(app, query):
    query = {
        "yql": "select * from sources * where ({targetHits:100}nearestNeighbor(embedding,e)) limit 5",
        "query": query,
        "ranking": "semantic",
        "input.query(e)": "embed(@query)"
    }
    response = app.query(query)
    return display_hits_as_df(response, ["doc_id", "title", "text", "views", "likes", "comment_count"])

# Retrieve embedding for a specific doc_id
def get_embedding(doc_id):
    query = {
        "yql" : f"select doc_id, title, text, embedding from doc where doc_id contains \'{doc_id}\'",
        "hits": 1
    }
    result = app.query(query)
    
    if result.hits:
        return result.hits[0]
    return None

# Query for recommendations based on embedding
def query_videos_by_embedding(embedding_vector):
    query = {
        'hits': 5,
        'yql': 'select * from doc where ({targetHits:5}nearestNeighbor(embedding, user_embedding))',
        'ranking.features.query(user_embedding)': str(embedding_vector),
        'ranking.profile': 'recommendation'
    }
    return app.query(query)

# Replace with the host and port of your local Vespa instance
app = Vespa(url="http://localhost", port=8080)

# Sample queries for testing
query = "Freakonomics"

# Perform keyword search
df = keyword_search(app, query)
print(df.head())

# Perform semantic search
df = semantic_search(app, query)
print(df.head())

# Retrieve embedding for a specific doc ID and use it for recommendations
emb = get_embedding("2kyS6SvSYSE")  # Sample doc_id
if emb:
    results = query_videos_by_embedding(emb["fields"]["embedding"])
    df = display_hits_as_df(results, ["doc_id", "title", "text", "views", "likes", "comment_count"])
    print(df.head())