import time
import psutil
from datetime import datetime
import requests
import pandas as pd
import io
import re
import torch
import nltk
from nltk.corpus import stopwords
from sentence_transformers import SentenceTransformer, util

# Download stopwords if not already present
nltk.download("stopwords")

# Load stopwords from nltk
STOP_WORDS = set(stopwords.words("english"))

# Load NLP model
model = SentenceTransformer("paraphrase-MiniLM-L6-v2")

# Function to preprocess text
def preprocess(text):
    if not isinstance(text, str) or not text.strip():  # Ensure input is valid
        return "empty_text"  # Return placeholder text
    
    # Tokenization using regex (alternative to Spacy)
    tokens = re.findall(r'\b\w+\b', text.lower())  # Extract words only
    tokens = [word for word in tokens if word not in STOP_WORDS]  # Remove stopwords

    return " ".join(tokens) if tokens else "empty_text"

# Function to calculate cosine similarity
def calculate_similarity(embedding1, embedding2):
    return util.pytorch_cos_sim(embedding1, embedding2)

# Function to compute similarity scores
def calculate_similarity_scores(new_comments, base_comments):
    new_comments_preprocessed = [preprocess(comment) for comment in new_comments]
    base_comments_preprocessed = [preprocess(comment) for comment in base_comments]
    
    if not new_comments_preprocessed or not base_comments_preprocessed:
        print("Warning: Empty input provided to similarity function.")
        return torch.tensor([])  
    new_embeddings = model.encode(new_comments_preprocessed, convert_to_tensor=True)
    base_embeddings = model.encode(base_comments_preprocessed, convert_to_tensor=True)
    
    print("New embeddings shape:", new_embeddings.shape)
    print("Base embeddings shape:", base_embeddings.shape)
    
    if new_embeddings.shape[0] == 0 or base_embeddings.shape[0] == 0:
        print("Error: One of the embeddings is empty!")
        return torch.tensor([])

    similarity_matrix = calculate_similarity(new_embeddings, base_embeddings) 
    return similarity_matrix 


if __name__ == "__main__":
    start_time = datetime.now()
    
    df = pd.DataFrame({"Target sentence": [
        "first quarter 2023 financial results revenue was $134.3 million for the first quarter of 2023, a 17% increase from $114.5 million for the corresponding prior year period."
    ]})
    
    targetSentences = df['Target sentence'].to_list()
    intent = ["What are you working on this weekend?", "Are you building today?", "What are you building today?", "Hello builders, it's Saturday but we're still building."]

    similarity_scores = calculate_similarity_scores(targetSentences, intent)
    df["Similarity Score"] = similarity_scores.tolist()
    df.to_csv("Test_result.csv", index=False)
    
    stop_time = datetime.now()
    print(f"Time taken: {(stop_time - start_time).total_seconds()} seconds")
