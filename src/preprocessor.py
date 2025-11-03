# src/preprocessor.py

import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer

# It's good practice to initialize these once
stop_words = set(stopwords.words("english"))
stemmer = PorterStemmer()

def preprocess_text(text: str) -> list[str]:
    """
    This is YOUR preprocessing function from Indexing.ipynb.
    It lowercases, removes URLs/HTML/symbols, tokenizes, removes stopwords, and stems.
    """
    text = text.lower() # Lowercasing
    text = re.sub(r"http\S+|www\S+|https\S+", '', text, flags=re.MULTILINE) # Removing URLs
    text = re.sub(r'<.*?>', '', text)  # Removing HTML tags
    text = re.sub(r'[^a-z\s]', ' ', text)  # Remove special symbols & punctuation
    
    # Tokenization
    tokens = word_tokenize(text)
    
    # stopwords & very short tokens removal
    tokens = [word for word in tokens if word not in stop_words and len(word) > 2]
    
    # Stemming
    tokens = [stemmer.stem(word) for word in tokens]
    
    return tokens