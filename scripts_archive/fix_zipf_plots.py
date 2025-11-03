"""
Fix Zipf's Law plots - Use log scale ONLY on x-axis (not y-axis)
Regenerate combined_zipf_before_preprocessing.png and combined_zipf_after_preprocessing.png
"""

import sys
import os
from collections import Counter
import matplotlib.pyplot as plt

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '.')))

from src.data_loader import load_wiki_data, load_news_data
from src.preprocessor import preprocess_text
import itertools

print("Loading data...")
# Load 5000 articles from each dataset
wiki_iterable = load_wiki_data("data/wiki/")
wiki_sample = list(itertools.islice(wiki_iterable, 5000))
wiki_corpus = " ".join([doc['content'] for doc in wiki_sample])
print(f"✓ Loaded {len(wiki_sample)} Wikipedia articles")

news_iterable = load_news_data("data/News_Datasets/")
news_sample = list(itertools.islice(news_iterable, 5000))
news_corpus = " ".join([doc['content'] for doc in news_sample])
print(f"✓ Loaded {len(news_sample)} News articles")

combined_corpus = wiki_corpus + " " + news_corpus
print(f"✓ Combined corpus ready")

def create_zipf_plot_log_x_only(corpus_text, is_preprocessed, output_filename):
    """
    Create Zipf's Law plot with log scale ONLY on x-axis (not y-axis)
    This matches the notebook implementation.
    """
    if is_preprocessed:
        tokens = preprocess_text(corpus_text)
        token_counts = Counter(tokens)
        title_suffix = "With Preprocessing"
        xlabel = "Token Frequency (log scale)"
        ylabel = "Number of Tokens with that Frequency"
        color = 'green'
    else:
        words = corpus_text.lower().split()
        token_counts = Counter(words)
        title_suffix = "Without Preprocessing"
        xlabel = "Word Frequency (log scale)"
        ylabel = "Number of Words with that Frequency"
        color = 'blue'
    
    # Count frequency of frequencies
    count_of_counts = Counter(token_counts.values())
    x = sorted(count_of_counts.keys())
    y = [count_of_counts[k] for k in x]
    
    # Create plot with ONLY x-axis log scale
    plt.figure(figsize=(10, 6))
    plt.plot(x, y, marker=".", linestyle="-", color=color, linewidth=1.5, markersize=4)
    plt.xscale("log")  # Log scale on x-axis ONLY
    # NO log scale on y-axis (this is the key fix!)
    plt.xlabel(xlabel, fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    plt.title(f"Zipf's Law Distribution for Combined Data ({title_suffix})", fontsize=14)
    plt.grid(True, which="both", ls="--", lw=0.5, alpha=0.7)
    plt.tight_layout()
    
    # Save with high DPI
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"✓ Saved: {output_filename}")
    print(f"  - Unique tokens: {len(token_counts):,}")
    print(f"  - Total tokens: {sum(token_counts.values()):,}")
    print(f"  - Log scale: X-axis ONLY (Y-axis linear)")

print("\n--- Generating Zipf Plots (Log X-axis ONLY) ---")

# Plot 1: Before preprocessing (log x-axis only)
create_zipf_plot_log_x_only(
    combined_corpus,
    is_preprocessed=False,
    output_filename="plots/combined_zipf_before_preprocessing.png"
)

print()

# Plot 2: After preprocessing (log x-axis only)
create_zipf_plot_log_x_only(
    combined_corpus,
    is_preprocessed=True,
    output_filename="plots/combined_zipf_after_preprocessing.png"
)

print("\n✓ All Zipf plots regenerated with correct scaling (log x-axis, linear y-axis)")
