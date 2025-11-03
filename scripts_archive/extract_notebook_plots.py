"""
Extract and save all plots from data_analysis notebook
"""
import itertools
from collections import Counter
import matplotlib.pyplot as plt
import sys
import os

# Add the parent directory of 'src' to sys.path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '.')))

# Import ALL our helper functions
from src.data_loader import load_wiki_data, load_news_data
from src.preprocessor import preprocess_text

def save_plots(corpus_name: str, raw_corpus: str, prefix: str):
    """
    Takes a corpus, performs analysis, and saves four plots.
    """
    print(f"--- Analyzing and Saving Plots for: {corpus_name} ---")
    
    # Create plots directory if it doesn't exist
    os.makedirs('plots', exist_ok=True)
    
    # --- Without Preprocessing ---
    raw_words = raw_corpus.lower().split()
    raw_word_counts = Counter(raw_words)
    top_20_raw = raw_word_counts.most_common(20)
    
    print(f"Top 20 raw words for {corpus_name}:")
    print(top_20_raw[:5], "...")
    
    # Plot 1: Top 20 Bar Chart (Before)
    words, counts = zip(*top_20_raw)
    plt.figure(figsize=(12, 6))
    plt.bar(words, counts, color='skyblue')
    plt.title(f"Top 20 Words (Before Preprocessing)", fontsize=14, fontweight='bold')
    plt.xlabel("Words", fontsize=12)
    plt.ylabel("Frequency", fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(f'plots/{prefix}_top20_before_preprocessing.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved: plots/{prefix}_top20_before_preprocessing.png")

    # Plot 2: Zipf's Law Plot (Before)
    # Get rank-frequency data for Zipf's law
    sorted_counts = sorted(raw_word_counts.values(), reverse=True)
    ranks = list(range(1, len(sorted_counts) + 1))
    
    plt.figure(figsize=(10, 6))
    plt.loglog(ranks, sorted_counts, marker=".", linestyle="-", markersize=3, alpha=0.7)
    plt.xlabel("Rank (log scale)", fontsize=12)
    plt.ylabel("Frequency (log scale)", fontsize=12)
    plt.title(f"Zipf's Law Distribution (Before Preprocessing)", fontsize=14, fontweight='bold')
    plt.grid(True, which="both", ls="--", lw=0.5, alpha=0.5)
    plt.tight_layout()
    plt.savefig(f'plots/{prefix}_zipf_before_preprocessing.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved: plots/{prefix}_zipf_before_preprocessing.png")

    # --- With Preprocessing ---
    print(f"Preprocessing {corpus_name}...")
    processed_tokens = preprocess_text(raw_corpus)
    processed_token_counts = Counter(processed_tokens)
    top_20_processed = processed_token_counts.most_common(20)

    print(f"Top 20 processed tokens for {corpus_name}:")
    print(top_20_processed[:5], "...")
    
    # Plot 3: Top 20 Bar Chart (After)
    tokens, counts = zip(*top_20_processed)
    plt.figure(figsize=(12, 6))
    plt.bar(tokens, counts, color='lightgreen')
    plt.title(f"Top 20 Tokens (After Preprocessing)", fontsize=14, fontweight='bold')
    plt.xlabel("Tokens (Stemmed)", fontsize=12)
    plt.ylabel("Frequency", fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(f'plots/{prefix}_top20_after_preprocessing.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved: plots/{prefix}_top20_after_preprocessing.png")
    
    # Plot 4: Zipf's Law Plot (After)
    sorted_token_counts = sorted(processed_token_counts.values(), reverse=True)
    token_ranks = list(range(1, len(sorted_token_counts) + 1))
    
    plt.figure(figsize=(10, 6))
    plt.loglog(token_ranks, sorted_token_counts, marker=".", linestyle="-", color='green', markersize=3, alpha=0.7)
    plt.xlabel("Rank (log scale)", fontsize=12)
    plt.ylabel("Frequency (log scale)", fontsize=12)
    plt.title(f"Zipf's Law Distribution (After Preprocessing)", fontsize=14, fontweight='bold')
    plt.grid(True, which="both", ls="--", lw=0.5, alpha=0.5)
    plt.tight_layout()
    plt.savefig(f'plots/{prefix}_zipf_after_preprocessing.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved: plots/{prefix}_zipf_after_preprocessing.png")
    
    # Statistics
    vocab_before = len(raw_word_counts)
    vocab_after = len(processed_token_counts)
    reduction = (1 - vocab_after/vocab_before) * 100
    
    print(f"\nVocabulary Statistics for {corpus_name}:")
    print(f"  Before preprocessing: {vocab_before:,} unique words")
    print(f"  After preprocessing: {vocab_after:,} unique tokens")
    print(f"  Reduction: {reduction:.1f}%")
    print("-" * 60)
    
    return vocab_before, vocab_after

def create_vocabulary_comparison_plot(stats):
    """Create a bar plot comparing vocabulary sizes"""
    plt.figure(figsize=(10, 6))
    
    categories = ['Wikipedia', 'News', 'Combined']
    before = [stats[cat][0] for cat in categories]
    after = [stats[cat][1] for cat in categories]
    
    x = range(len(categories))
    width = 0.35
    
    plt.bar([i - width/2 for i in x], before, width, label='Before Preprocessing', color='skyblue')
    plt.bar([i + width/2 for i in x], after, width, label='After Preprocessing', color='lightgreen')
    
    plt.xlabel('Corpus', fontsize=12)
    plt.ylabel('Vocabulary Size', fontsize=12)
    plt.title('Vocabulary Size Comparison: Before vs After Preprocessing', fontsize=14, fontweight='bold')
    plt.xticks(x, categories)
    plt.legend()
    plt.grid(axis='y', alpha=0.3)
    
    # Add reduction percentages on top
    for i, cat in enumerate(categories):
        reduction = (1 - stats[cat][1]/stats[cat][0]) * 100
        plt.text(i, max(before[i], after[i]) + 5000, f'-{reduction:.1f}%', 
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('plots/vocabulary_size_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved: plots/vocabulary_size_comparison.png")

if __name__ == "__main__":
    print("="*60)
    print("EXTRACTING PLOTS FROM DATA ANALYSIS NOTEBOOK")
    print("="*60)
    
    # Load a sample of 5,000 articles from each dataset
    print("\nLoading Wikipedia data...")
    wiki_iterable = load_wiki_data("data/wiki/")
    wiki_sample = list(itertools.islice(wiki_iterable, 5000))
    wiki_corpus = " ".join([doc['content'] for doc in wiki_sample])
    print(f"✓ Loaded {len(wiki_sample)} Wikipedia articles")

    print("\nLoading News data...")
    news_iterable = load_news_data("data/News_Datasets/")
    news_sample = list(itertools.islice(news_iterable, 5000))
    news_corpus = " ".join([doc['content'] for doc in news_sample])
    print(f"✓ Loaded {len(news_sample)} News articles")
    
    print("\n" + "="*60)
    
    # Save plots for Wikipedia
    wiki_stats = save_plots("Wikipedia Data", wiki_corpus, "wiki")
    
    # Save plots for News
    news_stats = save_plots("News Data", news_corpus, "news")
    
    # Save plots for Combined
    combined_corpus = wiki_corpus + " " + news_corpus
    combined_stats = save_plots("Combined Data", combined_corpus, "combined")
    
    # Create vocabulary comparison plot
    print("\nCreating vocabulary comparison plot...")
    stats = {
        'Wikipedia': wiki_stats,
        'News': news_stats,
        'Combined': combined_stats
    }
    create_vocabulary_comparison_plot(stats)
    
    print("\n" + "="*60)
    print("ALL PLOTS EXTRACTED SUCCESSFULLY!")
    print("="*60)
    print("\nGenerated files:")
    print("  • wiki_top20_before_preprocessing.png")
    print("  • wiki_zipf_before_preprocessing.png")
    print("  • wiki_top20_after_preprocessing.png")
    print("  • wiki_zipf_after_preprocessing.png")
    print("  • news_top20_before_preprocessing.png")
    print("  • news_zipf_before_preprocessing.png")
    print("  • news_top20_after_preprocessing.png")
    print("  • news_zipf_after_preprocessing.png")
    print("  • combined_top20_before_preprocessing.png")
    print("  • combined_zipf_before_preprocessing.png")
    print("  • combined_top20_after_preprocessing.png")
    print("  • combined_zipf_after_preprocessing.png")
    print("  • vocabulary_size_comparison.png")
    print("\nTotal: 13 plots saved to 'plots/' directory")
