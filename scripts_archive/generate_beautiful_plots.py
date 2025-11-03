"""#!/usr/bin/env python

Beautiful Plot Generation for Information Retrieval Assignment"""

Creates 6 comprehensive, visually appealing comparison plotsBeautiful Comprehensive Plots Generator

"""Creates publication-quality visualizations with all performance metrics



import jsonMetrics displayed:

import matplotlib.pyplot as plt- Latency (avg, median, p95, p99)

import numpy as np- Throughput (QPS)

from pathlib import Path- Memory Footprint (disk + RAM)

import seaborn as sns- Index Size

import warnings- Compression Impact

warnings.filterwarnings('ignore')- TAAT vs DAAT comparison



# Set style for beautiful plotsUsage:

plt.style.use('seaborn-v0_8-darkgrid')    python generate_beautiful_plots.py

sns.set_palette("husl")"""

plt.rcParams['figure.facecolor'] = 'white'

plt.rcParams['axes.facecolor'] = '#f8f9fa'import json

plt.rcParams['font.size'] = 11import numpy as np

plt.rcParams['axes.labelsize'] = 12import matplotlib.pyplot as plt

plt.rcParams['axes.titlesize'] = 14import seaborn as sns

plt.rcParams['legend.fontsize'] = 10from pathlib import Path

plt.rcParams['figure.dpi'] = 100from collections import defaultdict

import pandas as pd

# Create plots directory

PLOTS_DIR = Path("plots")# Set beautiful styling

PLOTS_DIR.mkdir(exist_ok=True)plt.style.use('seaborn-v0_8-whitegrid')

sns.set_palette("husl")

def load_evaluation_file(filepath):COLORS = {

    """Load and return evaluation JSON data"""    'Boolean': '#3498db',    # Blue

    with open(filepath, 'r') as f:    'TF': '#e74c3c',         # Red

        return json.load(f)    'TF-IDF': '#2ecc71',     # Green

    'JSON': '#9b59b6',       # Purple

def create_dual_axis_plot(ax, x_data, y1_data, y2_data, x_labels,     'SQLite': '#f39c12',     # Orange

                          y1_label, y2_label, title, colors):    'None': '#34495e',       # Dark gray

    """Create beautiful dual-axis bar plot"""    'VByte': '#1abc9c',      # Turquoise

    x_pos = np.arange(len(x_labels))    'Zlib': '#e67e22'        # Burnt orange

    width = 0.35}

    

    # Plot first metricdef load_results(filepath='results/full_evaluation.json'):

    bars1 = ax.bar(x_pos - width/2, y1_data, width,     """Load evaluation results"""

                   label=y1_label, color=colors[0], alpha=0.8,     with open(filepath, 'r') as f:

                   edgecolor='white', linewidth=1.5)        data = json.load(f)

    ax.set_ylabel(y1_label, color=colors[0], fontweight='bold')    return data['results']

    ax.tick_params(axis='y', labelcolor=colors[0])

    

    # Add value labels on barsdef create_performance_dashboard(results, output_dir='plots'):

    for bar in bars1:    """

        height = bar.get_height()    Create a comprehensive performance dashboard

        ax.text(bar.get_x() + bar.get_width()/2., height,    Shows latency, throughput, memory in one view

                f'{height:.1f}', ha='center', va='bottom',     """

                fontsize=9, fontweight='bold')    print("\n📊 Generating Performance Dashboard...")

        

    # Create second y-axis    # Prepare data

    ax2 = ax.twinx()    data = []

    bars2 = ax2.bar(x_pos + width/2, y2_data, width,     for r in results:

                    label=y2_label, color=colors[1], alpha=0.8,        config = f"{r['index_type']}\n{r['compression']}"

                    edgecolor='white', linewidth=1.5)        

    ax2.set_ylabel(y2_label, color=colors[1], fontweight='bold')        # TAAT metrics

    ax2.tick_params(axis='y', labelcolor=colors[1])        if 'taat' in r:

                data.append({

    # Add value labels on bars                'Config': config,

    for bar in bars2:                'Type': r['index_type'],

        height = bar.get_height()                'Compression': r['compression'],

        ax2.text(bar.get_x() + bar.get_width()/2., height,                'Datastore': r['datastore'],

                 f'{height:.0f}', ha='center', va='bottom',                'Mode': 'TAAT',

                 fontsize=9, fontweight='bold')                'Avg Latency (ms)': r['taat'].get('avg_time_ms', 0),

                    'Median Latency (ms)': r['taat'].get('median_time_ms', 0),

    ax.set_xlabel('Configuration', fontweight='bold')                'P95 Latency (ms)': r['taat'].get('p95_time_ms', 0),

    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)                'P99 Latency (ms)': r['taat'].get('p99_time_ms', 0),

    ax.set_xticks(x_pos)                'Memory (MB)': r['taat'].get('avg_memory_mb', 0),

    ax.set_xticklabels(x_labels, fontweight='bold')                'Index Size (MB)': r.get('index_size_mb', 0)

                })

    # Combined legend        

    lines1, labels1 = ax.get_legend_handles_labels()        # DAAT metrics

    lines2, labels2 = ax2.get_legend_handles_labels()        if 'daat' in r:

    ax.legend(lines1 + lines2, labels1 + labels2,             data.append({

              loc='upper left', framealpha=0.95, edgecolor='gray')                'Config': config,

                    'Type': r['index_type'],

    ax.grid(True, alpha=0.3, axis='y')                'Compression': r['compression'],

                    'Datastore': r['datastore'],

    return ax, ax2                'Mode': 'DAAT',

                'Avg Latency (ms)': r['daat'].get('avg_time_ms', 0),

                'Median Latency (ms)': r['daat'].get('median_time_ms', 0),

def plot_index_types():                'P95 Latency (ms)': r['daat'].get('p95_time_ms', 0),

    """Plot C: Compare Boolean (i1) vs TF (i2) vs TF-IDF (i3)"""                'P99 Latency (ms)': r['daat'].get('p99_time_ms', 0),

    print("📊 Generating Plot C: Index Types Comparison...")                'Memory (MB)': r['daat'].get('avg_memory_mb', 0),

                    'Index Size (MB)': r.get('index_size_mb', 0)

    # Load data for i1d1c1o0_qTAAT, i2d1c1o0_qTAAT, i3d1c1o0_qTAAT            })

    configs = [    

        ('i1d1c1o0', 'Boolean (i1)'),    df = pd.DataFrame(data)

        ('i2d1c1o0', 'TF (i2)'),    

        ('i3d1c1o0', 'TF-IDF (i3)')    # Create 2x2 dashboard

    ]    fig, axes = plt.subplots(2, 2, figsize=(20, 14))

        fig.suptitle('🚀 Index Performance Dashboard', fontsize=22, fontweight='bold', y=0.995)

    latencies = []    

    throughputs = []    # 1. Average Latency Comparison

    memories = []    ax1 = axes[0, 0]

        taat_data = df[df['Mode'] == 'TAAT'].groupby('Type')['Avg Latency (ms)'].mean()

    for config, label in configs:    daat_data = df[df['Mode'] == 'DAAT'].groupby('Type')['Avg Latency (ms)'].mean()

        filepath = f"results/eval_SelfIndex_{config}_qTAAT.json"    

        data = load_evaluation_file(filepath)    x = np.arange(len(taat_data))

        latencies.append(data['artifact_A']['latency_p95_ms'])    width = 0.35

        throughputs.append(data['artifact_B']['throughput_qps'])    

        memories.append(data['artifact_C']['memory_mb'])    bars1 = ax1.bar(x - width/2, taat_data.values, width, label='TAAT', 

                         color='#3498db', alpha=0.8, edgecolor='black', linewidth=1.5)

    # Create figure with subplots    bars2 = ax1.bar(x + width/2, daat_data.values, width, label='DAAT', 

    fig = plt.figure(figsize=(16, 5))                     color='#e74c3c', alpha=0.8, edgecolor='black', linewidth=1.5)

        

    # Subplot 1: Latency vs Throughput    ax1.set_xlabel('Index Type', fontsize=14, fontweight='bold')

    ax1 = plt.subplot(1, 3, 1)    ax1.set_ylabel('Average Latency (ms)', fontsize=14, fontweight='bold')

    create_dual_axis_plot(ax1, None, latencies, throughputs,     ax1.set_title('Average Query Latency by Index Type', fontsize=16, fontweight='bold', pad=15)

                          [c[1] for c in configs],    ax1.set_xticks(x)

                          'Latency P95 (ms)', 'Throughput (QPS)',    ax1.set_xticklabels(taat_data.index, fontsize=12)

                          'Index Types - Performance',    ax1.legend(fontsize=12, frameon=True, shadow=True)

                          ['#e74c3c', '#27ae60'])    ax1.grid(axis='y', alpha=0.3, linestyle='--')

        

    # Subplot 2: Memory Usage    # Add value labels

    ax2 = plt.subplot(1, 3, 2)    for bars in [bars1, bars2]:

    bars = ax2.bar([c[1] for c in configs], memories,         for bar in bars:

                   color=['#3498db', '#9b59b6', '#e67e22'],             height = bar.get_height()

                   alpha=0.8, edgecolor='white', linewidth=2)            ax1.text(bar.get_x() + bar.get_width()/2., height,

    ax2.set_ylabel('Memory (MB)', fontweight='bold')                    f'{height:.3f}',

    ax2.set_xlabel('Index Type', fontweight='bold')                    ha='center', va='bottom', fontsize=10, fontweight='bold')

    ax2.set_title('Memory Footprint', fontsize=14, fontweight='bold', pad=20)    

    ax2.grid(True, alpha=0.3, axis='y')    # 2. P95 & P99 Latency (TAAT only for clarity)

        ax2 = axes[0, 1]

    for bar in bars:    taat_df = df[df['Mode'] == 'TAAT'].groupby('Type').agg({

        height = bar.get_height()        'P95 Latency (ms)': 'mean',

        ax2.text(bar.get_x() + bar.get_width()/2., height,        'P99 Latency (ms)': 'mean'

                f'{height:.1f} MB', ha='center', va='bottom',    })

                fontsize=10, fontweight='bold')    

        x = np.arange(len(taat_df))

    # Subplot 3: Efficiency Score    bars1 = ax2.bar(x - width/2, taat_df['P95 Latency (ms)'], width, 

    ax3 = plt.subplot(1, 3, 3)                    label='P95', color='#f39c12', alpha=0.8, edgecolor='black', linewidth=1.5)

    efficiency = [t/l for t, l in zip(throughputs, latencies)]    bars2 = ax2.bar(x + width/2, taat_df['P99 Latency (ms)'], width, 

    bars = ax3.bar([c[1] for c in configs], efficiency,                    label='P99', color='#e67e22', alpha=0.8, edgecolor='black', linewidth=1.5)

                   color=['#1abc9c', '#f39c12', '#c0392b'],    

                   alpha=0.8, edgecolor='white', linewidth=2)    ax2.set_xlabel('Index Type', fontsize=14, fontweight='bold')

    ax3.set_ylabel('Efficiency (QPS/ms)', fontweight='bold')    ax2.set_ylabel('Latency (ms)', fontsize=14, fontweight='bold')

    ax3.set_xlabel('Index Type', fontweight='bold')    ax2.set_title('Tail Latencies (P95 & P99) - TAAT Mode', fontsize=16, fontweight='bold', pad=15)

    ax3.set_title('Query Efficiency', fontsize=14, fontweight='bold', pad=20)    ax2.set_xticks(x)

    ax3.grid(True, alpha=0.3, axis='y')    ax2.set_xticklabels(taat_df.index, fontsize=12)

        ax2.legend(fontsize=12, frameon=True, shadow=True)

    for bar in bars:    ax2.grid(axis='y', alpha=0.3, linestyle='--')

        height = bar.get_height()    

        ax3.text(bar.get_x() + bar.get_width()/2., height,    # Add value labels

                f'{height:.1f}', ha='center', va='bottom',    for bars in [bars1, bars2]:

                fontsize=10, fontweight='bold')        for bar in bars:

                height = bar.get_height()

    plt.suptitle('🔍 Index Type Comparison: Boolean vs TF vs TF-IDF',             ax2.text(bar.get_x() + bar.get_width()/2., height,

                 fontsize=16, fontweight='bold', y=1.02)                    f'{height:.3f}',

    plt.tight_layout()                    ha='center', va='bottom', fontsize=9, fontweight='bold')

    plt.savefig(PLOTS_DIR / 'plot_C_index_types.png', dpi=300, bbox_inches='tight')    

    plt.close()    # 3. Index Size by Compression

    print(f"✅ Saved: {PLOTS_DIR / 'plot_C_index_types.png'}")    ax3 = axes[1, 0]

    compression_df = df.groupby('Compression')['Index Size (MB)'].mean().sort_values()

    

def plot_datastores():    bars = ax3.barh(range(len(compression_df)), compression_df.values, 

    """Plot A: Compare JSON (d1) vs SQLite (d2)"""                     color=['#34495e', '#1abc9c', '#e67e22'][:len(compression_df)],

    print("📊 Generating Plot A: Datastores Comparison...")                     alpha=0.8, edgecolor='black', linewidth=1.5)

        

    # Load data for i3d1c1o0_qTAAT and i3d2c1o0_qTAAT    ax3.set_yticks(range(len(compression_df)))

    configs = [    ax3.set_yticklabels(compression_df.index, fontsize=12)

        ('i3d1c1o0', 'JSON (d1)'),    ax3.set_xlabel('Index Size (MB)', fontsize=14, fontweight='bold')

        ('i3d2c1o0', 'SQLite (d2)')    ax3.set_title('💾 Index Size by Compression Method', fontsize=16, fontweight='bold', pad=15)

    ]    ax3.grid(axis='x', alpha=0.3, linestyle='--')

        

    latencies = []    # Add value labels

    throughputs = []    for i, (idx, val) in enumerate(compression_df.items()):

    memories = []        ax3.text(val, i, f' {val:.1f} MB', va='center', fontsize=11, fontweight='bold')

        

    for config, label in configs:    # 4. Throughput (calculated from latency)

        filepath = f"results/eval_SelfIndex_{config}_qTAAT.json"    ax4 = axes[1, 1]

        data = load_evaluation_file(filepath)    

        latencies.append(data['artifact_A']['latency_p95_ms'])    # Calculate throughput (queries per second)

        throughputs.append(data['artifact_B']['throughput_qps'])    throughput_data = []

        memories.append(data['artifact_C']['memory_mb'])    for _, row in df.iterrows():

            qps = 1000 / row['Avg Latency (ms)'] if row['Avg Latency (ms)'] > 0 else 0

    # Create figure        throughput_data.append({

    fig = plt.figure(figsize=(16, 5))            'Type': row['Type'],

                'Mode': row['Mode'],

    # Subplot 1: Latency Comparison            'QPS': qps

    ax1 = plt.subplot(1, 3, 1)        })

    bars = ax1.bar([c[1] for c in configs], latencies,    

                   color=['#3498db', '#e74c3c'],    throughput_df = pd.DataFrame(throughput_data)

                   alpha=0.8, edgecolor='white', linewidth=2)    throughput_grouped = throughput_df.groupby(['Type', 'Mode'])['QPS'].mean().unstack()

    ax1.set_ylabel('Latency P95 (ms)', fontweight='bold')    

    ax1.set_xlabel('Datastore', fontweight='bold')    throughput_grouped.plot(kind='bar', ax=ax4, color=['#3498db', '#e74c3c'], 

    ax1.set_title('Query Latency', fontsize=14, fontweight='bold', pad=20)                           alpha=0.8, edgecolor='black', linewidth=1.5)

    ax1.grid(True, alpha=0.3, axis='y')    

        ax4.set_xlabel('Index Type', fontsize=14, fontweight='bold')

    for bar in bars:    ax4.set_ylabel('Throughput (Queries/Second)', fontsize=14, fontweight='bold')

        height = bar.get_height()    ax4.set_title('⚡ Query Throughput by Index Type', fontsize=16, fontweight='bold', pad=15)

        ax1.text(bar.get_x() + bar.get_width()/2., height,    ax4.set_xticklabels(ax4.get_xticklabels(), rotation=0, fontsize=12)

                f'{height:.2f} ms', ha='center', va='bottom',    ax4.legend(title='Mode', fontsize=12, frameon=True, shadow=True)

                fontsize=11, fontweight='bold')    ax4.grid(axis='y', alpha=0.3, linestyle='--')

        

    # Subplot 2: Throughput Comparison    # Add value labels

    ax2 = plt.subplot(1, 3, 2)    for container in ax4.containers:

    bars = ax2.bar([c[1] for c in configs], throughputs,        ax4.bar_label(container, fmt='%.0f', fontsize=10, fontweight='bold', padding=3)

                   color=['#27ae60', '#f39c12'],    

                   alpha=0.8, edgecolor='white', linewidth=2)    plt.tight_layout()

    ax2.set_ylabel('Throughput (QPS)', fontweight='bold')    output_path = f'{output_dir}/Dashboard_Performance_Overview.png'

    ax2.set_xlabel('Datastore', fontweight='bold')    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')

    ax2.set_title('Query Throughput', fontsize=14, fontweight='bold', pad=20)    print(f"  ✅ Saved: {output_path}")

    ax2.grid(True, alpha=0.3, axis='y')    plt.close()

    

    for bar in bars:

        height = bar.get_height()def create_latency_distribution_plot(results, output_dir='plots'):

        ax2.text(bar.get_x() + bar.get_width()/2., height,    """Create beautiful latency distribution visualization"""

                f'{height:.0f} QPS', ha='center', va='bottom',    print("\n📈 Generating Latency Distribution Plot...")

                fontsize=11, fontweight='bold')    

        fig, axes = plt.subplots(1, 2, figsize=(18, 7))

    # Subplot 3: Memory Comparison    fig.suptitle('📊 Latency Distribution Analysis', fontsize=20, fontweight='bold', y=0.98)

    ax3 = plt.subplot(1, 3, 3)    

    bars = ax3.bar([c[1] for c in configs], memories,    # Prepare data for TAAT

                   color=['#9b59b6', '#e67e22'],    taat_data = defaultdict(lambda: {'avg': [], 'p95': [], 'p99': []})

                   alpha=0.8, edgecolor='white', linewidth=2)    for r in results:

    ax3.set_ylabel('Memory (MB)', fontweight='bold')        if 'taat' in r:

    ax3.set_xlabel('Datastore', fontweight='bold')            key = str(r['index_type'])  # Normalize to string

    ax3.set_title('Memory Footprint', fontsize=14, fontweight='bold', pad=20)            taat_data[key]['avg'].append(r['taat'].get('avg_time_ms', 0))

    ax3.grid(True, alpha=0.3, axis='y')            taat_data[key]['p95'].append(r['taat'].get('p95_time_ms', 0))

                taat_data[key]['p99'].append(r['taat'].get('p99_time_ms', 0))

    for bar in bars:    

        height = bar.get_height()    # TAAT plot

        ax3.text(bar.get_x() + bar.get_width()/2., height,    ax1 = axes[0]

                f'{height:.1f} MB', ha='center', va='bottom',    x_pos = np.arange(len(taat_data))

                fontsize=11, fontweight='bold')    width = 0.25

        

    plt.suptitle('💾 Datastore Comparison: Custom JSON vs SQLite',     types = sorted(taat_data.keys())

                 fontsize=16, fontweight='bold', y=1.02)    avg_vals = [np.mean(taat_data[t]['avg']) for t in types]

    plt.tight_layout()    p95_vals = [np.mean(taat_data[t]['p95']) for t in types]

    plt.savefig(PLOTS_DIR / 'plot_A_datastores.png', dpi=300, bbox_inches='tight')    p99_vals = [np.mean(taat_data[t]['p99']) for t in types]

    plt.close()    

    print(f"✅ Saved: {PLOTS_DIR / 'plot_A_datastores.png'}")    bars1 = ax1.bar(x_pos - width, avg_vals, width, label='Average', 

                    color='#3498db', alpha=0.8, edgecolor='black', linewidth=1.5)

    bars2 = ax1.bar(x_pos, p95_vals, width, label='P95', 

def plot_compression():                    color='#f39c12', alpha=0.8, edgecolor='black', linewidth=1.5)

    """Plot AB: Compare None (c1) vs Elias (c2) vs Zlib (c3)"""    bars3 = ax1.bar(x_pos + width, p99_vals, width, label='P99', 

    print("📊 Generating Plot AB: Compression Comparison...")                    color='#e74c3c', alpha=0.8, edgecolor='black', linewidth=1.5)

        

    # Load data    ax1.set_xlabel('Index Type', fontsize=14, fontweight='bold')

    configs = [    ax1.set_ylabel('Latency (ms)', fontsize=14, fontweight='bold')

        ('i3d1c1o0', 'No Compression'),    ax1.set_title('TAAT Mode Latency Metrics', fontsize=16, fontweight='bold', pad=15)

        ('i3d1c2o0', 'Elias-Fano'),    ax1.set_xticks(x_pos)

        ('i3d1c3o0', 'Zlib')    ax1.set_xticklabels(types, fontsize=12)

    ]    ax1.legend(fontsize=12, loc='upper left', frameon=True, shadow=True)

        ax1.grid(axis='y', alpha=0.3, linestyle='--')

    latencies = []    

    throughputs = []    # Add value labels

    index_sizes = []    for bars in [bars1, bars2, bars3]:

            for bar in bars:

    for config, label in configs:            height = bar.get_height()

        filepath = f"results/eval_SelfIndex_{config}_qTAAT.json"            if height > 0.001:  # Only show if meaningful

        data = load_evaluation_file(filepath)                ax1.text(bar.get_x() + bar.get_width()/2., height,

        latencies.append(data['artifact_A']['latency_p95_ms'])                        f'{height:.3f}',

        throughputs.append(data['artifact_B']['throughput_qps'])                        ha='center', va='bottom', fontsize=9, fontweight='bold')

        index_sizes.append(data['artifact_C']['index_size_mb'])    

        # DAAT plot

    # Create figure    daat_data = defaultdict(lambda: {'avg': [], 'p95': [], 'p99': []})

    fig = plt.figure(figsize=(16, 10))    for r in results:

            if 'daat' in r:

    # Subplot 1: Performance Impact            key = str(r['index_type'])  # Normalize to string

    ax1 = plt.subplot(2, 2, 1)            daat_data[key]['avg'].append(r['daat'].get('avg_time_ms', 0))

    create_dual_axis_plot(ax1, None, latencies, throughputs,            daat_data[key]['p95'].append(r['daat'].get('p95_time_ms', 0))

                          [c[1] for c in configs],            daat_data[key]['p99'].append(r['daat'].get('p99_time_ms', 0))

                          'Latency P95 (ms)', 'Throughput (QPS)',    

                          'Performance Impact',    ax2 = axes[1]

                          ['#e74c3c', '#27ae60'])    types_d = sorted(daat_data.keys())

        avg_vals_d = [np.mean(daat_data[t]['avg']) for t in types_d]

    # Subplot 2: Index Size    p95_vals_d = [np.mean(daat_data[t]['p95']) for t in types_d]

    ax2 = plt.subplot(2, 2, 2)    p99_vals_d = [np.mean(daat_data[t]['p99']) for t in types_d]

    bars = ax2.bar([c[1] for c in configs], index_sizes,    

                   color=['#3498db', '#9b59b6', '#e67e22'],    x_pos_d = np.arange(len(types_d))

                   alpha=0.8, edgecolor='white', linewidth=2)    bars1 = ax2.bar(x_pos_d - width, avg_vals_d, width, label='Average', 

    ax2.set_ylabel('Index Size (MB)', fontweight='bold')                    color='#3498db', alpha=0.8, edgecolor='black', linewidth=1.5)

    ax2.set_xlabel('Compression Method', fontweight='bold')    bars2 = ax2.bar(x_pos_d, p95_vals_d, width, label='P95', 

    ax2.set_title('Disk Space Usage', fontsize=14, fontweight='bold', pad=20)                    color='#f39c12', alpha=0.8, edgecolor='black', linewidth=1.5)

    ax2.grid(True, alpha=0.3, axis='y')    bars3 = ax2.bar(x_pos_d + width, p99_vals_d, width, label='P99', 

                        color='#e74c3c', alpha=0.8, edgecolor='black', linewidth=1.5)

    for i, bar in enumerate(bars):    

        height = bar.get_height()    ax2.set_xlabel('Index Type', fontsize=14, fontweight='bold')

        if i == 0:    ax2.set_ylabel('Latency (ms)', fontsize=14, fontweight='bold')

            label_text = f'{height:.1f} MB\n(Baseline)'    ax2.set_title('DAAT Mode Latency Metrics', fontsize=16, fontweight='bold', pad=15)

        else:    ax2.set_xticks(x_pos_d)

            savings = (1 - height/index_sizes[0]) * 100    ax2.set_xticklabels(types_d, fontsize=12)

            label_text = f'{height:.1f} MB\n({savings:.1f}% saved)'    ax2.legend(fontsize=12, loc='upper left', frameon=True, shadow=True)

        ax2.text(bar.get_x() + bar.get_width()/2., height,    ax2.grid(axis='y', alpha=0.3, linestyle='--')

                label_text, ha='center', va='bottom',    

                fontsize=9, fontweight='bold')    # Add value labels

        for bars in [bars1, bars2, bars3]:

    # Subplot 3: Compression Ratio        for bar in bars:

    ax3 = plt.subplot(2, 2, 3)            height = bar.get_height()

    ratios = [index_sizes[0] / size for size in index_sizes]            if height > 0.001:

    bars = ax3.bar([c[1] for c in configs], ratios,                ax2.text(bar.get_x() + bar.get_width()/2., height,

                   color=['#95a5a6', '#1abc9c', '#f39c12'],                        f'{height:.3f}',

                   alpha=0.8, edgecolor='white', linewidth=2)                        ha='center', va='bottom', fontsize=9, fontweight='bold')

    ax3.set_ylabel('Compression Ratio (×)', fontweight='bold')    

    ax3.set_xlabel('Compression Method', fontweight='bold')    plt.tight_layout()

    ax3.set_title('Compression Effectiveness', fontsize=14, fontweight='bold', pad=20)    output_path = f'{output_dir}/Latency_Distribution_Analysis.png'

    ax3.axhline(y=1, color='red', linestyle='--', alpha=0.5, label='No compression')    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')

    ax3.grid(True, alpha=0.3, axis='y')    print(f"  ✅ Saved: {output_path}")

    ax3.legend()    plt.close()

    

    for bar in bars:

        height = bar.get_height()def create_compression_analysis(results, output_dir='plots'):

        ax3.text(bar.get_x() + bar.get_width()/2., height,    """Create beautiful compression impact analysis"""

                f'{height:.2f}×', ha='center', va='bottom',    print("\n🗜️ Generating Compression Analysis...")

                fontsize=10, fontweight='bold')    

        fig, axes = plt.subplots(2, 2, figsize=(18, 14))

    # Subplot 4: Trade-off Analysis    fig.suptitle('🗜️ Compression Method Analysis', fontsize=20, fontweight='bold', y=0.995)

    ax4 = plt.subplot(2, 2, 4)    

    space_saved = [(1 - size/index_sizes[0]) * 100 for size in index_sizes]    # Prepare data

    colors_map = ['#95a5a6', '#1abc9c', '#f39c12']    compression_data = defaultdict(lambda: {

            'sizes': [], 'latencies': [], 'types': []

    for i, (lat, space, label) in enumerate(zip(latencies, space_saved, [c[1] for c in configs])):    })

        ax4.scatter(space, lat, s=500, alpha=0.7,     

                   color=colors_map[i], edgecolor='white', linewidth=2,    # Compression mapping for numeric codes

                   label=label)    compression_map = {1: 'None', 2: 'VByte', 3: 'Zlib'}

        ax4.text(space, lat, label.split('(')[0].strip() if '(' in label else label,     

                ha='center', va='center', fontsize=9, fontweight='bold')    for r in results:

            comp = r['compression']

    ax4.set_xlabel('Space Saved (%)', fontweight='bold')        # Convert numeric compression codes to names

    ax4.set_ylabel('Latency P95 (ms)', fontweight='bold')        if isinstance(comp, int):

    ax4.set_title('Speed vs Space Trade-off', fontsize=14, fontweight='bold', pad=20)            comp = compression_map.get(comp, f'Unknown({comp})')

    ax4.grid(True, alpha=0.3)        comp = str(comp)  # Normalize to string

    ax4.legend(loc='upper right', framealpha=0.95)        

            compression_data[comp]['sizes'].append(r.get('index_size_mb', 0))

    plt.suptitle('🗜️ Compression Methods: Performance vs Space',         compression_data[comp]['latencies'].append(r['taat'].get('avg_time_ms', 0) if 'taat' in r else 0)

                 fontsize=16, fontweight='bold', y=0.995)        compression_data[comp]['types'].append(str(r['index_type']))

    plt.tight_layout()    

    plt.savefig(PLOTS_DIR / 'plot_AB_compression.png', dpi=300, bbox_inches='tight')    # 1. Index Size Comparison

    plt.close()    ax1 = axes[0, 0]

    print(f"✅ Saved: {PLOTS_DIR / 'plot_AB_compression.png'}")    compressions = sorted(compression_data.keys())

    avg_sizes = [np.mean(compression_data[c]['sizes']) for c in compressions]

    

def plot_optimizations():    colors_comp = [COLORS.get(c, '#95a5a6') for c in compressions]

    """Plot A: Compare No Optimization (o0) vs Skip Pointers (osp)"""    bars = ax1.bar(compressions, avg_sizes, color=colors_comp, alpha=0.8, 

    print("📊 Generating Plot A: Optimizations Comparison...")                   edgecolor='black', linewidth=2)

        

    configs = [    ax1.set_ylabel('Average Index Size (MB)', fontsize=13, fontweight='bold')

        ('i3d1c1o0', 'No Optimization'),    ax1.set_xlabel('Compression Method', fontsize=13, fontweight='bold')

        ('i3d1c1osp', 'Skip Pointers')    ax1.set_title('💾 Average Index Size by Compression', fontsize=15, fontweight='bold', pad=15)

    ]    ax1.grid(axis='y', alpha=0.3, linestyle='--')

        

    latencies = []    # Add value labels and compression ratio

    throughputs = []    baseline_size = avg_sizes[compressions.index('None')] if 'None' in compressions else avg_sizes[0]

        for i, (bar, size) in enumerate(zip(bars, avg_sizes)):

    for config, label in configs:        height = bar.get_height()

        filepath = f"results/eval_SelfIndex_{config}_qTAAT.json"        ratio = baseline_size / size if size > 0 else 1

        data = load_evaluation_file(filepath)        ax1.text(bar.get_x() + bar.get_width()/2., height,

        latencies.append(data['artifact_A']['latency_p95_ms'])                f'{size:.1f} MB\n({ratio:.2f}x)',

        throughputs.append(data['artifact_B']['throughput_qps'])                ha='center', va='bottom', fontsize=11, fontweight='bold')

        

    lat_improvement = ((latencies[0] - latencies[1]) / latencies[0]) * 100    # 2. Query Latency by Compression

    thr_improvement = ((throughputs[1] - throughputs[0]) / throughputs[0]) * 100    ax2 = axes[0, 1]

        avg_latencies = [np.mean(compression_data[c]['latencies']) for c in compressions]

    # Create figure    

    fig = plt.figure(figsize=(16, 5))    bars = ax2.bar(compressions, avg_latencies, color=colors_comp, alpha=0.8,

                       edgecolor='black', linewidth=2)

    # Subplot 1: Latency    

    ax1 = plt.subplot(1, 3, 1)    ax2.set_ylabel('Average Latency (ms)', fontsize=13, fontweight='bold')

    bars = ax1.bar([c[1] for c in configs], latencies,    ax2.set_xlabel('Compression Method', fontsize=13, fontweight='bold')

                   color=['#95a5a6', '#27ae60'],    ax2.set_title('⚡ Query Performance by Compression', fontsize=15, fontweight='bold', pad=15)

                   alpha=0.8, edgecolor='white', linewidth=2)    ax2.grid(axis='y', alpha=0.3, linestyle='--')

    ax1.set_ylabel('Latency P95 (ms)', fontweight='bold')    

    ax1.set_xlabel('Configuration', fontweight='bold')    # Add value labels

    ax1.set_title('Query Latency', fontsize=14, fontweight='bold', pad=20)    for bar in bars:

    ax1.grid(True, alpha=0.3, axis='y')        height = bar.get_height()

            ax2.text(bar.get_x() + bar.get_width()/2., height,

    for i, bar in enumerate(bars):                f'{height:.4f} ms',

        height = bar.get_height()                ha='center', va='bottom', fontsize=11, fontweight='bold')

        if i == 1:    

            label_text = f'{height:.2f} ms\n({lat_improvement:+.1f}%)'    # 3. Size-Speed Tradeoff

        else:    ax3 = axes[1, 0]

            label_text = f'{height:.2f} ms\n(Baseline)'    

        ax1.text(bar.get_x() + bar.get_width()/2., height,    for comp in compressions:

                label_text, ha='center', va='bottom',        sizes = compression_data[comp]['sizes']

                fontsize=10, fontweight='bold')        latencies = compression_data[comp]['latencies']

            color = COLORS.get(comp, '#95a5a6')

    # Subplot 2: Throughput        ax3.scatter(sizes, latencies, s=150, alpha=0.7, color=color, 

    ax2 = plt.subplot(1, 3, 2)                   label=comp, edgecolors='black', linewidth=1.5)

    bars = ax2.bar([c[1] for c in configs], throughputs,        

                   color=['#95a5a6', '#3498db'],        # Add trend line

                   alpha=0.8, edgecolor='white', linewidth=2)        if len(sizes) > 1:

    ax2.set_ylabel('Throughput (QPS)', fontweight='bold')            z = np.polyfit(sizes, latencies, 1)

    ax2.set_xlabel('Configuration', fontweight='bold')            p = np.poly1d(z)

    ax2.set_title('Query Throughput', fontsize=14, fontweight='bold', pad=20)            x_trend = np.linspace(min(sizes), max(sizes), 100)

    ax2.grid(True, alpha=0.3, axis='y')            ax3.plot(x_trend, p(x_trend), "--", alpha=0.5, color=color, linewidth=2)

        

    for i, bar in enumerate(bars):    ax3.set_xlabel('Index Size (MB)', fontsize=13, fontweight='bold')

        height = bar.get_height()    ax3.set_ylabel('Query Latency (ms)', fontsize=13, fontweight='bold')

        if i == 1:    ax3.set_title('📊 Size-Speed Tradeoff Analysis', fontsize=15, fontweight='bold', pad=15)

            label_text = f'{height:.0f} QPS\n({thr_improvement:+.1f}%)'    ax3.legend(fontsize=11, frameon=True, shadow=True)

        else:    ax3.grid(True, alpha=0.3, linestyle='--')

            label_text = f'{height:.0f} QPS\n(Baseline)'    

        ax2.text(bar.get_x() + bar.get_width()/2., height,    # 4. Compression Efficiency Table

                label_text, ha='center', va='bottom',    ax4 = axes[1, 1]

                fontsize=10, fontweight='bold')    ax4.axis('tight')

        ax4.axis('off')

    # Subplot 3: Performance Summary    

    ax3 = plt.subplot(1, 3, 3)    # Create table data

    metrics = ['Latency\nReduction', 'Throughput\nIncrease']    table_data = []

    improvements = [lat_improvement, thr_improvement]    for comp in compressions:

    colors = ['#27ae60' if x > 0 else '#e74c3c' for x in improvements]        avg_size = np.mean(compression_data[comp]['sizes'])

            avg_lat = np.mean(compression_data[comp]['latencies'])

    bars = ax3.barh(metrics, improvements, color=colors,        ratio = baseline_size / avg_size if avg_size > 0 else 1

                    alpha=0.8, edgecolor='white', linewidth=2)        space_saved = ((baseline_size - avg_size) / baseline_size * 100) if baseline_size > 0 else 0

    ax3.set_xlabel('Improvement (%)', fontweight='bold')        

    ax3.set_title('Performance Gain', fontsize=14, fontweight='bold', pad=20)        table_data.append([

    ax3.axvline(x=0, color='black', linestyle='-', linewidth=1)            comp,

    ax3.grid(True, alpha=0.3, axis='x')            f'{avg_size:.1f}',

                f'{ratio:.2f}x',

    for i, (bar, val) in enumerate(zip(bars, improvements)):            f'{space_saved:.1f}%',

        width = bar.get_width()            f'{avg_lat:.4f}'

        ax3.text(width, bar.get_y() + bar.get_height()/2.,        ])

                f' {val:+.1f}%', ha='left' if width > 0 else 'right',    

                va='center', fontsize=12, fontweight='bold')    table = ax4.table(cellText=table_data,

                         colLabels=['Compression', 'Size (MB)', 'Ratio', 'Space Saved', 'Latency (ms)'],

    plt.suptitle('⚡ Query Optimization: Impact of Skip Pointers',                      cellLoc='center',

                 fontsize=16, fontweight='bold', y=1.02)                     loc='center',

    plt.tight_layout()                     colWidths=[0.2, 0.2, 0.2, 0.2, 0.2])

    plt.savefig(PLOTS_DIR / 'plot_A_optimizations.png', dpi=300, bbox_inches='tight')    

    plt.close()    table.auto_set_font_size(False)

    print(f"✅ Saved: {PLOTS_DIR / 'plot_A_optimizations.png'}")    table.set_fontsize(12)

    table.scale(1, 2.5)

    

def plot_query_modes():    # Style header

    """Plot AC: Compare TAAT vs DAAT"""    for i in range(5):

    print("📊 Generating Plot AC: Query Processing Modes...")        table[(0, i)].set_facecolor('#34495e')

            table[(0, i)].set_text_props(weight='bold', color='white')

    configs = [    

        ('i3d1c1o0_qTAAT', 'TAAT'),    # Style rows with alternating colors

        ('i3d1c1o0_qDAAT', 'DAAT')    for i in range(1, len(table_data) + 1):

    ]        color = '#ecf0f1' if i % 2 == 0 else 'white'

            for j in range(5):

    latencies = []            table[(i, j)].set_facecolor(color)

    throughputs = []            table[(i, j)].set_text_props(weight='bold')

    latency_p99 = []    

        ax4.set_title('📋 Compression Efficiency Summary', fontsize=15, fontweight='bold', pad=20)

    for config, label in configs:    

        filepath = f"results/eval_SelfIndex_{config}.json"    plt.tight_layout()

        data = load_evaluation_file(filepath)    output_path = f'{output_dir}/Compression_Analysis.png'

        latencies.append(data['artifact_A']['latency_p95_ms'])    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')

        latency_p99.append(data['artifact_A']['latency_p99_ms'])    print(f"  ✅ Saved: {output_path}")

        throughputs.append(data['artifact_B']['throughput_qps'])    plt.close()

    

    # Create figure

    fig = plt.figure(figsize=(16, 5))def create_datastore_comparison(results, output_dir='plots'):

        """Create beautiful datastore comparison"""

    # Subplot 1: Latency (P95 and P99)    print("\n💾 Generating Datastore Comparison...")

    ax1 = plt.subplot(1, 3, 1)    

    x = np.arange(len(configs))    fig, axes = plt.subplots(1, 2, figsize=(18, 7))

    width = 0.35    fig.suptitle('💾 Datastore Performance Comparison (JSON vs SQLite)', 

                     fontsize=20, fontweight='bold', y=0.98)

    bars1 = ax1.bar(x - width/2, latencies, width, label='P95',    

                    color='#3498db', alpha=0.8, edgecolor='white', linewidth=2)    # Prepare data

    bars2 = ax1.bar(x + width/2, latency_p99, width, label='P99',    datastore_data = defaultdict(lambda: {'latencies': [], 'sizes': []})

                    color='#e74c3c', alpha=0.8, edgecolor='white', linewidth=2)    for r in results:

            ds = str(r['datastore'])  # Normalize to string

    ax1.set_ylabel('Latency (ms)', fontweight='bold')        datastore_data[ds]['latencies'].append(r['taat'].get('avg_time_ms', 0) if 'taat' in r else 0)

    ax1.set_xlabel('Query Processing Mode', fontweight='bold')        datastore_data[ds]['sizes'].append(r.get('index_size_mb', 0))

    ax1.set_title('Latency Distribution', fontsize=14, fontweight='bold', pad=20)    

    ax1.set_xticks(x)    # Plot 1: Latency Comparison

    ax1.set_xticklabels([c[1] for c in configs], fontweight='bold')    ax1 = axes[0]

    ax1.legend(framealpha=0.95, edgecolor='gray')    datastores = sorted(datastore_data.keys())

    ax1.grid(True, alpha=0.3, axis='y')    avg_latencies = [np.mean(datastore_data[ds]['latencies']) for ds in datastores]

        

    for bars in [bars1, bars2]:    colors_ds = [COLORS.get(ds, '#95a5a6') for ds in datastores]

        for bar in bars:    bars = ax1.bar(datastores, avg_latencies, color=colors_ds, alpha=0.8,

            height = bar.get_height()                   edgecolor='black', linewidth=2, width=0.5)

            ax1.text(bar.get_x() + bar.get_width()/2., height,    

                    f'{height:.2f}', ha='center', va='bottom',    ax1.set_ylabel('Average Query Latency (ms)', fontsize=14, fontweight='bold')

                    fontsize=9, fontweight='bold')    ax1.set_xlabel('Datastore Type', fontsize=14, fontweight='bold')

        ax1.set_title('⚡ Query Performance Comparison', fontsize=16, fontweight='bold', pad=15)

    # Subplot 2: Throughput    ax1.grid(axis='y', alpha=0.3, linestyle='--')

    ax2 = plt.subplot(1, 3, 2)    

    bars = ax2.bar([c[1] for c in configs], throughputs,    # Add value labels and speedup

                   color=['#27ae60', '#f39c12'],    for i, (bar, lat) in enumerate(zip(bars, avg_latencies)):

                   alpha=0.8, edgecolor='white', linewidth=2)        height = bar.get_height()

    ax2.set_ylabel('Throughput (QPS)', fontweight='bold')        speedup = max(avg_latencies) / lat if lat > 0 else 1

    ax2.set_xlabel('Query Processing Mode', fontweight='bold')        label = f'{lat:.4f} ms'

    ax2.set_title('Query Throughput', fontsize=14, fontweight='bold', pad=20)        if speedup > 1.1:

    ax2.grid(True, alpha=0.3, axis='y')            label += f'\n({speedup:.2f}x faster)'

            ax1.text(bar.get_x() + bar.get_width()/2., height,

    for bar in bars:                label, ha='center', va='bottom', fontsize=12, fontweight='bold')

        height = bar.get_height()    

        ax2.text(bar.get_x() + bar.get_width()/2., height,    # Plot 2: Index Size Comparison

                f'{height:.0f} QPS', ha='center', va='bottom',    ax2 = axes[1]

                fontsize=10, fontweight='bold')    avg_sizes = [np.mean(datastore_data[ds]['sizes']) for ds in datastores]

        

    # Subplot 3: Efficiency    bars = ax2.bar(datastores, avg_sizes, color=colors_ds, alpha=0.8,

    ax3 = plt.subplot(1, 3, 3)                   edgecolor='black', linewidth=2, width=0.5)

    efficiency = [t/l for t, l in zip(throughputs, latencies)]    

    bars = ax3.bar([c[1] for c in configs], efficiency,    ax2.set_ylabel('Average Index Size (MB)', fontsize=14, fontweight='bold')

                   color=['#9b59b6', '#1abc9c'],    ax2.set_xlabel('Datastore Type', fontsize=14, fontweight='bold')

                   alpha=0.8, edgecolor='white', linewidth=2)    ax2.set_title('💾 Storage Space Comparison', fontsize=16, fontweight='bold', pad=15)

    ax3.set_ylabel('Efficiency (QPS/ms)', fontweight='bold')    ax2.grid(axis='y', alpha=0.3, linestyle='--')

    ax3.set_xlabel('Query Processing Mode', fontweight='bold')    

    ax3.set_title('Processing Efficiency', fontsize=14, fontweight='bold', pad=20)    # Add value labels

    ax3.grid(True, alpha=0.3, axis='y')    for bar, size in zip(bars, avg_sizes):

            height = bar.get_height()

    for bar in bars:        ax2.text(bar.get_x() + bar.get_width()/2., height,

        height = bar.get_height()                f'{size:.1f} MB',

        ax3.text(bar.get_x() + bar.get_width()/2., height,                ha='center', va='bottom', fontsize=12, fontweight='bold')

                f'{height:.1f}', ha='center', va='bottom',    

                fontsize=10, fontweight='bold')    plt.tight_layout()

        output_path = f'{output_dir}/Datastore_Comparison.png'

    plt.suptitle('🔄 Query Processing: TAAT vs DAAT Algorithms',     plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')

                 fontsize=16, fontweight='bold', y=1.02)    print(f"  ✅ Saved: {output_path}")

    plt.tight_layout()    plt.close()

    plt.savefig(PLOTS_DIR / 'plot_AC_query_modes.png', dpi=300, bbox_inches='tight')

    plt.close()

    print(f"✅ Saved: {PLOTS_DIR / 'plot_AC_query_modes.png'}")def create_taat_vs_daat(results, output_dir='plots'):

    """Create beautiful TAAT vs DAAT comparison"""

    print("\n🔄 Generating TAAT vs DAAT Comparison...")

def plot_elasticsearch_comparison():    

    """ES Comparison: SelfIndex vs Elasticsearch (COLD and WARM)"""    fig, axes = plt.subplots(1, 2, figsize=(18, 7))

    print("📊 Generating ES Comparison Plot...")    fig.suptitle('🔄 Query Processing: TAAT vs DAAT Performance', 

                     fontsize=20, fontweight='bold', y=0.98)

    # Load data    

    selfindex_data = load_evaluation_file("results/eval_SelfIndex_i3d1c1o0_qDAAT.json")    # Prepare data

    es_cold_data = load_evaluation_file("results/eval_esindex-v1.0_COLD.json")    comparison_data = defaultdict(lambda: {'taat': [], 'daat': [], 'speedup': []})

    es_warm_data = load_evaluation_file("results/eval_esindex-v1.0_WARM.json")    

        for r in results:

    systems = ['SelfIndex\n(TF-IDF)', 'Elasticsearch\n(COLD)', 'Elasticsearch\n(WARM)']        if 'taat' in r and 'daat' in r:

    latencies = [            idx_type = str(r['index_type'])  # Normalize to string

        selfindex_data['artifact_A']['latency_p95_ms'],            taat_time = r['taat'].get('avg_time_ms', 0)

        es_cold_data['artifact_A']['latency_p95_ms'],            daat_time = r['daat'].get('avg_time_ms', 0)

        es_warm_data['artifact_A']['latency_p95_ms']            speedup = taat_time / daat_time if daat_time > 0 else 1

    ]            

    throughputs = [            comparison_data[idx_type]['taat'].append(taat_time)

        selfindex_data['artifact_B']['throughput_qps'],            comparison_data[idx_type]['daat'].append(daat_time)

        es_cold_data['artifact_B']['throughput_qps'],            comparison_data[idx_type]['speedup'].append(speedup)

        es_warm_data['artifact_B']['throughput_qps']    

    ]    # Plot 1: Latency Comparison

    index_sizes = [    ax1 = axes[0]

        selfindex_data['artifact_C']['index_size_mb'],    types = sorted(comparison_data.keys())

        es_cold_data['artifact_C']['index_size_mb'],    taat_times = [np.mean(comparison_data[t]['taat']) for t in types]

        es_warm_data['artifact_C']['index_size_mb']    daat_times = [np.mean(comparison_data[t]['daat']) for t in types]

    ]    

        x = np.arange(len(types))

    # Create figure    width = 0.35

    fig = plt.figure(figsize=(18, 10))    

        bars1 = ax1.bar(x - width/2, taat_times, width, label='TAAT',

    # Subplot 1: Latency                    color='#3498db', alpha=0.8, edgecolor='black', linewidth=1.5)

    ax1 = plt.subplot(2, 3, 1)    bars2 = ax1.bar(x + width/2, daat_times, width, label='DAAT',

    bars = ax1.bar(systems, latencies,                    color='#e74c3c', alpha=0.8, edgecolor='black', linewidth=1.5)

                   color=['#2ecc71', '#e74c3c', '#f39c12'],    

                   alpha=0.8, edgecolor='white', linewidth=2)    ax1.set_xlabel('Index Type', fontsize=14, fontweight='bold')

    ax1.set_ylabel('Latency P95 (ms)', fontweight='bold')    ax1.set_ylabel('Average Query Latency (ms)', fontsize=14, fontweight='bold')

    ax1.set_title('Query Latency', fontsize=13, fontweight='bold', pad=15)    ax1.set_title('Query Processing Time Comparison', fontsize=16, fontweight='bold', pad=15)

    ax1.grid(True, alpha=0.3, axis='y')    ax1.set_xticks(x)

        ax1.set_xticklabels(types, fontsize=12)

    for i, bar in enumerate(bars):    ax1.legend(fontsize=13, frameon=True, shadow=True, loc='upper left')

        height = bar.get_height()    ax1.grid(axis='y', alpha=0.3, linestyle='--')

        if i == 0:    

            label_text = f'{height:.2f} ms'    # Add value labels

        else:    for bars in [bars1, bars2]:

            speedup = latencies[i] / latencies[0]        for bar in bars:

            label_text = f'{height:.2f} ms\n({speedup:.1f}× slower)'            height = bar.get_height()

        ax1.text(bar.get_x() + bar.get_width()/2., height,            ax1.text(bar.get_x() + bar.get_width()/2., height,

                label_text, ha='center', va='bottom',                    f'{height:.4f}',

                fontsize=9, fontweight='bold')                    ha='center', va='bottom', fontsize=10, fontweight='bold')

        

    # Subplot 2: Throughput    # Plot 2: Speedup Factor

    ax2 = plt.subplot(2, 3, 2)    ax2 = axes[1]

    bars = ax2.bar(systems, throughputs,    avg_speedups = [np.mean(comparison_data[t]['speedup']) for t in types]

                   color=['#2ecc71', '#3498db', '#9b59b6'],    

                   alpha=0.8, edgecolor='white', linewidth=2)    colors_speedup = ['#2ecc71' if s > 1 else '#e74c3c' for s in avg_speedups]

    ax2.set_ylabel('Throughput (QPS)', fontweight='bold')    bars = ax2.bar(types, avg_speedups, color=colors_speedup, alpha=0.8,

    ax2.set_title('Query Throughput', fontsize=13, fontweight='bold', pad=15)                   edgecolor='black', linewidth=2)

    ax2.grid(True, alpha=0.3, axis='y')    

        ax2.axhline(y=1, color='#34495e', linestyle='--', linewidth=2, label='No speedup')

    for i, bar in enumerate(bars):    ax2.set_ylabel('Speedup Factor (DAAT vs TAAT)', fontsize=14, fontweight='bold')

        height = bar.get_height()    ax2.set_xlabel('Index Type', fontsize=14, fontweight='bold')

        if i == 0:    ax2.set_title('⚡ DAAT Speedup Over TAAT', fontsize=16, fontweight='bold', pad=15)

            label_text = f'{height:.0f} QPS'    ax2.legend(fontsize=12, frameon=True, shadow=True)

        else:    ax2.grid(axis='y', alpha=0.3, linestyle='--')

            ratio = throughputs[i] / throughputs[0]    

            label_text = f'{height:.0f} QPS\n({ratio:.2f}×)'    # Add value labels

        ax2.text(bar.get_x() + bar.get_width()/2., height,    for bar, speedup in zip(bars, avg_speedups):

                label_text, ha='center', va='bottom',        height = bar.get_height()

                fontsize=9, fontweight='bold')        label = f'{speedup:.1f}x'

            if speedup > 1:

    # Subplot 3: Index Size            label += '\nfaster'

    ax3 = plt.subplot(2, 3, 3)        else:

    bars = ax3.bar(systems, index_sizes,            label += '\nslower'

                   color=['#2ecc71', '#e67e22', '#c0392b'],        ax2.text(bar.get_x() + bar.get_width()/2., height,

                   alpha=0.8, edgecolor='white', linewidth=2)                label, ha='center', va='bottom', fontsize=11, fontweight='bold')

    ax3.set_ylabel('Index Size (MB)', fontweight='bold')    

    ax3.set_title('Storage Footprint', fontsize=13, fontweight='bold', pad=15)    plt.tight_layout()

    ax3.grid(True, alpha=0.3, axis='y')    output_path = f'{output_dir}/TAAT_vs_DAAT_Comparison.png'

        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')

    for i, bar in enumerate(bars):    print(f"  ✅ Saved: {output_path}")

        height = bar.get_height()    plt.close()

        if i == 0:

            label_text = f'{height:.1f} MB'

        else:def save_metrics_csv(results, output_dir='plots'):

            ratio = height / index_sizes[0]    """Save comprehensive metrics to CSV for reference"""

            label_text = f'{height:.1f} MB\n({ratio:.1f}× larger)'    print("\n💾 Saving Metrics to CSV...")

        ax3.text(bar.get_x() + bar.get_width()/2., height,    

                label_text, ha='center', va='bottom',    rows = []

                fontsize=9, fontweight='bold')    

        # Compression mapping for numeric codes

    # Subplot 4: Performance vs Size    compression_map = {1: 'None', 2: 'VByte', 3: 'Zlib'}

    ax4 = plt.subplot(2, 3, 4)    

    colors_map = ['#2ecc71', '#e74c3c', '#f39c12']    for r in results:

    labels_short = ['SelfIndex', 'ES (COLD)', 'ES (WARM)']        # Normalize compression field

            comp = r.get('compression', 'N/A')

    for i, (size, thr, label) in enumerate(zip(index_sizes, throughputs, labels_short)):        if isinstance(comp, int):

        ax4.scatter(size, thr, s=800, alpha=0.7,            comp = compression_map.get(comp, f'Unknown({comp})')

                   color=colors_map[i], edgecolor='white', linewidth=3,        

                   label=label, zorder=3)        base = {

        ax4.text(size, thr, label, ha='center', va='center',            'identifier': r.get('identifier', 'N/A'),

                fontsize=9, fontweight='bold', color='white')            'index_type': r.get('index_type', 'N/A'),

                'datastore': r.get('datastore', 'N/A'),

    ax4.set_xlabel('Index Size (MB)', fontweight='bold')            'compression': comp,

    ax4.set_ylabel('Throughput (QPS)', fontweight='bold')            'index_size_mb': r.get('index_size_mb', 0)

    ax4.set_title('Performance vs Storage', fontsize=13, fontweight='bold', pad=15)        }

    ax4.grid(True, alpha=0.3)        

    ax4.legend(loc='upper left', framealpha=0.95, fontsize=9)        # TAAT metrics

            if 'taat' in r:

    # Subplot 5: Latency Percentiles            row = base.copy()

    ax5 = plt.subplot(2, 3, 5)            row.update({

    p50_vals = [                'mode': 'TAAT',

        selfindex_data['artifact_A']['latency_median_ms'],                'avg_latency_ms': r['taat'].get('avg_time_ms', 0),

        es_cold_data['artifact_A']['latency_median_ms'],                'median_latency_ms': r['taat'].get('median_time_ms', 0),

        es_warm_data['artifact_A']['latency_median_ms']                'p95_latency_ms': r['taat'].get('p95_time_ms', 0),

    ]                'p99_latency_ms': r['taat'].get('p99_time_ms', 0),

    p99_vals = [                'min_latency_ms': r['taat'].get('min_time_ms', 0),

        selfindex_data['artifact_A']['latency_p99_ms'],                'max_latency_ms': r['taat'].get('max_time_ms', 0),

        es_cold_data['artifact_A']['latency_p99_ms'],                'avg_memory_mb': r['taat'].get('avg_memory_mb', 0),

        es_warm_data['artifact_A']['latency_p99_ms']                'throughput_qps': 1000 / r['taat'].get('avg_time_ms', 1) if r['taat'].get('avg_time_ms', 0) > 0 else 0

    ]            })

                rows.append(row)

    x = np.arange(len(systems))        

    width = 0.25        # DAAT metrics

            if 'daat' in r:

    ax5.bar(x - width, p50_vals, width, label='P50',            row = base.copy()

            color='#27ae60', alpha=0.8, edgecolor='white', linewidth=1.5)            row.update({

    ax5.bar(x, latencies, width, label='P95',                'mode': 'DAAT',

            color='#f39c12', alpha=0.8, edgecolor='white', linewidth=1.5)                'avg_latency_ms': r['daat'].get('avg_time_ms', 0),

    ax5.bar(x + width, p99_vals, width, label='P99',                'median_latency_ms': r['daat'].get('median_time_ms', 0),

            color='#e74c3c', alpha=0.8, edgecolor='white', linewidth=1.5)                'p95_latency_ms': r['daat'].get('p95_time_ms', 0),

                    'p99_latency_ms': r['daat'].get('p99_time_ms', 0),

    ax5.set_ylabel('Latency (ms)', fontweight='bold')                'min_latency_ms': r['daat'].get('min_time_ms', 0),

    ax5.set_title('Latency Percentiles', fontsize=13, fontweight='bold', pad=15)                'max_latency_ms': r['daat'].get('max_time_ms', 0),

    ax5.set_xticks(x)                'avg_memory_mb': r['daat'].get('avg_memory_mb', 0),

    ax5.set_xticklabels([s.replace('\n', ' ') for s in systems], fontsize=9)                'throughput_qps': 1000 / r['daat'].get('avg_time_ms', 1) if r['daat'].get('avg_time_ms', 0) > 0 else 0

    ax5.legend(framealpha=0.95, fontsize=9)            })

    ax5.grid(True, alpha=0.3, axis='y')            rows.append(row)

        

    # Subplot 6: Summary Table    df = pd.DataFrame(rows)

    ax6 = plt.subplot(2, 3, 6)    output_path = f'{output_dir}/comprehensive_metrics.csv'

    ax6.axis('off')    df.to_csv(output_path, index=False)

        print(f"  ✅ Saved: {output_path}")

    summary_data = [    print(f"\n📊 Metrics Summary:")

        ['Metric', 'SelfIndex', 'ES (COLD)', 'ES (WARM)'],    print(f"  - Total Configurations: {len(results)}")

        ['Latency P95', f'{latencies[0]:.2f} ms', f'{latencies[1]:.2f} ms', f'{latencies[2]:.2f} ms'],    print(f"  - Metrics Tracked: Latency (avg, median, p95, p99), Throughput, Memory, Index Size")

        ['Throughput', f'{throughputs[0]:.0f} QPS', f'{throughputs[1]:.0f} QPS', f'{throughputs[2]:.0f} QPS'],    print(f"  - CSV Location: {output_path}")

        ['Index Size', f'{index_sizes[0]:.1f} MB', f'{index_sizes[1]:.1f} MB', f'{index_sizes[2]:.1f} MB'],

        ['Efficiency', f'{throughputs[0]/latencies[0]:.1f}', 

         f'{throughputs[1]/latencies[1]:.1f}', f'{throughputs[2]/latencies[2]:.1f}'],def main():

        ['Winner', '✅ Fastest', '❄️ Cold', '🔥 Warm']    """Generate all beautiful plots"""

    ]    # Create output directory

        output_dir = Path('plots')

    table = ax6.table(cellText=summary_data, cellLoc='center', loc='center',    output_dir.mkdir(exist_ok=True)

                     bbox=[0, 0, 1, 1])    

    table.auto_set_font_size(False)    print("\n" + "="*80)

    table.set_fontsize(10)    print("🎨 BEAUTIFUL PLOTS GENERATOR".center(80))

    table.scale(1, 2.5)    print("="*80)

        

    for i in range(4):    # Load results

        table[(0, i)].set_facecolor('#34495e')    print("\n📂 Loading evaluation results...")

        table[(0, i)].set_text_props(weight='bold', color='white')    results = load_results('results/full_evaluation.json')

        print(f"  ✅ Loaded {len(results)} configurations")

    row_colors = ['#ecf0f1', '#ffffff']    

    for i in range(1, len(summary_data)):    # Generate all plots

        for j in range(4):    create_performance_dashboard(results, output_dir)

            table[(i, j)].set_facecolor(row_colors[i % 2])    create_latency_distribution_plot(results, output_dir)

            if j == 1:    create_compression_analysis(results, output_dir)

                table[(i, j)].set_facecolor('#d5f4e6')    create_datastore_comparison(results, output_dir)

        create_taat_vs_daat(results, output_dir)

    ax6.set_title('Performance Summary', fontsize=13, fontweight='bold', pad=20)    

        # Save metrics

    plt.suptitle('🏆 SelfIndex vs Elasticsearch: Performance Comparison',     save_metrics_csv(results, output_dir)

                 fontsize=16, fontweight='bold', y=0.995)    

        print("\n" + "="*80)

    fig.text(0.5, 0.02,    print("✅ ALL PLOTS GENERATED SUCCESSFULLY!".center(80))

             'Fair comparison: top-10 results, single-field search, no document fetching\n'    print("="*80)

             'COLD = Cache cleared, WARM = Cache populated',    print(f"\n📁 Output Directory: {output_dir.absolute()}")

             ha='center', fontsize=9, style='italic', color='#555',    print("\n📊 Generated Plots:")

             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))    print("  1. Dashboard_Performance_Overview.png - Comprehensive metrics dashboard")

        print("  2. Latency_Distribution_Analysis.png - Latency percentiles (avg, p95, p99)")

    plt.tight_layout(rect=[0, 0.04, 1, 0.99])    print("  3. Compression_Analysis.png - Compression impact on size and speed")

    plt.savefig(PLOTS_DIR / 'plot_ES_comparison.png', dpi=300, bbox_inches='tight')    print("  4. Datastore_Comparison.png - JSON vs SQLite performance")

    plt.close()    print("  5. TAAT_vs_DAAT_Comparison.png - Query processing comparison")

    print(f"✅ Saved: {PLOTS_DIR / 'plot_ES_comparison.png'}")    print("  6. comprehensive_metrics.csv - All metrics in tabular format")

    print("\n✨ All plots are publication-quality (300 DPI) with clear labels!")

    print("="*80 + "\n")

def main():

    """Generate all plots"""

    print("\n" + "="*60)if __name__ == '__main__':

    print("🎨 BEAUTIFUL PLOT GENERATION - IR Assignment")    main()

    print("="*60 + "\n")
    
    plot_index_types()
    plot_datastores()
    plot_compression()
    plot_optimizations()
    plot_query_modes()
    plot_elasticsearch_comparison()
    
    print("\n" + "="*60)
    print("✅ ALL PLOTS GENERATED SUCCESSFULLY!")
    print("="*60)
    print(f"\n📁 Location: {PLOTS_DIR.absolute()}")
    print(f"📊 Total plots: 6")
    print("\nPlot files:")
    for plot_file in sorted(PLOTS_DIR.glob("*.png")):
        print(f"  • {plot_file.name}")
    print("\n🎉 Ready for your LaTeX report!")


if __name__ == "__main__":
    main()
