"""Plot Generator for IR Assignment - Generates 6 comparison plots""""""

Plot Generator for Information Retrieval Assignment

import jsonGenerates 6 comparison plots from evaluation results

import matplotlib.pyplot as plt"""

import numpy as np

from pathlib import Pathimport json

import matplotlib.pyplot as plt

plt.style.use('ggplot')import numpy as np

plt.rcParams['figure.facecolor'] = 'white'from pathlib import Path

plt.rcParams['font.size'] = 10import seaborn as sns

import warnings

PLOTS_DIR = Path("plots")warnings.filterwarnings('ignore')

PLOTS_DIR.mkdir(exist_ok=True)

# Configure matplotlib style

def load_eval(filepath):plt.style.use('seaborn-v0_8-darkgrid')

    with open(filepath) as f:sns.set_palette("husl")

        return json.load(f)plt.rcParams['figure.facecolor'] = 'white'

plt.rcParams['axes.facecolor'] = '#f8f9fa'

def plot_index_types():plt.rcParams['font.size'] = 11

    print("📊 Plot C: Index Types...")plt.rcParams['axes.labelsize'] = 12

    plt.rcParams['axes.titlesize'] = 14

    configs = [('i1d1c1o0', 'Boolean'), ('i2d1c1o0', 'TF'), ('i3d1c1o0', 'TF-IDF')]plt.rcParams['legend.fontsize'] = 10

    lat, thr, mem = [], [], []plt.rcParams['figure.dpi'] = 100

    

    for cfg, _ in configs:PLOTS_DIR = Path("plots")

        d = load_eval(f"results/eval_SelfIndex_{cfg}_qTAAT.json")PLOTS_DIR.mkdir(exist_ok=True)

        lat.append(d['artifact_A_latency']['p95_ms'])

        thr.append(d['artifact_B_throughput']['queries_per_second'])def load_evaluation_file(filepath):

        mem.append(d['artifact_C_memory']['ram_gb'] * 1024)    """Load evaluation JSON data"""

        with open(filepath, 'r') as f:

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(15, 4))        return json.load(f)

    labels = [c[1] for c in configs]

    def create_dual_axis_plot(ax, x_data, y1_data, y2_data, x_labels, 

    ax1.bar(labels, lat, color=['#e74c3c', '#3498db', '#2ecc71'], alpha=0.8)                          y1_label, y2_label, title, colors):

    ax1.set_ylabel('Latency P95 (ms)', fontweight='bold')    """Create dual-axis bar plot"""

    ax1.set_title('Query Latency')    x_pos = np.arange(len(x_labels))

    ax1.grid(alpha=0.3)    width = 0.35

        

    ax2.bar(labels, thr, color=['#e74c3c', '#3498db', '#2ecc71'], alpha=0.8)    bars1 = ax.bar(x_pos - width/2, y1_data, width, 

    ax2.set_ylabel('Throughput (QPS)', fontweight='bold')                   label=y1_label, color=colors[0], alpha=0.8, 

    ax2.set_title('Query Throughput')                   edgecolor='white', linewidth=1.5)

    ax2.grid(alpha=0.3)    ax.set_ylabel(y1_label, color=colors[0], fontweight='bold')

        ax.tick_params(axis='y', labelcolor=colors[0])

    ax3.bar(labels, mem, color=['#e74c3c', '#3498db', '#2ecc71'], alpha=0.8)    

    ax3.set_ylabel('Memory (MB)', fontweight='bold')    for bar in bars1:

    ax3.set_title('Memory Usage')        height = bar.get_height()

    ax3.grid(alpha=0.3)        ax.text(bar.get_x() + bar.get_width()/2., height,

                    f'{height:.1f}', ha='center', va='bottom', 

    plt.suptitle('Index Type Comparison: Boolean vs TF vs TF-IDF', fontsize=14, fontweight='bold')                fontsize=9, fontweight='bold')

    plt.tight_layout()    

    plt.savefig(PLOTS_DIR / 'plot_C_index_types.png', dpi=300, bbox_inches='tight')    ax2 = ax.twinx()

    plt.close()    bars2 = ax2.bar(x_pos + width/2, y2_data, width, 

    print(f"✅ Saved plot_C_index_types.png")                    label=y2_label, color=colors[1], alpha=0.8,

                    edgecolor='white', linewidth=1.5)

def plot_datastores():    ax2.set_ylabel(y2_label, color=colors[1], fontweight='bold')

    print("📊 Plot A: Datastores...")    ax2.tick_params(axis='y', labelcolor=colors[1])

        

    configs = [('i3d1c1o0', 'JSON'), ('i3d2c1o0', 'SQLite')]    for bar in bars2:

    lat, thr, mem = [], [], []        height = bar.get_height()

            ax2.text(bar.get_x() + bar.get_width()/2., height,

    for cfg, _ in configs:                 f'{height:.0f}', ha='center', va='bottom',

        d = load_eval(f"results/eval_SelfIndex_{cfg}_qTAAT.json")                 fontsize=9, fontweight='bold')

        lat.append(d['artifact_A_latency']['p95_ms'])    

        thr.append(d['artifact_B_throughput']['queries_per_second'])    ax.set_xlabel('Configuration', fontweight='bold')

        mem.append(d['artifact_C_memory']['ram_gb'] * 1024)    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)

        ax.set_xticks(x_pos)

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(15, 4))    ax.set_xticklabels(x_labels, fontweight='bold')

    labels = [c[1] for c in configs]    

        lines1, labels1 = ax.get_legend_handles_labels()

    ax1.bar(labels, lat, color=['#3498db', '#e74c3c'], alpha=0.8)    lines2, labels2 = ax2.get_legend_handles_labels()

    ax1.set_ylabel('Latency P95 (ms)', fontweight='bold')    ax.legend(lines1 + lines2, labels1 + labels2, 

    ax1.set_title('Query Latency')              loc='upper left', framealpha=0.95, edgecolor='gray')

    ax1.grid(alpha=0.3)    

        ax.grid(True, alpha=0.3, axis='y')

    ax2.bar(labels, thr, color=['#27ae60', '#f39c12'], alpha=0.8)    return ax, ax2

    ax2.set_ylabel('Throughput (QPS)', fontweight='bold')

    ax2.set_title('Query Throughput')

    ax2.grid(alpha=0.3)def plot_index_types():

        """Plot C: Compare Boolean (i1) vs TF (i2) vs TF-IDF (i3)"""

    ax3.bar(labels, mem, color=['#9b59b6', '#e67e22'], alpha=0.8)    print("📊 Generating Plot C: Index Types...")

    ax3.set_ylabel('Memory (MB)', fontweight='bold')    

    ax3.set_title('Memory Usage')    configs = [

    ax3.grid(alpha=0.3)        ('i1d1c1o0', 'Boolean (i1)'),

            ('i2d1c1o0', 'TF (i2)'),

    plt.suptitle('Datastore Comparison: JSON vs SQLite', fontsize=14, fontweight='bold')        ('i3d1c1o0', 'TF-IDF (i3)')

    plt.tight_layout()    ]

    plt.savefig(PLOTS_DIR / 'plot_A_datastores.png', dpi=300, bbox_inches='tight')    

    plt.close()    latencies = []

    print(f"✅ Saved plot_A_datastores.png")    throughputs = []

    memories = []

def plot_compression():    

    print("📊 Plot AB: Compression...")    for config, label in configs:

            filepath = f"results/eval_SelfIndex_{config}_qTAAT.json"

    configs = [('i3d1c1o0', 'None'), ('i3d1c2o0', 'Elias'), ('i3d1c3o0', 'Zlib')]        data = load_evaluation_file(filepath)

    lat, thr, size = [], [], []        latencies.append(data['artifact_A']['latency_p95_ms'])

            throughputs.append(data['artifact_B']['throughput_qps'])

    for cfg, _ in configs:        memories.append(data['artifact_C']['memory_mb'])

        d = load_eval(f"results/eval_SelfIndex_{cfg}_qTAAT.json")    

        lat.append(d['artifact_A_latency']['p95_ms'])    fig = plt.figure(figsize=(16, 5))

        thr.append(d['artifact_B_throughput']['queries_per_second'])    

        size.append(d['artifact_C_memory']['disk_mb'])    ax1 = plt.subplot(1, 3, 1)

        create_dual_axis_plot(ax1, None, latencies, throughputs, 

    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10))                          [c[1] for c in configs],

    labels = [c[1] for c in configs]                          'Latency P95 (ms)', 'Throughput (QPS)',

                              'Performance Comparison',

    ax1.bar(labels, lat, color=['#95a5a6', '#1abc9c', '#f39c12'], alpha=0.8)                          ['#e74c3c', '#27ae60'])

    ax1.set_ylabel('Latency P95 (ms)', fontweight='bold')    

    ax1.set_title('Query Latency Impact')    ax2 = plt.subplot(1, 3, 2)

    ax1.grid(alpha=0.3)    bars = ax2.bar([c[1] for c in configs], memories, 

                       color=['#3498db', '#9b59b6', '#e67e22'], 

    ax2.bar(labels, thr, color=['#95a5a6', '#1abc9c', '#f39c12'], alpha=0.8)                   alpha=0.8, edgecolor='white', linewidth=2)

    ax2.set_ylabel('Throughput (QPS)', fontweight='bold')    ax2.set_ylabel('Memory (MB)', fontweight='bold')

    ax2.set_title('Query Throughput Impact')    ax2.set_xlabel('Index Type', fontweight='bold')

    ax2.grid(alpha=0.3)    ax2.set_title('Memory Footprint', fontsize=14, fontweight='bold', pad=20)

        ax2.grid(True, alpha=0.3, axis='y')

    ax3.bar(labels, size, color=['#3498db', '#9b59b6', '#e67e22'], alpha=0.8)    

    ax3.set_ylabel('Index Size (MB)', fontweight='bold')    for bar in bars:

    ax3.set_title('Disk Space Usage')        height = bar.get_height()

    ax3.grid(alpha=0.3)        ax2.text(bar.get_x() + bar.get_width()/2., height,

    for i, (lbl, sz) in enumerate(zip(labels, size)):                f'{height:.1f} MB', ha='center', va='bottom',

        if i > 0:                fontsize=10, fontweight='bold')

            savings = (1 - sz/size[0]) * 100    

            ax3.text(i, sz, f'{savings:.1f}% saved', ha='center', va='bottom', fontweight='bold')    ax3 = plt.subplot(1, 3, 3)

        efficiency = [t/l for t, l in zip(throughputs, latencies)]

    ratios = [size[0] / s for s in size]    bars = ax3.bar([c[1] for c in configs], efficiency,

    ax4.bar(labels, ratios, color=['#95a5a6', '#1abc9c', '#f39c12'], alpha=0.8)                   color=['#1abc9c', '#f39c12', '#c0392b'],

    ax4.set_ylabel('Compression Ratio (×)', fontweight='bold')                   alpha=0.8, edgecolor='white', linewidth=2)

    ax4.set_title('Compression Effectiveness')    ax3.set_ylabel('Efficiency (QPS/ms)', fontweight='bold')

    ax4.axhline(y=1, color='red', linestyle='--', alpha=0.5)    ax3.set_xlabel('Index Type', fontweight='bold')

    ax4.grid(alpha=0.3)    ax3.set_title('Query Efficiency', fontsize=14, fontweight='bold', pad=20)

        ax3.grid(True, alpha=0.3, axis='y')

    plt.suptitle('Compression Comparison: Performance vs Space Trade-off', fontsize=14, fontweight='bold')    

    plt.tight_layout()    for bar in bars:

    plt.savefig(PLOTS_DIR / 'plot_AB_compression.png', dpi=300, bbox_inches='tight')        height = bar.get_height()

    plt.close()        ax3.text(bar.get_x() + bar.get_width()/2., height,

    print(f"✅ Saved plot_AB_compression.png")                f'{height:.1f}', ha='center', va='bottom',

                fontsize=10, fontweight='bold')

def plot_optimizations():    

    print("📊 Plot A: Optimizations...")    plt.suptitle('🔍 Index Type Comparison: Boolean vs TF vs TF-IDF', 

                     fontsize=16, fontweight='bold', y=1.02)

    configs = [('i3d1c1o0', 'No Optimization'), ('i3d1c1osp', 'Skip Pointers')]    plt.tight_layout()

    lat, thr = [], []    plt.savefig(PLOTS_DIR / 'plot_C_index_types.png', dpi=300, bbox_inches='tight')

        plt.close()

    for cfg, _ in configs:    print(f"✅ {PLOTS_DIR / 'plot_C_index_types.png'}")

        d = load_eval(f"results/eval_SelfIndex_{cfg}_qTAAT.json")

        lat.append(d['artifact_A_latency']['p95_ms'])

        thr.append(d['artifact_B_throughput']['queries_per_second'])def plot_datastores():

        """Plot A: Compare JSON (d1) vs SQLite (d2)"""

    lat_imp = ((lat[0] - lat[1]) / lat[0]) * 100    print("📊 Generating Plot A: Datastores...")

    thr_imp = ((thr[1] - thr[0]) / thr[0]) * 100    

        configs = [

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(15, 4))        ('i3d1c1o0', 'JSON (d1)'),

    labels = [c[1] for c in configs]        ('i3d2c1o0', 'SQLite (d2)')

        ]

    ax1.bar(labels, lat, color=['#95a5a6', '#27ae60'], alpha=0.8)    

    ax1.set_ylabel('Latency P95 (ms)', fontweight='bold')    latencies = []

    ax1.set_title('Query Latency')    throughputs = []

    ax1.grid(alpha=0.3)    memories = []

    ax1.text(1, lat[1], f'{lat_imp:+.1f}%', ha='center', va='bottom', fontweight='bold')    

        for config, label in configs:

    ax2.bar(labels, thr, color=['#95a5a6', '#3498db'], alpha=0.8)        filepath = f"results/eval_SelfIndex_{config}_qTAAT.json"

    ax2.set_ylabel('Throughput (QPS)', fontweight='bold')        data = load_evaluation_file(filepath)

    ax2.set_title('Query Throughput')        latencies.append(data['artifact_A']['latency_p95_ms'])

    ax2.grid(alpha=0.3)        throughputs.append(data['artifact_B']['throughput_qps'])

    ax2.text(1, thr[1], f'{thr_imp:+.1f}%', ha='center', va='bottom', fontweight='bold')        memories.append(data['artifact_C']['memory_mb'])

        

    ax3.barh(['Latency\nReduction', 'Throughput\nIncrease'], [lat_imp, thr_imp],    fig = plt.figure(figsize=(16, 5))

             color=['#27ae60' if x > 0 else '#e74c3c' for x in [lat_imp, thr_imp]], alpha=0.8)    

    ax3.set_xlabel('Improvement (%)', fontweight='bold')    ax1 = plt.subplot(1, 3, 1)

    ax3.set_title('Performance Gain Summary')    bars = ax1.bar([c[1] for c in configs], latencies,

    ax3.axvline(x=0, color='black', linestyle='-', linewidth=1)                   color=['#3498db', '#e74c3c'],

    ax3.grid(alpha=0.3)                   alpha=0.8, edgecolor='white', linewidth=2)

        ax1.set_ylabel('Latency P95 (ms)', fontweight='bold')

    plt.suptitle('Query Optimization: Skip Pointers Impact', fontsize=14, fontweight='bold')    ax1.set_xlabel('Datastore', fontweight='bold')

    plt.tight_layout()    ax1.set_title('Query Latency', fontsize=14, fontweight='bold', pad=20)

    plt.savefig(PLOTS_DIR / 'plot_A_optimizations.png', dpi=300, bbox_inches='tight')    ax1.grid(True, alpha=0.3, axis='y')

    plt.close()    

    print(f"✅ Saved plot_A_optimizations.png")    for bar in bars:

        height = bar.get_height()

def plot_query_modes():        ax1.text(bar.get_x() + bar.get_width()/2., height,

    print("📊 Plot AC: Query Modes...")                f'{height:.2f} ms', ha='center', va='bottom',

                    fontsize=11, fontweight='bold')

    configs = [('i3d1c1o0_qTAAT', 'TAAT'), ('i3d1c1o0_qDAAT', 'DAAT')]    

    lat_p95, lat_p99, thr = [], [], []    ax2 = plt.subplot(1, 3, 2)

        bars = ax2.bar([c[1] for c in configs], throughputs,

    for cfg, _ in configs:                   color=['#27ae60', '#f39c12'],

        d = load_eval(f"results/eval_SelfIndex_{cfg}.json")                   alpha=0.8, edgecolor='white', linewidth=2)

        lat_p95.append(d['artifact_A_latency']['p95_ms'])    ax2.set_ylabel('Throughput (QPS)', fontweight='bold')

        lat_p99.append(d['artifact_A_latency']['p99_ms'])    ax2.set_xlabel('Datastore', fontweight='bold')

        thr.append(d['artifact_B_throughput']['queries_per_second'])    ax2.set_title('Query Throughput', fontsize=14, fontweight='bold', pad=20)

        ax2.grid(True, alpha=0.3, axis='y')

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(15, 4))    

    labels = [c[1] for c in configs]    for bar in bars:

    x = np.arange(len(labels))        height = bar.get_height()

    width = 0.35        ax2.text(bar.get_x() + bar.get_width()/2., height,

                    f'{height:.0f} QPS', ha='center', va='bottom',

    ax1.bar(x - width/2, lat_p95, width, label='P95', color='#3498db', alpha=0.8)                fontsize=11, fontweight='bold')

    ax1.bar(x + width/2, lat_p99, width, label='P99', color='#e74c3c', alpha=0.8)    

    ax1.set_ylabel('Latency (ms)', fontweight='bold')    ax3 = plt.subplot(1, 3, 3)

    ax1.set_title('Latency Distribution')    bars = ax3.bar([c[1] for c in configs], memories,

    ax1.set_xticks(x)                   color=['#9b59b6', '#e67e22'],

    ax1.set_xticklabels(labels)                   alpha=0.8, edgecolor='white', linewidth=2)

    ax1.legend()    ax3.set_ylabel('Memory (MB)', fontweight='bold')

    ax1.grid(alpha=0.3)    ax3.set_xlabel('Datastore', fontweight='bold')

        ax3.set_title('Memory Footprint', fontsize=14, fontweight='bold', pad=20)

    ax2.bar(labels, thr, color=['#27ae60', '#f39c12'], alpha=0.8)    ax3.grid(True, alpha=0.3, axis='y')

    ax2.set_ylabel('Throughput (QPS)', fontweight='bold')    

    ax2.set_title('Query Throughput')    for bar in bars:

    ax2.grid(alpha=0.3)        height = bar.get_height()

            ax3.text(bar.get_x() + bar.get_width()/2., height,

    eff = [t/l for t, l in zip(thr, lat_p95)]                f'{height:.1f} MB', ha='center', va='bottom',

    ax3.bar(labels, eff, color=['#9b59b6', '#1abc9c'], alpha=0.8)                fontsize=11, fontweight='bold')

    ax3.set_ylabel('Efficiency (QPS/ms)', fontweight='bold')    

    ax3.set_title('Processing Efficiency')    plt.suptitle('💾 Datastore Comparison: Custom JSON vs SQLite', 

    ax3.grid(alpha=0.3)                 fontsize=16, fontweight='bold', y=1.02)

        plt.tight_layout()

    plt.suptitle('Query Processing: TAAT vs DAAT', fontsize=14, fontweight='bold')    plt.savefig(PLOTS_DIR / 'plot_A_datastores.png', dpi=300, bbox_inches='tight')

    plt.tight_layout()    plt.close()

    plt.savefig(PLOTS_DIR / 'plot_AC_query_modes.png', dpi=300, bbox_inches='tight')    print(f"✅ {PLOTS_DIR / 'plot_A_datastores.png'}")

    plt.close()

    print(f"✅ Saved plot_AC_query_modes.png")

def plot_compression():

def plot_elasticsearch():    """Plot AB: Compare None (c1) vs Elias (c2) vs Zlib (c3)"""

    print("📊 ES Comparison...")    print("📊 Generating Plot AB: Compression...")

        

    si = load_eval("results/eval_SelfIndex_i3d1c1o0_qDAAT.json")    configs = [

    cold = load_eval("results/eval_esindex-v1.0_COLD.json")        ('i3d1c1o0', 'No Compression'),

    warm = load_eval("results/eval_esindex-v1.0_WARM.json")        ('i3d1c2o0', 'Elias-Fano'),

            ('i3d1c3o0', 'Zlib')

    labels = ['SelfIndex', 'ES (COLD)', 'ES (WARM)']    ]

    lat = [si['artifact_A_latency']['p95_ms'],     

           cold['artifact_A_latency']['p95_ms'],     latencies = []

           warm['artifact_A_latency']['p95_ms']]    throughputs = []

    thr = [si['artifact_B_throughput']['queries_per_second'],    index_sizes = []

           cold['artifact_B_throughput']['queries_per_second'],    

           warm['artifact_B_throughput']['queries_per_second']]    for config, label in configs:

    size = [si['artifact_C_memory']['disk_mb'],        filepath = f"results/eval_SelfIndex_{config}_qTAAT.json"

            cold['artifact_C_memory']['disk_mb'],        data = load_evaluation_file(filepath)

            warm['artifact_C_memory']['disk_mb']]        latencies.append(data['artifact_A']['latency_p95_ms'])

            throughputs.append(data['artifact_B']['throughput_qps'])

    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10))        index_sizes.append(data['artifact_C']['index_size_mb'])

        

    ax1.bar(labels, lat, color=['#2ecc71', '#e74c3c', '#f39c12'], alpha=0.8)    fig = plt.figure(figsize=(16, 10))

    ax1.set_ylabel('Latency P95 (ms)', fontweight='bold')    

    ax1.set_title('Query Latency')    ax1 = plt.subplot(2, 2, 1)

    ax1.grid(alpha=0.3)    create_dual_axis_plot(ax1, None, latencies, throughputs,

    for i in range(1, 3):                          [c[1] for c in configs],

        speedup = lat[i] / lat[0]                          'Latency P95 (ms)', 'Throughput (QPS)',

        ax1.text(i, lat[i], f'{speedup:.1f}× slower', ha='center', va='bottom', fontsize=9)                          'Performance Impact',

                              ['#e74c3c', '#27ae60'])

    ax2.bar(labels, thr, color=['#2ecc71', '#3498db', '#9b59b6'], alpha=0.8)    

    ax2.set_ylabel('Throughput (QPS)', fontweight='bold')    ax2 = plt.subplot(2, 2, 2)

    ax2.set_title('Query Throughput')    bars = ax2.bar([c[1] for c in configs], index_sizes,

    ax2.grid(alpha=0.3)                   color=['#3498db', '#9b59b6', '#e67e22'],

    for i in range(1, 3):                   alpha=0.8, edgecolor='white', linewidth=2)

        ratio = thr[i] / thr[0]    ax2.set_ylabel('Index Size (MB)', fontweight='bold')

        ax2.text(i, thr[i], f'{ratio:.2f}×', ha='center', va='bottom', fontsize=9)    ax2.set_xlabel('Compression Method', fontweight='bold')

        ax2.set_title('Disk Space Usage', fontsize=14, fontweight='bold', pad=20)

    ax3.bar(labels, size, color=['#2ecc71', '#e67e22', '#c0392b'], alpha=0.8)    ax2.grid(True, alpha=0.3, axis='y')

    ax3.set_ylabel('Index Size (MB)', fontweight='bold')    

    ax3.set_title('Storage Footprint')    for i, bar in enumerate(bars):

    ax3.grid(alpha=0.3)        height = bar.get_height()

    for i in range(1, 3):        if i == 0:

        ratio = size[i] / size[0]            label_text = f'{height:.1f} MB\n(Baseline)'

        ax3.text(i, size[i], f'{ratio:.1f}× larger', ha='center', va='bottom', fontsize=9)        else:

                savings = (1 - height/index_sizes[0]) * 100

    # Performance vs Size scatter            label_text = f'{height:.1f} MB\n({savings:.1f}% saved)'

    colors_map = ['#2ecc71', '#e74c3c', '#f39c12']        ax2.text(bar.get_x() + bar.get_width()/2., height,

    for i, (s, t, lbl) in enumerate(zip(size, thr, labels)):                label_text, ha='center', va='bottom',

        ax4.scatter(s, t, s=800, alpha=0.7, color=colors_map[i], edgecolor='white', linewidth=3)                fontsize=9, fontweight='bold')

        ax4.text(s, t, lbl, ha='center', va='center', fontsize=9, fontweight='bold', color='white')    

        ax3 = plt.subplot(2, 2, 3)

    ax4.set_xlabel('Index Size (MB)', fontweight='bold')    ratios = [index_sizes[0] / size for size in index_sizes]

    ax4.set_ylabel('Throughput (QPS)', fontweight='bold')    bars = ax3.bar([c[1] for c in configs], ratios,

    ax4.set_title('Performance vs Storage Trade-off')                   color=['#95a5a6', '#1abc9c', '#f39c12'],

    ax4.grid(alpha=0.3)                   alpha=0.8, edgecolor='white', linewidth=2)

        ax3.set_ylabel('Compression Ratio (×)', fontweight='bold')

    plt.suptitle('SelfIndex vs Elasticsearch Comparison', fontsize=14, fontweight='bold')    ax3.set_xlabel('Compression Method', fontweight='bold')

    plt.figtext(0.5, 0.01, 'Fair comparison: top-10, single-field, no doc fetching | COLD=cache cleared, WARM=cache populated',    ax3.set_title('Compression Effectiveness', fontsize=14, fontweight='bold', pad=20)

                ha='center', fontsize=8, style='italic')    ax3.axhline(y=1, color='red', linestyle='--', alpha=0.5, label='No compression')

    plt.tight_layout(rect=[0, 0.03, 1, 1])    ax3.grid(True, alpha=0.3, axis='y')

    plt.savefig(PLOTS_DIR / 'plot_ES_comparison.png', dpi=300, bbox_inches='tight')    ax3.legend()

    plt.close()    

    print(f"✅ Saved plot_ES_comparison.png")    for bar in bars:

        height = bar.get_height()

def main():        ax3.text(bar.get_x() + bar.get_width()/2., height,

    print("\n" + "="*60)                f'{height:.2f}×', ha='center', va='bottom',

    print("🎨 PLOT GENERATION")                fontsize=10, fontweight='bold')

    print("="*60 + "\n")    

        ax4 = plt.subplot(2, 2, 4)

    plot_index_types()    space_saved = [(1 - size/index_sizes[0]) * 100 for size in index_sizes]

    plot_datastores()    colors_map = ['#95a5a6', '#1abc9c', '#f39c12']

    plot_compression()    

    plot_optimizations()    for i, (lat, space, label) in enumerate(zip(latencies, space_saved, [c[1] for c in configs])):

    plot_query_modes()        ax4.scatter(space, lat, s=500, alpha=0.7, 

    plot_elasticsearch()                   color=colors_map[i], edgecolor='white', linewidth=2,

                       label=label)

    print("\n" + "="*60)        ax4.text(space, lat, label, 

    print("✅ ALL 6 PLOTS GENERATED")                ha='center', va='center', fontsize=9, fontweight='bold')

    print("="*60)    

    print(f"\n📁 {PLOTS_DIR.absolute()}\n")    ax4.set_xlabel('Space Saved (%)', fontweight='bold')

    for f in sorted(PLOTS_DIR.glob("*.png")):    ax4.set_ylabel('Latency P95 (ms)', fontweight='bold')

        print(f"  • {f.name}")    ax4.set_title('Speed vs Space Trade-off', fontsize=14, fontweight='bold', pad=20)

    print("\n🎉 Ready for your report!\n")    ax4.grid(True, alpha=0.3)

    ax4.legend(loc='upper right', framealpha=0.95)

if __name__ == "__main__":    

    main()    plt.suptitle('🗜️ Compression: Performance vs Space Trade-off', 

                 fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / 'plot_AB_compression.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✅ {PLOTS_DIR / 'plot_AB_compression.png'}")


def plot_optimizations():
    """Plot A: Compare No Optimization (o0) vs Skip Pointers (osp)"""
    print("📊 Generating Plot A: Optimizations...")
    
    configs = [
        ('i3d1c1o0', 'No Optimization'),
        ('i3d1c1osp', 'Skip Pointers')
    ]
    
    latencies = []
    throughputs = []
    
    for config, label in configs:
        filepath = f"results/eval_SelfIndex_{config}_qTAAT.json"
        data = load_evaluation_file(filepath)
        latencies.append(data['artifact_A']['latency_p95_ms'])
        throughputs.append(data['artifact_B']['throughput_qps'])
    
    lat_improvement = ((latencies[0] - latencies[1]) / latencies[0]) * 100
    thr_improvement = ((throughputs[1] - throughputs[0]) / throughputs[0]) * 100
    
    fig = plt.figure(figsize=(16, 5))
    
    ax1 = plt.subplot(1, 3, 1)
    bars = ax1.bar([c[1] for c in configs], latencies,
                   color=['#95a5a6', '#27ae60'],
                   alpha=0.8, edgecolor='white', linewidth=2)
    ax1.set_ylabel('Latency P95 (ms)', fontweight='bold')
    ax1.set_xlabel('Configuration', fontweight='bold')
    ax1.set_title('Query Latency', fontsize=14, fontweight='bold', pad=20)
    ax1.grid(True, alpha=0.3, axis='y')
    
    for i, bar in enumerate(bars):
        height = bar.get_height()
        if i == 1:
            label_text = f'{height:.2f} ms\n({lat_improvement:+.1f}%)'
        else:
            label_text = f'{height:.2f} ms\n(Baseline)'
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                label_text, ha='center', va='bottom',
                fontsize=10, fontweight='bold')
    
    ax2 = plt.subplot(1, 3, 2)
    bars = ax2.bar([c[1] for c in configs], throughputs,
                   color=['#95a5a6', '#3498db'],
                   alpha=0.8, edgecolor='white', linewidth=2)
    ax2.set_ylabel('Throughput (QPS)', fontweight='bold')
    ax2.set_xlabel('Configuration', fontweight='bold')
    ax2.set_title('Query Throughput', fontsize=14, fontweight='bold', pad=20)
    ax2.grid(True, alpha=0.3, axis='y')
    
    for i, bar in enumerate(bars):
        height = bar.get_height()
        if i == 1:
            label_text = f'{height:.0f} QPS\n({thr_improvement:+.1f}%)'
        else:
            label_text = f'{height:.0f} QPS\n(Baseline)'
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                label_text, ha='center', va='bottom',
                fontsize=10, fontweight='bold')
    
    ax3 = plt.subplot(1, 3, 3)
    metrics = ['Latency\nReduction', 'Throughput\nIncrease']
    improvements = [lat_improvement, thr_improvement]
    colors = ['#27ae60' if x > 0 else '#e74c3c' for x in improvements]
    
    bars = ax3.barh(metrics, improvements, color=colors,
                    alpha=0.8, edgecolor='white', linewidth=2)
    ax3.set_xlabel('Improvement (%)', fontweight='bold')
    ax3.set_title('Performance Gain', fontsize=14, fontweight='bold', pad=20)
    ax3.axvline(x=0, color='black', linestyle='-', linewidth=1)
    ax3.grid(True, alpha=0.3, axis='x')
    
    for i, (bar, val) in enumerate(zip(bars, improvements)):
        width = bar.get_width()
        ax3.text(width, bar.get_y() + bar.get_height()/2.,
                f' {val:+.1f}%', ha='left' if width > 0 else 'right',
                va='center', fontsize=12, fontweight='bold')
    
    plt.suptitle('⚡ Query Optimization: Skip Pointers Impact', 
                 fontsize=16, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / 'plot_A_optimizations.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✅ {PLOTS_DIR / 'plot_A_optimizations.png'}")


def plot_query_modes():
    """Plot AC: Compare TAAT vs DAAT"""
    print("📊 Generating Plot AC: Query Modes...")
    
    configs = [
        ('i3d1c1o0_qTAAT', 'TAAT'),
        ('i3d1c1o0_qDAAT', 'DAAT')
    ]
    
    latencies = []
    throughputs = []
    latency_p99 = []
    
    for config, label in configs:
        filepath = f"results/eval_SelfIndex_{config}.json"
        data = load_evaluation_file(filepath)
        latencies.append(data['artifact_A']['latency_p95_ms'])
        latency_p99.append(data['artifact_A']['latency_p99_ms'])
        throughputs.append(data['artifact_B']['throughput_qps'])
    
    fig = plt.figure(figsize=(16, 5))
    
    ax1 = plt.subplot(1, 3, 1)
    x = np.arange(len(configs))
    width = 0.35
    
    bars1 = ax1.bar(x - width/2, latencies, width, label='P95',
                    color='#3498db', alpha=0.8, edgecolor='white', linewidth=2)
    bars2 = ax1.bar(x + width/2, latency_p99, width, label='P99',
                    color='#e74c3c', alpha=0.8, edgecolor='white', linewidth=2)
    
    ax1.set_ylabel('Latency (ms)', fontweight='bold')
    ax1.set_xlabel('Query Processing Mode', fontweight='bold')
    ax1.set_title('Latency Distribution', fontsize=14, fontweight='bold', pad=20)
    ax1.set_xticks(x)
    ax1.set_xticklabels([c[1] for c in configs], fontweight='bold')
    ax1.legend(framealpha=0.95, edgecolor='gray')
    ax1.grid(True, alpha=0.3, axis='y')
    
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.2f}', ha='center', va='bottom',
                    fontsize=9, fontweight='bold')
    
    ax2 = plt.subplot(1, 3, 2)
    bars = ax2.bar([c[1] for c in configs], throughputs,
                   color=['#27ae60', '#f39c12'],
                   alpha=0.8, edgecolor='white', linewidth=2)
    ax2.set_ylabel('Throughput (QPS)', fontweight='bold')
    ax2.set_xlabel('Query Processing Mode', fontweight='bold')
    ax2.set_title('Query Throughput', fontsize=14, fontweight='bold', pad=20)
    ax2.grid(True, alpha=0.3, axis='y')
    
    for bar in bars:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.0f} QPS', ha='center', va='bottom',
                fontsize=10, fontweight='bold')
    
    ax3 = plt.subplot(1, 3, 3)
    efficiency = [t/l for t, l in zip(throughputs, latencies)]
    bars = ax3.bar([c[1] for c in configs], efficiency,
                   color=['#9b59b6', '#1abc9c'],
                   alpha=0.8, edgecolor='white', linewidth=2)
    ax3.set_ylabel('Efficiency (QPS/ms)', fontweight='bold')
    ax3.set_xlabel('Query Processing Mode', fontweight='bold')
    ax3.set_title('Processing Efficiency', fontsize=14, fontweight='bold', pad=20)
    ax3.grid(True, alpha=0.3, axis='y')
    
    for bar in bars:
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}', ha='center', va='bottom',
                fontsize=10, fontweight='bold')
    
    plt.suptitle('🔄 Query Processing: TAAT vs DAAT', 
                 fontsize=16, fontweight='bold', y=1.02)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / 'plot_AC_query_modes.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✅ {PLOTS_DIR / 'plot_AC_query_modes.png'}")


def plot_elasticsearch_comparison():
    """ES Comparison: SelfIndex vs Elasticsearch"""
    print("📊 Generating ES Comparison Plot...")
    
    selfindex_data = load_evaluation_file("results/eval_SelfIndex_i3d1c1o0_qDAAT.json")
    es_cold_data = load_evaluation_file("results/eval_esindex-v1.0_COLD.json")
    es_warm_data = load_evaluation_file("results/eval_esindex-v1.0_WARM.json")
    
    systems = ['SelfIndex\n(TF-IDF)', 'Elasticsearch\n(COLD)', 'Elasticsearch\n(WARM)']
    latencies = [
        selfindex_data['artifact_A']['latency_p95_ms'],
        es_cold_data['artifact_A']['latency_p95_ms'],
        es_warm_data['artifact_A']['latency_p95_ms']
    ]
    throughputs = [
        selfindex_data['artifact_B']['throughput_qps'],
        es_cold_data['artifact_B']['throughput_qps'],
        es_warm_data['artifact_B']['throughput_qps']
    ]
    index_sizes = [
        selfindex_data['artifact_C']['index_size_mb'],
        es_cold_data['artifact_C']['index_size_mb'],
        es_warm_data['artifact_C']['index_size_mb']
    ]
    
    fig = plt.figure(figsize=(18, 10))
    
    ax1 = plt.subplot(2, 3, 1)
    bars = ax1.bar(systems, latencies,
                   color=['#2ecc71', '#e74c3c', '#f39c12'],
                   alpha=0.8, edgecolor='white', linewidth=2)
    ax1.set_ylabel('Latency P95 (ms)', fontweight='bold')
    ax1.set_title('Query Latency', fontsize=13, fontweight='bold', pad=15)
    ax1.grid(True, alpha=0.3, axis='y')
    
    for i, bar in enumerate(bars):
        height = bar.get_height()
        if i == 0:
            label_text = f'{height:.2f} ms'
        else:
            speedup = latencies[i] / latencies[0]
            label_text = f'{height:.2f} ms\n({speedup:.1f}× slower)'
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                label_text, ha='center', va='bottom',
                fontsize=9, fontweight='bold')
    
    ax2 = plt.subplot(2, 3, 2)
    bars = ax2.bar(systems, throughputs,
                   color=['#2ecc71', '#3498db', '#9b59b6'],
                   alpha=0.8, edgecolor='white', linewidth=2)
    ax2.set_ylabel('Throughput (QPS)', fontweight='bold')
    ax2.set_title('Query Throughput', fontsize=13, fontweight='bold', pad=15)
    ax2.grid(True, alpha=0.3, axis='y')
    
    for i, bar in enumerate(bars):
        height = bar.get_height()
        if i == 0:
            label_text = f'{height:.0f} QPS'
        else:
            ratio = throughputs[i] / throughputs[0]
            label_text = f'{height:.0f} QPS\n({ratio:.2f}×)'
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                label_text, ha='center', va='bottom',
                fontsize=9, fontweight='bold')
    
    ax3 = plt.subplot(2, 3, 3)
    bars = ax3.bar(systems, index_sizes,
                   color=['#2ecc71', '#e67e22', '#c0392b'],
                   alpha=0.8, edgecolor='white', linewidth=2)
    ax3.set_ylabel('Index Size (MB)', fontweight='bold')
    ax3.set_title('Storage Footprint', fontsize=13, fontweight='bold', pad=15)
    ax3.grid(True, alpha=0.3, axis='y')
    
    for i, bar in enumerate(bars):
        height = bar.get_height()
        if i == 0:
            label_text = f'{height:.1f} MB'
        else:
            ratio = height / index_sizes[0]
            label_text = f'{height:.1f} MB\n({ratio:.1f}× larger)'
        ax3.text(bar.get_x() + bar.get_width()/2., height,
                label_text, ha='center', va='bottom',
                fontsize=9, fontweight='bold')
    
    ax4 = plt.subplot(2, 3, 4)
    colors_map = ['#2ecc71', '#e74c3c', '#f39c12']
    labels_short = ['SelfIndex', 'ES (COLD)', 'ES (WARM)']
    
    for i, (size, thr, label) in enumerate(zip(index_sizes, throughputs, labels_short)):
        ax4.scatter(size, thr, s=800, alpha=0.7,
                   color=colors_map[i], edgecolor='white', linewidth=3,
                   label=label, zorder=3)
        ax4.text(size, thr, label, ha='center', va='center',
                fontsize=9, fontweight='bold', color='white')
    
    ax4.set_xlabel('Index Size (MB)', fontweight='bold')
    ax4.set_ylabel('Throughput (QPS)', fontweight='bold')
    ax4.set_title('Performance vs Storage', fontsize=13, fontweight='bold', pad=15)
    ax4.grid(True, alpha=0.3)
    ax4.legend(loc='upper left', framealpha=0.95, fontsize=9)
    
    ax5 = plt.subplot(2, 3, 5)
    p50_vals = [
        selfindex_data['artifact_A']['latency_median_ms'],
        es_cold_data['artifact_A']['latency_median_ms'],
        es_warm_data['artifact_A']['latency_median_ms']
    ]
    p99_vals = [
        selfindex_data['artifact_A']['latency_p99_ms'],
        es_cold_data['artifact_A']['latency_p99_ms'],
        es_warm_data['artifact_A']['latency_p99_ms']
    ]
    
    x = np.arange(len(systems))
    width = 0.25
    
    ax5.bar(x - width, p50_vals, width, label='P50',
            color='#27ae60', alpha=0.8, edgecolor='white', linewidth=1.5)
    ax5.bar(x, latencies, width, label='P95',
            color='#f39c12', alpha=0.8, edgecolor='white', linewidth=1.5)
    ax5.bar(x + width, p99_vals, width, label='P99',
            color='#e74c3c', alpha=0.8, edgecolor='white', linewidth=1.5)
    
    ax5.set_ylabel('Latency (ms)', fontweight='bold')
    ax5.set_title('Latency Percentiles', fontsize=13, fontweight='bold', pad=15)
    ax5.set_xticks(x)
    ax5.set_xticklabels([s.replace('\n', ' ') for s in systems], fontsize=9)
    ax5.legend(framealpha=0.95, fontsize=9)
    ax5.grid(True, alpha=0.3, axis='y')
    
    ax6 = plt.subplot(2, 3, 6)
    ax6.axis('off')
    
    summary_data = [
        ['Metric', 'SelfIndex', 'ES (COLD)', 'ES (WARM)'],
        ['Latency P95', f'{latencies[0]:.2f} ms', f'{latencies[1]:.2f} ms', f'{latencies[2]:.2f} ms'],
        ['Throughput', f'{throughputs[0]:.0f} QPS', f'{throughputs[1]:.0f} QPS', f'{throughputs[2]:.0f} QPS'],
        ['Index Size', f'{index_sizes[0]:.1f} MB', f'{index_sizes[1]:.1f} MB', f'{index_sizes[2]:.1f} MB'],
        ['Efficiency', f'{throughputs[0]/latencies[0]:.1f}', 
         f'{throughputs[1]/latencies[1]:.1f}', f'{throughputs[2]/latencies[2]:.1f}'],
        ['Winner', '✅ Fastest', '❄️ Cold', '🔥 Warm']
    ]
    
    table = ax6.table(cellText=summary_data, cellLoc='center', loc='center',
                     bbox=[0, 0, 1, 1])
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 2.5)
    
    for i in range(4):
        table[(0, i)].set_facecolor('#34495e')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    row_colors = ['#ecf0f1', '#ffffff']
    for i in range(1, len(summary_data)):
        for j in range(4):
            table[(i, j)].set_facecolor(row_colors[i % 2])
            if j == 1:
                table[(i, j)].set_facecolor('#d5f4e6')
    
    ax6.set_title('Performance Summary', fontsize=13, fontweight='bold', pad=20)
    
    plt.suptitle('🏆 SelfIndex vs Elasticsearch Comparison', 
                 fontsize=16, fontweight='bold', y=0.995)
    
    fig.text(0.5, 0.02,
             'Fair comparison: top-10 results, single-field search, no document fetching\n'
             'COLD = Cache cleared, WARM = Cache populated',
             ha='center', fontsize=9, style='italic', color='#555',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))
    
    plt.tight_layout(rect=[0, 0.04, 1, 0.99])
    plt.savefig(PLOTS_DIR / 'plot_ES_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✅ {PLOTS_DIR / 'plot_ES_comparison.png'}")


def main():
    """Generate all plots"""
    print("\n" + "="*60)
    print("🎨 PLOT GENERATION - IR Assignment")
    print("="*60 + "\n")
    
    plot_index_types()
    plot_datastores()
    plot_compression()
    plot_optimizations()
    plot_query_modes()
    plot_elasticsearch_comparison()
    
    print("\n" + "="*60)
    print("✅ ALL PLOTS GENERATED")
    print("="*60)
    print(f"\n📁 Location: {PLOTS_DIR.absolute()}")
    print(f"📊 Total plots: 6\n")
    for plot_file in sorted(PLOTS_DIR.glob("*.png")):
        print(f"  • {plot_file.name}")
    print("\n🎉 Ready for LaTeX report!")


if __name__ == "__main__":
    main()