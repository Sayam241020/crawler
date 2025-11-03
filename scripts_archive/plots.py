import json, matplotlib.pyplot as plt, numpy as np
from pathlib import Path
plt.style.use('ggplot')
PLOTS_DIR = Path('plots')
PLOTS_DIR.mkdir(exist_ok=True)
def load(f): return json.load(open(f))
print('Starting...')
