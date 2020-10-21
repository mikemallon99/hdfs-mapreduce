import glob
import numpy as np
from pathlib import Path
import matplotlib

import matplotlib.pyplot as plt

a2a_means = []
a2a_sds = []
gossip_means = []
gossip_sds = []

for filename in sorted(glob.glob("./bw_data/*.log")):
    with open(filename, "r") as f:
        data = []
        for line in f.readlines():
            if not line[0].isnumeric():
                continue
            tokens = line.split()
            bw_data = float(tokens[1])
            data.append(bw_data)

        if filename.startswith("./bw_data/a2a"):
            a2a_means.append(np.mean(data))
            a2a_sds.append(np.std(data))
        else:
            gossip_means.append(np.mean(data))
            gossip_sds.append(np.std(data))

fig, (ax1, ax2) = plt.subplots(1, 2, sharex=True, figsize=(8, 4))

ax1.errorbar(np.arange(2, 11, 2), a2a_means, a2a_sds, fmt='ok', lw=1)
ax1.title.set_text("All2All")
ax1.set_ylabel("Bandwidth (bps)")
ax1.set_xlabel("# of nodes in group")

ax2.errorbar(np.arange(2, 11, 2), gossip_means, gossip_sds, fmt='ok', lw=1)
ax2.title.set_text("Gossip")
ax2.set_ylabel("Bandwidth (bps)")
ax2.set_xlabel("# of nodes in group")

plt.subplots_adjust(wspace=0.4)

plt.tight_layout()
plt.show()
