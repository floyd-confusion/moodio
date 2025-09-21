import pandas as pd
import matplotlib.pyplot as plt

# Load CSV file
df = pd.read_csv("../data/dataset.csv")

# List of properties to plot
properties = [
    'danceability', 'energy', 'speechiness', 'valence',
    'tempo', 'acousticness', 'instrumentalness', 'liveness'
]

# Keep only existing columns
properties = [p for p in properties if p in df.columns]

# Create one figure with subplots stacked
fig, axes = plt.subplots(len(properties), 1, figsize=(10, 2*len(properties)), sharex=False)

if len(properties) == 1:
    axes = [axes]  # make iterable if only one subplot

for ax, prop in zip(axes, properties):
    df[prop].dropna().plot(kind="kde", ax=ax, fill=True, alpha=0.6)
    ax.set_title(prop, fontsize=10)
    ax.set_ylabel("Density")
    ax.set_xlabel("")
    ax.grid(True, linestyle="--", alpha=0.4)

plt.tight_layout()
plt.savefig("all_distributions.png", dpi=150)
plt.close()