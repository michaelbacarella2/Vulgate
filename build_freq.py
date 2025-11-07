import pandas as pd
import re
from collections import Counter

# Load Vulgate with automatic delimiter detection
try:
    df = pd.read_csv("vulgate.csv", sep=None, engine="python")
except Exception as e:
    raise SystemExit(f"Failed to read vulgate.csv: {e}")

# Normalize column names
df.columns = [c.strip().lower() for c in df.columns]

if "text" not in df.columns:
    raise SystemExit(f"Could not find 'text' column in vulgate.csv. Found columns: {df.columns}")

texts = df["text"].astype(str).tolist()
tokens = []

for line in texts:
    line = line.lower()
    # letters (including accented) + spaces only
    line = re.sub(r"[^a-zA-ZÀ-ÿ\s]", " ", line)
    words = [w for w in line.split() if w]
    tokens.extend(words)

freq = Counter(tokens)

# All forms, sorted by frequency
most_common = freq.most_common()

out = pd.DataFrame(most_common, columns=["form", "count"])

# Add:
# id: simple 1..N
# freq_rank: same as id (1 = most frequent)
out.insert(0, "id", range(1, len(out) + 1))
out["freq_rank"] = out["id"]

out.to_csv("freq_all.csv", index=False, encoding="utf-8")

print("Wrote freq_all.csv with", len(out), "rows.")
