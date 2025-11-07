import pandas as pd

TOKENS_FILE = "tokens.csv"
FREQ_FILE = "freq_all.csv"
OUT_FILE = "tokens_with_freq.csv"

# Load
tokens = pd.read_csv(TOKENS_FILE, sep=None, engine="python")
freq = pd.read_csv(FREQ_FILE, sep=None, engine="python")

# Normalize column names
tokens.columns = [c.strip().lower() for c in tokens.columns]
freq.columns = [c.strip().lower() for c in freq.columns]

for col in ("token_id", "sentence_id", "position", "form"):
    if col not in tokens.columns:
        raise SystemExit(f"Missing '{col}' in {TOKENS_FILE}. Found: {tokens.columns}")

for col in ("form", "freq_rank", "count"):
    if col not in freq.columns:
        raise SystemExit(f"Missing '{col}' in {FREQ_FILE}. Found: {freq.columns}")

# Merge on normalized 'form'
merged = tokens.merge(
    freq[["form", "freq_rank", "count"]],
    on="form",
    how="left"
)

# Any form not found in freq_all (should be rare) goes to bottom priority
max_rank = merged["freq_rank"].max()
merged["freq_rank"] = merged["freq_rank"].fillna(max_rank + 1)
merged["count"] = merged["count"].fillna(1)

merged = merged.sort_values(["sentence_id", "position", "token_id"])

merged.to_csv(OUT_FILE, index=False, encoding="utf-8")

print(f"Wrote {OUT_FILE} with {len(merged)} rows.")
