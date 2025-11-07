import pandas as pd
import re

SENTENCES_FILE = "sentences.csv"
OUTPUT_FILE = "tokens.csv"

# Robust load with automatic delimiter detection
try:
    df = pd.read_csv(SENTENCES_FILE, sep=None, engine="python")
except Exception as e:
    raise SystemExit(f"Failed to read {SENTENCES_FILE}: {e}")

# Normalize column names
df.columns = [c.strip().lower() for c in df.columns]

required = ("sentence_id", "latin_text")
missing = [c for c in required if c not in df.columns]
if missing:
    raise SystemExit(f"Missing columns {missing} in {SENTENCES_FILE}. Found: {list(df.columns)}")

token_rows = []
token_id = 1

for _, row in df.iterrows():
    try:
        sentence_id = int(row["sentence_id"])
    except ValueError:
        continue

    text = str(row["latin_text"])

    # Split on any non-letter
    parts = re.split(r"[^A-Za-zÀ-ÿ]+", text)

    position = 0
    for p in parts:
        surface = p.strip()
        if not surface:
            continue

        # normalized form for frequency matching
        form = surface.lower()

        position += 1
        token_rows.append({
            "token_id": token_id,
            "sentence_id": sentence_id,
            "position": position,
            "surface": surface,  # original as it appears
            "form": form         # normalized for joins/frequency
        })
        token_id += 1

tokens_df = pd.DataFrame(token_rows, columns=["token_id", "sentence_id", "position", "surface", "form"])
tokens_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

print(f"Wrote {OUTPUT_FILE} with {len(tokens_df)} rows.")
