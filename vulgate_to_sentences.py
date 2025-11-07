import pandas as pd
import re

INPUT_FILE = "vulgate.csv"
OUTPUT_FILE = "sentences.csv"

# Load with auto delimiter detection
df = pd.read_csv(INPUT_FILE, sep=None, engine="python")

# Normalize column names
df.columns = [c.strip().lower() for c in df.columns]

for col in ("book", "chapter", "verse", "text"):
    if col not in df.columns:
        raise SystemExit(f"Missing column '{col}' in {INPUT_FILE}. Found: {df.columns}")

sentences = []
sentence_id = 1

# Simple Latin-ish sentence splitter
split_pattern = re.compile(r'(?<=[\.\?\!\;\:])\s+')

for _, row in df.iterrows():
    book = str(row["book"]).strip()
    chapter = str(row["chapter"]).strip()
    verse = str(row["verse"]).strip()
    text = str(row["text"]).strip()

    if not text:
        continue

    parts = split_pattern.split(text)
    for part in parts:
        s = part.strip()
        # discard useless fragments
        if len(s) < 3:
            continue
        sentences.append({
            "sentence_id": sentence_id,
            "book": book,
            "chapter": chapter,
            "verse": verse,
            "latin_text": s
        })
        sentence_id += 1

out_df = pd.DataFrame(sentences)
out_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

print(f"Wrote {OUTPUT_FILE} with {len(out_df)} rows.")
