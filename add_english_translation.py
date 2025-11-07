import sqlite3
import pandas as pd

DB_FILE = "vulgate_latlearn.db"
EN_FILE = "english_vulgate.csv"

# Load English file with robust delimiter detection
en = pd.read_csv(EN_FILE, sep=None, engine="python")
en.columns = [c.strip().lower() for c in en.columns]

for col in ("book", "chapter", "verse"):
    if col not in en.columns:
        raise SystemExit(f"english_vulgate.csv missing '{col}'. Found: {en.columns}")

# Find English text column
text_col = None
for cand in ("text", "english", "translation", "verse_text"):
    if cand in en.columns:
        text_col = cand
        break

if not text_col:
    raise SystemExit(f"Could not find English text column in english_vulgate.csv. Found: {en.columns}")

en = en[["book", "chapter", "verse", text_col]].rename(columns={text_col: "translation_en"})
en["book"] = en["book"].astype(str).str.strip()
en["chapter"] = en["chapter"].astype(str).str.strip()
en["verse"] = en["verse"].astype(str).str.strip()
en["translation_en"] = en["translation_en"].astype(str)

# Connect DB
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

# Ensure sentences table has translation_en column
cur.execute("PRAGMA table_info(sentences)")
cols = {row[1] for row in cur.fetchall()}
if "translation_en" not in cols:
    cur.execute("ALTER TABLE sentences ADD COLUMN translation_en TEXT;")
    conn.commit()

# Build index from English verses
en_map = {
    (row["book"], row["chapter"], row["verse"]): row["translation_en"]
    for _, row in en.iterrows()
}

# Update sentences: every sentence from a verse gets that verse's English
cur.execute("SELECT id, book, chapter, verse FROM sentences")
rows = cur.fetchall()

updates = []
for sid, book, chapter, verse in rows:
    key = (str(book).strip(), str(chapter).strip(), str(verse).strip())
    eng = en_map.get(key, "")
    if eng:
        updates.append((eng, sid))

if updates:
    cur.executemany(
        "UPDATE sentences SET translation_en = ? WHERE id = ?",
        updates
    )
    conn.commit()

conn.close()
print(f"Attached English translations to {len(updates)} sentences.")
