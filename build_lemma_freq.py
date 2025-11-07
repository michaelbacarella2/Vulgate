import sqlite3
import pandas as pd

DB_FILE = "vulgate_latlearn.db"

conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

# Ensure tokens has lemma column from Whitaker step
cur.execute("PRAGMA table_info(tokens);")
cols = {row[1] for row in cur.fetchall()}
if "lemma" not in cols:
    raise SystemExit("tokens table has no 'lemma' column. Run add_morphology_whitaker.py first.")

# Read lemmas from tokens
df = pd.read_sql_query(
    "SELECT lemma FROM tokens WHERE lemma IS NOT NULL AND TRIM(lemma) != ''",
    conn,
)

# Normalize lemma strings
df["lemma"] = df["lemma"].astype(str).str.strip()

# Count frequency by lemma
lemma_counts = (
    df.groupby("lemma")
      .size()
      .reset_index(name="count")
      .sort_values("count", ascending=False)
      .reset_index(drop=True)
)

# Add freq_rank (1 = most frequent)
lemma_counts.insert(0, "id", range(1, len(lemma_counts) + 1))
lemma_counts["freq_rank"] = lemma_counts["id"]

# Create / replace lemma_freq table
cur.execute("DROP TABLE IF EXISTS lemma_freq")
cur.execute("""
CREATE TABLE lemma_freq (
    id INTEGER PRIMARY KEY,
    lemma TEXT NOT NULL,
    freq_rank INTEGER NOT NULL,
    count INTEGER NOT NULL
)
""")

lemma_counts[["id", "lemma", "freq_rank", "count"]].to_sql(
    "lemma_freq", conn, if_exists="append", index=False
)

conn.commit()
conn.close()

print(f"Built lemma_freq with {len(lemma_counts)} lemmas.")
