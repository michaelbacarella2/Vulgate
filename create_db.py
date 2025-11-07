import sqlite3
import pandas as pd

DB_FILE = "vulgate_latlearn.db"

# Load CSVs with robust parsing
sentences = pd.read_csv("sentences.csv", sep=None, engine="python")
tokens = pd.read_csv("tokens_with_freq.csv", sep=None, engine="python")
freq = pd.read_csv("freq_all.csv", sep=None, engine="python")

# Normalize column names
sentences.columns = [c.strip().lower() for c in sentences.columns]
tokens.columns = [c.strip().lower() for c in tokens.columns]
freq.columns = [c.strip().lower() for c in freq.columns]

# Required column checks
sent_required = {"sentence_id", "book", "chapter", "verse", "latin_text"}
if not sent_required.issubset(sentences.columns):
    raise SystemExit(f"sentences.csv missing columns: {sent_required - set(sentences.columns)}")

tok_required = {"token_id", "sentence_id", "position", "surface", "form", "freq_rank", "count"}
if not tok_required.issubset(tokens.columns):
    raise SystemExit(f"tokens_with_freq.csv missing columns: {tok_required - set(tokens.columns)}")

freq_required = {"id", "form", "freq_rank", "count"}
if not freq_required.issubset(freq.columns):
    raise SystemExit(f"freq_all.csv missing columns: {freq_required - set(freq.columns)}")

# Clean sentences: drop empty / NaN latin_text
sentences = sentences.dropna(subset=["latin_text"])
sentences["latin_text"] = sentences["latin_text"].astype(str).str.strip()
sentences = sentences[sentences["latin_text"] != ""]

# Clean tokens: keep only tokens pointing to valid sentences and non-empty forms
valid_sentence_ids = set(sentences["sentence_id"].astype(int))

tokens = tokens.dropna(subset=["sentence_id", "surface", "form"])
tokens["sentence_id"] = tokens["sentence_id"].astype(int)
tokens["surface"] = tokens["surface"].astype(str).str.strip()
tokens["form"] = tokens["form"].astype(str).str.strip()

tokens = tokens[tokens["sentence_id"].isin(valid_sentence_ids)]
tokens = tokens[tokens["surface"] != ""]
tokens = tokens[tokens["form"] != ""]

# Clean frequency: drop rows with missing/blank form
freq = freq.dropna(subset=["form"])
freq["form"] = freq["form"].astype(str).str.strip()
freq = freq[freq["form"] != ""]

# Connect / reset DB
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS sentences")
cur.execute("DROP TABLE IF EXISTS tokens")
cur.execute("DROP TABLE IF EXISTS forms_freq")

# Create tables
cur.execute("""
CREATE TABLE sentences (
    id INTEGER PRIMARY KEY,
    book TEXT NOT NULL,
    chapter TEXT NOT NULL,
    verse TEXT NOT NULL,
    latin_text TEXT NOT NULL
)
""")

cur.execute("""
CREATE TABLE tokens (
    id INTEGER PRIMARY KEY,
    sentence_id INTEGER NOT NULL,
    position INTEGER NOT NULL,
    surface TEXT NOT NULL,
    form TEXT NOT NULL,
    freq_rank INTEGER NOT NULL,
    count INTEGER NOT NULL,
    FOREIGN KEY(sentence_id) REFERENCES sentences(id)
)
""")

cur.execute("""
CREATE TABLE forms_freq (
    id INTEGER PRIMARY KEY,
    form TEXT NOT NULL,
    freq_rank INTEGER NOT NULL,
    count INTEGER NOT NULL
)
""")

# Insert sentences
sentences_to_insert = sentences[["sentence_id", "book", "chapter", "verse", "latin_text"]].rename(
    columns={"sentence_id": "id"}
)
sentences_to_insert.to_sql("sentences", conn, if_exists="append", index=False)

# Insert tokens
tokens_to_insert = tokens[["token_id", "sentence_id", "position", "surface", "form", "freq_rank", "count"]].rename(
    columns={"token_id": "id"}
)
tokens_to_insert.to_sql("tokens", conn, if_exists="append", index=False)

# Insert forms_freq
freq_to_insert = freq[["id", "form", "freq_rank", "count"]]
freq_to_insert.to_sql("forms_freq", conn, if_exists="append", index=False)

conn.commit()
conn.close()

print(f"Created {DB_FILE} with sentences, tokens, and forms_freq.")
