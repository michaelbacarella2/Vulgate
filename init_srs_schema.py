import sqlite3
from datetime import datetime

DB_FILE = "vulgate_latlearn.db"

conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

# Users table: minimal for now
cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    email TEXT
)
""")

# User settings: per-user toggles and limits
cur.execute("""
CREATE TABLE IF NOT EXISTS user_settings (
    user_id INTEGER PRIMARY KEY,
    show_translation INTEGER NOT NULL DEFAULT 1,
    show_morphology INTEGER NOT NULL DEFAULT 1,
    daily_new_limit INTEGER NOT NULL DEFAULT 20,
    FOREIGN KEY(user_id) REFERENCES users(id)
)
""")

# User-lemma SRS state
cur.execute("""
CREATE TABLE IF NOT EXISTS user_lemma (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    lemma TEXT NOT NULL,
    streak INTEGER NOT NULL DEFAULT 0,
    interval_days INTEGER NOT NULL DEFAULT 1,
    due_date TEXT NOT NULL,
    last_result TEXT,
    last_seen_at TEXT,
    total_reviews INTEGER NOT NULL DEFAULT 0,
    correct_reviews INTEGER NOT NULL DEFAULT 0,
    UNIQUE(user_id, lemma),
    FOREIGN KEY(user_id) REFERENCES users(id)
)
""")

conn.commit()

# Seed a default local user (id = 1) if none
cur.execute("SELECT COUNT(*) FROM users")
count = cur.fetchone()[0]
if count == 0:
    cur.execute("INSERT INTO users (id, email) VALUES (?, ?)", (1, "local@example.com"))
    cur.execute("""
        INSERT INTO user_settings (user_id, show_translation, show_morphology, daily_new_limit)
        VALUES (1, 1, 1, 20)
    """)
    conn.commit()
    print("Created default user with id=1")

conn.close()
print("SRS schema initialized.")
