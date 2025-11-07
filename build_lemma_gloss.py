import sqlite3
from whitakers_words.parser import Parser

DB_FILE = "vulgate_latlearn.db"

parser = Parser()

def normalize_lemma(raw: str) -> str:
    return raw.strip() if raw else ""

def extract_gloss(lemma: str) -> str:
    if not lemma:
        return ""
    try:
        result = parser.parse(lemma)
    except Exception:
        return ""
    if not result:
        return ""
    try:
        forms = getattr(result, "forms", None)
        if isinstance(forms, dict) and forms:
            f0 = next(iter(forms.values()))
        elif isinstance(forms, (list, tuple)) and forms:
            f0 = forms[0]
        else:
            f0 = forms
        analyses = getattr(f0, "analyses", None)
        if isinstance(analyses, dict) and analyses:
            a0 = next(iter(analyses.values()))
        elif isinstance(analyses, (list, tuple)) and analyses:
            a0 = analyses[0]
        else:
            a0 = analyses
        if not a0:
            return ""
        lex = getattr(a0, "lexeme", None)
        if lex is None:
            return ""
        for attr in ("meaning", "meanings", "gloss", "definition", "definitions"):
            if hasattr(lex, attr):
                val = getattr(lex, attr)
                if isinstance(val, str) and val.strip():
                    return val.split(";")[0].split(",")[0].strip()
                if isinstance(val, (list, tuple)) and val:
                    s = str(val[0])
                    return s.split(";")[0].split(",")[0].strip()
        return str(lex)
    except Exception:
        return ""

def main():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='lemma_freq'")
    if not cur.fetchone():
        raise SystemExit("lemma_freq table not found; run build_lemma_freq.py first.")

    cur.execute("SELECT lemma FROM lemma_freq ORDER BY freq_rank ASC")
    lemmas = [normalize_lemma(r[0]) for r in cur.fetchall() if normalize_lemma(r[0])]

    cur.execute("DROP TABLE IF EXISTS lemma_gloss")
    cur.execute("""
        CREATE TABLE lemma_gloss (
            lemma TEXT PRIMARY KEY,
            gloss TEXT
        )
    """)

    batch = []
    done = 0
    total = len(lemmas)
    for lemma in lemmas:
        gloss = extract_gloss(lemma)
        batch.append((lemma, gloss))
        if len(batch) >= 200:
            cur.executemany("INSERT INTO lemma_gloss (lemma, gloss) VALUES (?, ?)", batch)
            conn.commit()
            done += len(batch)
            print(f"{done} / {total} lemmas processed")
            batch = []
    if batch:
        cur.executemany("INSERT INTO lemma_gloss (lemma, gloss) VALUES (?, ?)", batch)
        conn.commit()
        done += len(batch)
        print(f"{done} / {total} lemmas processed")

    conn.close()
    print("lemma_gloss table built.")

if __name__ == "__main__":
    main()
