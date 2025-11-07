import sqlite3
import re
from datetime import datetime

from whitakers_words.parser import Parser

DB_FILE = "vulgate_latlearn.db"
parser = Parser()


# ---------- DB helpers ----------

def _get_conn():
    return sqlite3.connect(DB_FILE)


def _now_iso():
    return datetime.now().isoformat(timespec="seconds")


def _ensure_schema(conn):
    cur = conn.cursor()

    # Base table (legacy-compatible)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_lemma (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            lemma TEXT NOT NULL,
            streak INTEGER DEFAULT 0,
            interval_days INTEGER DEFAULT 0,
            due_date TEXT,
            last_result TEXT,
            last_seen_at TEXT,
            total_reviews INTEGER DEFAULT 0,
            correct_reviews INTEGER DEFAULT 0
        )
    """)

    # Add SRS columns if missing
    cur.execute("PRAGMA table_info(user_lemma)")
    cols = {row[1] for row in cur.fetchall()}

    if "level" not in cols:
        cur.execute("ALTER TABLE user_lemma ADD COLUMN level INTEGER DEFAULT 1")
    if "next_due_at_card" not in cols:
        cur.execute("ALTER TABLE user_lemma ADD COLUMN next_due_at_card INTEGER")

    # Global per-user card counter
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_state (
            user_id INTEGER PRIMARY KEY,
            card_counter INTEGER NOT NULL DEFAULT 0
        )
    """)

    # User settings (for toggles)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id INTEGER PRIMARY KEY,
            show_translation INTEGER DEFAULT 1,
            show_morphology INTEGER DEFAULT 1,
            daily_new_limit INTEGER DEFAULT 999999
        )
    """)

    conn.commit()


def _get_card_counter(cur, user_id: int) -> int:
    cur.execute(
        "SELECT card_counter FROM user_state WHERE user_id = ?",
        (user_id,),
    )
    row = cur.fetchone()
    if row:
        return int(row[0])

    cur.execute(
        "INSERT INTO user_state (user_id, card_counter) VALUES (?, 0)",
        (user_id,),
    )
    return 0


def _increment_card_counter(cur, user_id: int, step: int = 1):
    cur.execute(
        "UPDATE user_state SET card_counter = card_counter + ? WHERE user_id = ?",
        (step, user_id),
    )


def _get_user_settings(cur, user_id: int):
    cur.execute(
        """
        SELECT show_translation, show_morphology, daily_new_limit
        FROM user_settings
        WHERE user_id = ?
        """,
        (user_id,),
    )
    row = cur.fetchone()
    if not row:
        cur.execute(
            "INSERT INTO user_settings (user_id, show_translation, show_morphology, daily_new_limit) VALUES (?, 1, 1, 999999)",
            (user_id,),
        )
        return 1, 1, 999999
    return int(row[0]), int(row[1]), int(row[2])


# ---------- Card-count SRS ----------

def _level_interval_cards(level: int) -> int:
    if level <= 1:
        return 5
    if level == 2:
        return 15
    if level == 3:
        return 60
    if level == 4:
        return 300
    return 1000  # level 5+


def _get_due_lemma(cur, user_id: int, current_idx: int):
    cur.execute(
        """
        SELECT ul.lemma
        FROM user_lemma ul
        JOIN (
            SELECT lemma, COUNT(*) AS c
            FROM tokens
            WHERE lemma IS NOT NULL AND TRIM(lemma) != ''
            GROUP BY lemma
        ) tf ON tf.lemma = ul.lemma
        WHERE ul.user_id = ?
          AND ul.next_due_at_card IS NOT NULL
          AND ul.next_due_at_card <= ?
        ORDER BY ul.next_due_at_card ASC, tf.c DESC
        LIMIT 1
        """,
        (user_id, current_idx),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _get_new_lemma(cur, user_id: int):
    # Real lemmas first
    cur.execute(
        """
        SELECT t.lemma
        FROM tokens t
        LEFT JOIN user_lemma ul
          ON ul.lemma = t.lemma AND ul.user_id = ?
        WHERE t.lemma IS NOT NULL
          AND TRIM(t.lemma) != ''
          AND ul.lemma IS NULL
        GROUP BY t.lemma
        ORDER BY COUNT(*) DESC
        LIMIT 1
        """,
        (user_id,),
    )
    row = cur.fetchone()
    if row and row[0]:
        return row[0]

    # Fallback: surface/form as pseudo-lemmas
    cur.execute(
        """
        SELECT lower(COALESCE(t.surface, t.form))
        FROM tokens t
        LEFT JOIN user_lemma ul
          ON ul.lemma = lower(COALESCE(t.surface, t.form)) AND ul.user_id = ?
        WHERE COALESCE(t.surface, t.form) IS NOT NULL
          AND TRIM(COALESCE(t.surface, t.form)) != ''
          AND ul.lemma IS NULL
        GROUP BY lower(COALESCE(t.surface, t.form))
        ORDER BY COUNT(*) DESC
        LIMIT 1
        """,
        (user_id,),
    )
    row = cur.fetchone()
    return row[0] if row and row[0] else None


# ---------- Whitaker helpers ----------

def _parse_form(form: str):
    try:
        return parser.parse(form)
    except Exception:
        return None


def _normalize_surface(surface: str) -> str:
    return "".join(ch for ch in surface.lower() if ch.isalpha())


def _collect_analyses(result):
    if result is None:
        return []
    forms = getattr(result, "forms", None)

    if isinstance(forms, dict):
        form_iter = forms.values()
    elif isinstance(forms, (list, tuple, set)):
        form_iter = forms
    else:
        form_iter = [forms] if forms is not None else []

    out = []
    for f in form_iter:
        if f is None:
            continue

        analyses = getattr(f, "analyses", None)
        if analyses is None and isinstance(f, dict):
            analyses = f.get("analyses")

        if isinstance(analyses, dict):
            a_iter = analyses.values()
        elif isinstance(analyses, (list, tuple, set)):
            a_iter = analyses
        elif analyses is not None:
            a_iter = [analyses]
        else:
            a_iter = []

        for a in a_iter:
            if a is not None:
                out.append(a)

    return out


def _candidate_glosses_from_senses(senses):
    candidates = []
    for s in senses:
        s = re.sub(r"\([^)]*\)", "", s)
        parts = re.split(r"[;/,]", s)
        for p in parts:
            c = p.strip()
            if not c:
                continue
            c = re.sub(r"[\.:\;]+$", "", c).strip()
            if re.search(r"[a-zA-Z]", c):
                candidates.append(c)
    seen = set()
    out = []
    for c in candidates:
        lc = c.lower()
        if lc not in seen:
            seen.add(lc)
            out.append(c)
    return out


def _get_token_gloss(surface: str, translation_en: str) -> str:
    if not surface:
        return ""

    res = _parse_form(surface)
    if not res:
        norm = _normalize_surface(surface)
        if norm:
            res = _parse_form(norm)
    if not res:
        return ""

    analyses = _collect_analyses(res)
    senses = []
    for a in analyses:
        lex = getattr(a, "lexeme", None)
        if lex is None and isinstance(a, dict):
            lex = a.get("lexeme")
        if lex is None:
            continue
        raw = getattr(lex, "senses", None)
        if raw is None and isinstance(lex, dict):
            raw = lex.get("senses")
        if isinstance(raw, (list, tuple, set)):
            for s in raw:
                s = str(s).strip()
                if s:
                    senses.append(s)
        elif isinstance(raw, str) and raw.strip():
            senses.append(raw.strip())
    if not senses:
        return ""

    cands = _candidate_glosses_from_senses(senses)
    if not cands:
        return ""

    if translation_en:
        t = translation_en.lower()
        for cand in cands:
            lc = cand.lower()
            if " " in lc and lc in t:
                return cand
            if " " not in lc and re.search(r"\b" + re.escape(lc) + r"\b", t):
                return cand

    return ""


def _get_morph_hint(surface: str) -> str:
    if not surface:
        return ""

    res = _parse_form(surface)
    if not res:
        norm = _normalize_surface(surface)
        if norm:
            res = _parse_form(norm)
    if not res:
        return ""

    analyses = _collect_analyses(res)
    if not analyses:
        return ""

    a0 = analyses[0]

    infls = getattr(a0, "inflections", None)
    if infls is None and isinstance(a0, dict):
        infls = a0.get("inflections")

    if isinstance(infls, dict):
        infls_iter = infls.values()
    elif isinstance(infls, (list, tuple, set)):
        infls_iter = infls
    elif infls is not None:
        infls_iter = [infls]
    else:
        infls_iter = []

    for inf in infls_iter:
        if inf is None:
            continue
        feats = getattr(inf, "features", None)
        if feats is None and isinstance(inf, dict):
            feats = inf.get("features")
        if not isinstance(feats, dict):
            continue

        def sval(v):
            return getattr(v, "value", str(v))

        vals = {k: sval(v) for k, v in feats.items()}

        # Verb-like
        if any(k in vals for k in ("Tense", "Mood", "Person", "Voice")):
            person = vals.get("Person", "")
            number = vals.get("Number", "")
            tense = vals.get("Tense", "")
            voice = vals.get("Voice", "")

            def s_num(x):
                x = x.lower()
                if "sing" in x:
                    return "sg"
                if "plur" in x:
                    return "pl"
                return ""

            def s_tense(x):
                x = x.lower()
                if "pres" in x:
                    return "pres"
                if "impf" in x:
                    return "impf"
                if "fut" in x and "perf" not in x:
                    return "fut"
                if "perf" in x and "fut" not in x:
                    return "perf"
                if "plup" in x:
                    return "plup"
                return ""

            def s_voice(x):
                x = x.lower()
                if "act" in x:
                    return "act"
                if "pass" in x:
                    return "pass"
                return ""

            bits = []
            if person:
                bits.append(str(person)[0])
            n = s_num(number)
            if n:
                bits.append(n)
            t = s_tense(tense)
            if t:
                bits.append(t)
            v = s_voice(voice)
            if v:
                bits.append(v)
            if bits:
                return " ".join(bits)

        # Noun/adj-like
        case = vals.get("Case", "")
        number = vals.get("Number", "")
        gender = vals.get("Gender", "")

        def s_case(x):
            x = x.lower()
            if "nom" in x:
                return "nom"
            if "gen" in x:
                return "gen"
            if "dat" in x:
                return "dat"
            if "acc" in x:
                return "acc"
            if "abl" in x:
                return "abl"
            if "voc" in x:
                return "voc"
            return ""

        def s_num2(x):
            x = x.lower()
            if "sing" in x:
                return "sg"
            if "plur" in x:
                return "pl"
            return ""

        def s_gender(x):
            x = x.lower()
            if x.startswith("m"):
                return "m"
            if x.startswith("f"):
                return "f"
            if x.startswith("n"):
                return "n"
            return ""

        bits = []
        g = s_gender(gender)
        c = s_case(case)
        n = s_num2(number)
        if g:
            bits.append(g)
        if c:
            bits.append(c)
        if n:
            bits.append(n)
        if bits:
            return " ".join(bits)

    return ""


# ---------- Token selection ----------

def _pick_token_for_lemma(cur, lemma: str):
    if not lemma:
        return None

    # Try lemma
    cur.execute(
        """
        SELECT
            t.id,
            COALESCE(t.surface, t.form) AS surf,
            s.id,
            s.latin_text,
            s.book,
            s.chapter,
            s.verse
        FROM tokens t
        JOIN sentences s ON s.id = t.sentence_id
        WHERE t.lemma = ?
          AND COALESCE(t.surface, t.form) IS NOT NULL
          AND TRIM(COALESCE(t.surface, t.form)) != ''
          AND s.latin_text IS NOT NULL
          AND TRIM(s.latin_text) != ''
        ORDER BY RANDOM()
        LIMIT 1
        """,
        (lemma,),
    )
    row = cur.fetchone()
    if row:
        return {
            "token_id": row[0],
            "surface": row[1],
            "sentence_id": row[2],
            "latin_text": row[3],
            "book": row[4],
            "chapter": row[5],
            "verse": row[6],
        }

    # Fallback: lemma as surface
    cur.execute(
        """
        SELECT
            t.id,
            COALESCE(t.surface, t.form) AS surf,
            s.id,
            s.latin_text,
            s.book,
            s.chapter,
            s.verse
        FROM tokens t
        JOIN sentences s ON s.id = t.sentence_id
        WHERE lower(COALESCE(t.surface, t.form)) = lower(?)
          AND COALESCE(t.surface, t.form) IS NOT NULL
          AND TRIM(COALESCE(t.surface, t.form)) != ''
          AND s.latin_text IS NOT NULL
          AND TRIM(s.latin_text) != ''
        ORDER BY RANDOM()
        LIMIT 1
        """,
        (lemma,),
    )
    row = cur.fetchone()
    if not row:
        return None

    return {
        "token_id": row[0],
        "surface": row[1],
        "sentence_id": row[2],
        "latin_text": row[3],
        "book": row[4],
        "chapter": row[5],
        "verse": row[6],
    }


def _pick_any_token(cur):
    cur.execute(
        """
        SELECT
            t.id,
            COALESCE(t.surface, t.form) AS surf,
            COALESCE(t.lemma, lower(COALESCE(t.surface, t.form))) AS lem,
            s.id,
            s.latin_text,
            s.book,
            s.chapter,
            s.verse
        FROM tokens t
        JOIN sentences s ON s.id = t.sentence_id
        WHERE COALESCE(t.surface, t.form) IS NOT NULL
          AND TRIM(COALESCE(t.surface, t.form)) != ''
          AND s.latin_text IS NOT NULL
          AND TRIM(s.latin_text) != ''
        ORDER BY RANDOM()
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if not row:
        return None

    token_id, surface, lemma, sid, latin, book, chap, verse = row
    lemma = (lemma or "").strip() or surface.lower()
    return {
        "token_id": token_id,
        "surface": surface,
        "lemma": lemma,
        "sentence_id": sid,
        "latin_text": latin,
        "book": book,
        "chapter": chap,
        "verse": verse,
    }


# ---------- Cloze ----------

def _make_cloze(latin_text: str, surface: str) -> str:
    pattern = r"\b" + re.escape(surface) + r"\b"
    return re.sub(pattern, "____", latin_text, count=1)


# ---------- Public: get_next_card ----------

def get_next_card(user_id: int = 1):
    conn = _get_conn()
    _ensure_schema(conn)
    cur = conn.cursor()

    show_translation, show_morphology, _ = _get_user_settings(cur, user_id)
    current_idx = _get_card_counter(cur, user_id)

    lemma = _get_due_lemma(cur, user_id, current_idx)
    if lemma is None:
        lemma = _get_new_lemma(cur, user_id)

    token = None
    if lemma:
        token = _pick_token_for_lemma(cur, lemma)

    if token is None:
        any_token = _pick_any_token(cur)
        if not any_token:
            conn.close()
            return None
        lemma = any_token["lemma"]
        token = any_token

    cur.execute(
        "SELECT translation_en FROM sentences WHERE id = ?",
        (token["sentence_id"],),
    )
    row = cur.fetchone()
    translation = str(row[0]) if row and row[0] else ""

    cloze = _make_cloze(token["latin_text"], token["surface"])
    english = _get_token_gloss(token["surface"], translation)
    morph_hint = _get_morph_hint(token["surface"]) if show_morphology else ""

    conn.close()

    card_id = f"{lemma}|{token['sentence_id']}|{token['token_id']}"

    return {
        "card_id": card_id,
        "lemma": lemma,
        "expected": token["surface"],
        "cloze": cloze,
        "latin_text": token["latin_text"],
        "book": token["book"],
        "chapter": token["chapter"],
        "verse": token["verse"],
        "reference": f"{token['book']} {token['chapter']}:{token['verse']}",
        "morph_hint": morph_hint,
        "show_translation": bool(show_translation),
        "translation": translation if show_translation else "",
        "english_gloss": english,
    }


# ---------- Public: submit_answer ----------

def submit_answer(card_id: str, user_answer: str, user_id: int = 1):
    try:
        lemma, sentence_id_str, token_id_str = card_id.split("|")
        token_id = int(token_id_str)
    except ValueError:
        raise ValueError("Invalid card_id")

    conn = _get_conn()
    _ensure_schema(conn)
    cur = conn.cursor()

    cur.execute(
        "SELECT COALESCE(surface, form) FROM tokens WHERE id = ?",
        (token_id,),
    )
    row = cur.fetchone()
    if not row or not row[0]:
        conn.close()
        raise ValueError("Token not found for this card_id")
    expected = row[0]

    ua = (user_answer or "").strip().lower()
    exp = expected.strip().lower()
    correct = (ua == exp)

    current_idx = _get_card_counter(cur, user_id)

    # Fetch or create lemma row
    cur.execute(
        """
        SELECT id, level, total_reviews, correct_reviews
        FROM user_lemma
        WHERE user_id = ? AND lemma = ?
        """,
        (user_id, lemma),
    )
    r = cur.fetchone()

    if r is None:
        # Insert with legacy columns populated (due_date non-null)
        dummy_due = "1970-01-01"
        now = _now_iso()
        cur.execute(
            """
            INSERT INTO user_lemma
            (user_id, lemma,
             streak, interval_days, due_date,
             level, next_due_at_card,
             last_result, last_seen_at,
             total_reviews, correct_reviews)
            VALUES (?, ?, 0, 0, ?, 1, NULL, ?, ?, 0, 0)
            """,
            (user_id, lemma, dummy_due, "init", now),
        )
        cur.execute(
            """
            SELECT id, level, total_reviews, correct_reviews
            FROM user_lemma
            WHERE user_id = ? AND lemma = ?
            """,
            (user_id, lemma),
        )
        r = cur.fetchone()

    ule_id, level, total_reviews, correct_reviews = r

    if correct:
        if level < 5:
            level += 1
        correct_reviews += 1
        last_result = "correct"
    else:
        level = max(1, level - 1)
        last_result = "wrong"

    total_reviews += 1
    gap = _level_interval_cards(level)
    next_due = current_idx + gap

    # Maintain due_date with some non-null (legacy compatibility)
    dummy_due = "1970-01-01"

    cur.execute(
        """
        UPDATE user_lemma
        SET level = ?, next_due_at_card = ?,
            last_result = ?, last_seen_at = ?,
            total_reviews = ?, correct_reviews = ?,
            due_date = COALESCE(due_date, ?)
        WHERE id = ?
        """,
        (
            level,
            next_due,
            last_result,
            _now_iso(),
            total_reviews,
            correct_reviews,
            dummy_due,
            ule_id,
        ),
    )

    _increment_card_counter(cur, user_id, 1)

    conn.commit()
    conn.close()

    return {
        "correct": correct,
        "expected": expected,
        "lemma": lemma,
        "level": level,
        "next_due_card_index": next_due,
    }


# ---------- CLI sanity ----------

if __name__ == "__main__":
    card = get_next_card(user_id=1)
    if not card:
        print("No card available.")
    else:
        print(f"[{card['reference']}] {card['cloze']}")
        print(f"Target English (missing word): {card['english_gloss'] or '[none]'}")
        if card["morph_hint"]:
            print(f"Hint: {card['morph_hint']}")
        if card["translation"]:
            print(f"Full EN: {card['translation']}")
        ans = input("Your answer: ").strip()
        result = submit_answer(card["card_id"], ans, user_id=1)
        print(result)
