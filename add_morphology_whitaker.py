import sqlite3
from whitakers_words.parser import Parser

DB_FILE = "vulgate_latlearn.db"

parser = Parser()


def get_existing_columns(cur, table):
    cur.execute(f"PRAGMA table_info({table});")
    return {row[1] for row in cur.fetchall()}


def ensure_schema(conn):
    cur = conn.cursor()
    existing = get_existing_columns(cur, "tokens")

    needed = [
        ("lemma", "TEXT"),
        ("pos", "TEXT"),
        ("morph", "TEXT"),
        ("morph_hint", "TEXT"),
    ]

    for name, ctype in needed:
        if name not in existing:
            cur.execute(f"ALTER TABLE tokens ADD COLUMN {name} {ctype};")

    conn.commit()


def build_hint_from_morph_desc(desc: str) -> str:
    if not desc:
        return ""

    d = desc.upper().replace(",", " ")
    parts = d.split()

    if "PARTICIPLE" in parts or "PPL" in parts:
        return "part"

    case_map = {
        "NOM": "nom",
        "GEN": "gen",
        "DAT": "dat",
        "ACC": "acc",
        "ABL": "abl",
        "VOC": "voc",
    }
    num_map = {
        "S": "sg",
        "SING": "sg",
        "PL": "pl",
        "PLUR": "pl",
    }
    g_map = {
        "M": "m",
        "MASC": "m",
        "F": "f",
        "FEM": "f",
        "N": "n",
        "NEUT": "n",
    }
    tense_map = {
        "PRES": "pres",
        "IMPF": "impf",
        "IMP": "impf",
        "FUT": "fut",
        "PERF": "perf",
        "PLUP": "plup",
        "FUTP": "futperf",
    }
    mood_map = {
        "IND": "ind",
        "SUBJ": "subj",
        "SUB": "subj",
        "IMP": "imp",
    }
    voice_map = {
        "ACT": "act",
        "PASS": "pass",
    }

    is_verb = "V" in parts or any(p in parts for p in tense_map)

    if is_verb:
        person = ""
        for p in ("1", "2", "3"):
            if p in parts:
                person = p
                break

        number = ""
        for k, v in num_map.items():
            if k in parts:
                number = v
                break

        tense = ""
        for k, v in tense_map.items():
            if k in parts:
                tense = v
                break

        voice = ""
        for k, v in voice_map.items():
            if k in parts:
                voice = v
                break

        mood = ""
        for k, v in mood_map.items():
            if k in parts:
                mood = v
                break

        bits = []
        if person:
            bits.append(person)
        if number:
            bits.append(number)
        if tense:
            bits.append(tense)
        if voice:
            bits.append(voice)
        if mood in ("subj", "imp"):
            bits.append(mood)

        return " ".join(bits).strip()

    gender = ""
    for k, v in g_map.items():
        if k in parts:
            gender = v
            break

    case = ""
    for k, v in case_map.items():
        if k in parts:
            case = v
            break

    number = ""
    for k, v in num_map.items():
        if k in parts:
            number = v
            break

    bits = []
    if gender:
        bits.append(gender)
    if case:
        bits.append(case)
    if number:
        bits.append(number)

    return " ".join(bits).strip()


def normalize_form(s: str) -> str:
    return "".join(ch for ch in s.lower() if ch.isalpha())


def pick_analysis(result):
    """
    Robustly extract (lemma, pos, morph_desc) from whitakers_words parse() output.
    Never assumes integer keys.
    """
    if not result:
        return None, None, None

    # Some versions return an object with .forms; others a dict/iterable
    forms = getattr(result, "forms", None)

    if forms is None:
        # If it's dict-like
        if isinstance(result, dict):
            if not result:
                return None, None, None
            forms = next(iter(result.values()))
        elif isinstance(result, (list, tuple, set)):
            if not result:
                return None, None, None
            forms = result
        else:
            return None, None, None

    # If dict of forms
    if isinstance(forms, dict):
        if not forms:
            return None, None, None
        f0 = next(iter(forms.values()))
    elif isinstance(forms, (list, tuple, set)):
        if not forms:
            return None, None, None
        f0 = next(iter(forms))
    else:
        f0 = forms

    if f0 is None:
        return None, None, None

    analyses = getattr(f0, "analyses", None)
    if not analyses:
        return None, None, None

    # analyses may be list/tuple/dict; take a deterministic first
    if isinstance(analyses, dict):
        if not analyses:
            return None, None, None
        a0 = next(iter(analyses.values()))
    elif isinstance(analyses, (list, tuple, set)):
        if not analyses:
            return None, None, None
        a0 = next(iter(analyses))
    else:
        a0 = analyses

    if a0 is None:
        return None, None, None

    lemma = None
    pos = None
    morph_desc = None

    lexeme = getattr(a0, "lexeme", None)
    if lexeme is not None:
        lemma = str(lexeme)
        if hasattr(lexeme, "pos"):
            pos = str(lexeme.pos)

    infls = getattr(a0, "inflections", None)
    if infls:
        if isinstance(infls, dict):
            inf0 = next(iter(infls.values()))
        elif isinstance(infls, (list, tuple, set)):
            inf0 = next(iter(infls))
        else:
            inf0 = infls
        if hasattr(inf0, "description"):
            morph_desc = str(inf0.description)
        else:
            morph_desc = str(inf0)

    return lemma, pos, morph_desc


def main():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    ensure_schema(conn)

    cur.execute("SELECT id, surface FROM tokens ORDER BY id")
    rows = cur.fetchall()
    total = len(rows)
    print(f"Annotating {total} tokens with Whitaker...")

    batch = []
    done = 0
    batch_size = 500

    for tok_id, surface in rows:
        if not surface:
            continue

        raw = surface.strip()
        if not raw:
            continue

        # Try raw first
        try:
            result = parser.parse(raw)
        except Exception:
            result = None

        # If nothing, try normalized
        if (not result):
            nf = normalize_form(raw)
            if nf:
                try:
                    result = parser.parse(nf)
                except Exception:
                    result = None

        lemma, pos, morph_desc = pick_analysis(result)

        morph_hint = build_hint_from_morph_desc(morph_desc or "")

        batch.append((lemma, pos, morph_desc, morph_hint, tok_id))

        if len(batch) >= batch_size:
            cur.executemany(
                "UPDATE tokens SET lemma = ?, pos = ?, morph = ?, morph_hint = ? WHERE id = ?",
                batch,
            )
            conn.commit()
            done += len(batch)
            print(f"{done} / {total} tokens updated")
            batch = []

    if batch:
        cur.executemany(
            "UPDATE tokens SET lemma = ?, pos = ?, morph = ?, morph_hint = ? WHERE id = ?",
            batch,
        )
        conn.commit()

    conn.close()
    print("Done adding Whitaker-based morphology.")


if __name__ == "__main__":
    main()
