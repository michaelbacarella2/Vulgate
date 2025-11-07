from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from srs_engine import get_next_card, submit_answer

app = FastAPI(title="Latin Vulgate SRS API")

# Permissive for local dev: no credentials, any origin.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AnswerRequest(BaseModel):
    card_id: str
    answer: str
    user_id: int = 1


class CardResponse(BaseModel):
    card_id: str
    lemma: str
    cloze: str
    expected: str
    latin_text: str
    reference: str
    morph_hint: str
    show_translation: bool
    translation: str
    english_gloss: str


class AnswerResponse(BaseModel):
    correct: bool
    expected: str
    lemma: str
    level: int
    next_due_card_index: int


@app.get("/next-card", response_model=CardResponse)
def api_next_card(user_id: int = 1):
    card = get_next_card(user_id=user_id)
    if not card:
        raise HTTPException(status_code=404, detail="No card available")

    return CardResponse(
        card_id=card["card_id"],
        lemma=card["lemma"],
        cloze=card["cloze"],
        expected=card["expected"],
        latin_text=card.get("latin_text", ""),
        reference=card.get("reference", ""),
        morph_hint=card.get("morph_hint", "") or "",
        show_translation=bool(card.get("show_translation", False)),
        translation=card.get("translation", "") or "",
        english_gloss=card.get("english_gloss", "") or "",
    )


@app.post("/answer", response_model=AnswerResponse)
def api_answer(payload: AnswerRequest):
    try:
        result = submit_answer(
            card_id=payload.card_id,
            user_answer=payload.answer,
            user_id=payload.user_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return AnswerResponse(
        correct=bool(result["correct"]),
        expected=result["expected"],
        lemma=result["lemma"],
        level=int(result["level"]),
        next_due_card_index=int(result["next_due_card_index"]),
    )
