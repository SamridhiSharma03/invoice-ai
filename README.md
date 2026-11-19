# Aravali AP: invoice automation (PS-1)
AI extracts (Gemini), deterministic Python decides, every stage is visible live.

## Run locally
    pip install -r requirements.txt
    export GEMINI_API_KEY=your_key      # optional: only needed for uploaded PDFs
    uvicorn app:app --reload
Open http://localhost:8000 and click "Run all in order".

## Deploy (one service, no Netlify needed)
Render > New Web Service > this repo. Build: `pip install -r requirements.txt`. Start: `uvicorn app:app --host 0.0.0.0 --port $PORT`.
Env vars: GEMINI_API_KEY, optional GEMINI_MODEL, GEMINI_FALLBACK_MODEL. Free tier sleeps when idle and SQLite resets on restart: open it 5 minutes before the demo.

## Rules (editable on the Policy page)
Tolerance vs PO remaining balance (2% or ₹500 auto-OK, to 5% review, above rejects), high value over ₹10,00,000 needs sign-off, blocked vendor rejects, duplicate (same file or vendor + invoice no.) rejects, missing fields / invalid GSTIN / no PO go to review.

## Assumptions
Amounts in whole rupees. Sample scenarios use pre-extracted fields so the demo needs no API; uploads use Gemini. Single user, no login. Decisions depend on run order (split POs, duplicates), which is correct AP behaviour.
