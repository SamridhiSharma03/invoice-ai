"""Aravali Industries - AP invoice automation.
AI extracts (Gemini), deterministic Python decides. One file: API + pipeline + rules."""

from dotenv import load_dotenv
load_dotenv()

import os, json, re, time, uuid, hashlib, sqlite3, threading, datetime as dt
from difflib import SequenceMatcher
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

DB = os.getenv("DB_PATH", "invoices.db")
DELAY = float(os.getenv("STAGE_DELAY_S", "0.45"))
TODAY = dt.date.today()
LOCK = threading.Lock()
app = FastAPI(title="AP Invoice Automation")

def db():
    c = sqlite3.connect(DB, check_same_thread=False); c.row_factory = sqlite3.Row; return c

def inr(n):
    s = str(int(n))
    if len(s) > 3: s = re.sub(r"(\d)(?=(\d\d)+$)", r"\1,", s[:-3]) + "," + s[-3:]
    return "₹" + s

# ---------- master data (synthetic) ----------
CH = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
def gst_check(b):  # official GSTIN mod-36 checksum
    s = 0
    for i, ch in enumerate(b):
        v = CH.index(ch) * (1 if i % 2 == 0 else 2); s += v // 36 + v % 36
    return CH[(36 - s % 36) % 36]
def mkgst(b): return b + gst_check(b)
def gst_ok(g): return bool(g) and re.fullmatch(r"\d{2}[A-Z]{5}\d{4}[A-Z][1-9A-Z]Z[0-9A-Z]", g) is not None and gst_check(g[:14]) == g[14]

VENDORS = {"V1": ("Shree Ganesh Steel Pvt Ltd", mkgst("07AAACS1234A1Z"), "ok"),
           "V2": ("Kaveri Polymers Limited", mkgst("27AABCK5678B1Z"), "ok"),
           "V3": ("Narmada Engineering LLP", mkgst("24AAFFN9012C1Z"), "ok"),
           "V4": ("Apex Trading Co", mkgst("06AAGPA3456D1Z"), "blocked")}
POS = {"PO-1001": ("V1", 500000), "PO-1002": ("V1", 500000), "PO-1003": ("V1", 500000),
       "PO-0112": ("V2", 1200000), "PO-1004": ("V3", 1400000), "PO-1007": ("V2", 80000),
       "PO-1008": ("V2", 60000), "PO-1009": ("V3", 250000)}
DEFAULT_POLICY = dict(ok_pct=2.0, ok_abs=500, review_pct=5.0, high_value=1000000, stale_days=180,
                      unknown_vendor="REVIEW", no_po="REVIEW")

def d(n): return (TODAY - dt.timedelta(days=n)).isoformat()
def mk(v, inv, po, total, date=None, gst=None, name=None):
    n, g, _ = VENDORS[v]; sub = round(total / 1.18)
    return dict(vendor_name=name or n, vendor_gstin=gst or g, invoice_number=inv, invoice_date=date or d(5),
                po_reference=po, currency="INR", subtotal=sub, tax_total=total - sub, total=total)
G2 = VENDORS["V2"][1]
SAMPLES = [  # (id, label, expected decision under default policy, pre-extracted fields)
 ("s01", "Clean invoice, exact PO match", "APPROVE", mk("V1", "SGS/26/0451", "PO-1001", 500000)),
 ("s02", "Scanned copy, invoice number unreadable", "REVIEW", mk("V2", None, "PO-1007", 80000)),
 ("s03", "Split PO, delivery 1 of 3", "APPROVE", mk("V2", "KP/1101", "PO-0112", 400000)),
 ("s04", "Split PO, delivery 2 of 3", "APPROVE", mk("V2", "KP/1102", "PO-0112", 400000)),
 ("s05", "Split PO, delivery 3 exceeds balance", "REJECT", mk("V2", "KP/1103", "PO-0112", 500000)),
 ("s06", "Re-issued duplicate of s01", "REJECT", mk("V1", "SGS/26/0451", "PO-1001", 500000, d(3), name="Shree Ganesh Steel Private Limited")),
 ("s07", "1.4% over PO, inside tolerance", "APPROVE", mk("V1", "SGS/26/0460", "PO-1002", 507000)),
 ("s08", "3.5% over PO, needs review", "REVIEW", mk("V1", "SGS/26/0461", "PO-1003", 517500)),
 ("s09", "Blocked vendor", "REJECT", mk("V4", "APX/77", None, 90000)),
 ("s10", "High value, manager sign-off", "REVIEW", mk("V3", "NE/26/88", "PO-1004", 1400000)),
 ("s11", "GSTIN fails checksum", "REVIEW", mk("V2", "KP/1120", "PO-1008", 60000, gst=G2[:-1] + ("A" if G2[-1] != "A" else "B"))),
 ("s12", "No PO reference, inferred from amount", "APPROVE", mk("V3", "NE/26/91", None, 250000)),
]

def init():
    with LOCK, db() as c:
        c.executescript("""create table if not exists policy(version integer primary key autoincrement,cfg text,note text,created real);
        create table if not exists runs(id text primary key,name text,hash text,status text,decision text,final text,ex text,reasons text,
          trace text,explanation text,message text,po text,vid text,policy_version integer,created real,finished real);
        create table if not exists events(id integer primary key autoincrement,run_id text,stage text,status text,msg text,data text,ts real);""")
        if not c.execute("select 1 from policy").fetchone():
            c.execute("insert into policy(cfg,note,created) values(?,?,?)", (json.dumps(DEFAULT_POLICY), "Factory default", time.time()))
init()

def policy():
    r = db().execute("select * from policy order by version desc limit 1").fetchone()
    return json.loads(r["cfg"]), r["version"]

def emit(rid, stage, status, msg, data=None):
    with LOCK, db() as c:
        c.execute("insert into events(run_id,stage,status,msg,data,ts) values(?,?,?,?,?,?)",
                  (rid, stage, status, msg, json.dumps(data or {}), time.time()))

# ---------- deterministic rules ----------
def norm(s):
    s = re.sub(r"\bprivate\b", "pvt", s.lower()); s = re.sub(r"\blimited\b", "ltd", s)
    return re.sub(r"[^a-z0-9 ]", "", s).strip()

def vendor_match(name):
    if not name: return None, "unknown", 0
    vid, sc = max(((k, SequenceMatcher(None, norm(name), norm(v[0])).ratio()) for k, v in VENDORS.items()), key=lambda x: x[1])
    return vid, ("known" if sc >= .92 else "ambiguous" if sc >= .8 else "unknown"), round(sc, 2)

def remaining(po, prior):
    used = sum((p["ex"].get("total") or 0) for p in prior if p["final"] == "APPROVE" and p["po"] == po)
    return POS[po][1] - used

def decide(ex, P, prior, h):
    R, T = [], []
    def chk(stage, rule, ok, detail, code=None, sev="REVIEW"):
        T.append(dict(stage=stage, rule=rule, passed=ok, detail=detail))
        if not ok: R.append(dict(code=code or rule, severity=sev, detail=detail))
    miss = [f for f in ("vendor_name", "invoice_number", "invoice_date", "total") if not ex.get(f)]
    chk("Completeness", "Required fields", not miss, "Missing: " + ", ".join(miss) if miss else "All required fields present", "MISSING_FIELD")
    sub, tax, tot = ex.get("subtotal"), ex.get("tax_total"), ex.get("total")
    chk("Completeness", "Arithmetic", None in (sub, tax, tot) or abs(sub + tax - tot) <= 1,
        "Subtotal + tax does not equal total" if None not in (sub, tax, tot) and abs(sub + tax - tot) > 1 else "Subtotal + tax = total", "ARITHMETIC_MISMATCH")
    vid, vs, sc = vendor_match(ex.get("vendor_name"))
    if vid and vs != "unknown" and VENDORS[vid][2] == "blocked":
        chk("Vendor check", "Vendor master", False, f"{VENDORS[vid][0]} is a blocked vendor", "BLOCKED_VENDOR", "REJECT")
    elif vs == "known": chk("Vendor check", "Vendor master", True, f"Matched approved vendor {VENDORS[vid][0]} (score {sc})")
    else: chk("Vendor check", "Vendor master", False, f"Vendor {'unknown' if vs == 'unknown' else 'ambiguous'} (best score {sc})",
              "UNKNOWN_VENDOR" if vs == "unknown" else "AMBIGUOUS_VENDOR", P["unknown_vendor"] if vs == "unknown" else "REVIEW")
    g = ex.get("vendor_gstin")
    if not gst_ok(g): chk("Vendor check", "GSTIN", False, f"GSTIN {g or '(missing)'} is invalid (format/checksum)", "INVALID_GSTIN")
    elif vid and g != VENDORS[vid][1]: chk("Vendor check", "GSTIN", False, "GSTIN differs from the vendor master", "GSTIN_MISMATCH")
    else: chk("Vendor check", "GSTIN", True, "GSTIN valid and matches vendor master")
    po = ex.get("po_reference"); how = "explicit"
    if po and (po not in POS or (vid and POS[po][0] != vid)):
        chk("PO match", "PO", False, f"PO {po} does not exist or belongs to another vendor", "PO_INVALID"); po = None
    elif not po:
        c = [p for p, (v, t) in POS.items() if v == vid and tot and abs(remaining(p, prior) - tot) <= tot * .01]
        if len(c) == 1: po, how = c[0], "inferred"; chk("PO match", "PO", True, f"No PO on invoice; inferred {po} from vendor + amount")
        else: chk("PO match", "PO", False, "No PO reference and no unique match by vendor + amount", "NO_PO", P["no_po"])
    else: chk("PO match", "PO", True, f"Explicit PO {po} belongs to this vendor")
    if po and tot:
        rem = remaining(po, prior); over = tot - rem
        if over <= 0: chk("Rules engine", "Amount vs PO balance", True, f"{inr(tot)} within PO remaining {inr(rem)}")
        elif over <= max(rem * P["ok_pct"] / 100, P["ok_abs"]): chk("Rules engine", "Amount vs PO balance", True, f"{inr(over)} over remaining {inr(rem)}, inside tolerance")
        elif over <= rem * P["review_pct"] / 100: chk("Rules engine", "Amount vs PO balance", False, f"{over / rem * 100:.1f}% over PO remaining {inr(rem)}, above the {P['ok_pct']}% tolerance", "PRICE_VARIANCE")
        else: chk("Rules engine", "Amount vs PO balance", False, f"Exceeds PO remaining {inr(rem)} by {inr(over)}", "EXCEEDS_PO", "REJECT")
    dup = next((p for p in prior if p["hash"] == h or (p["vid"] == vid and vid and ex.get("invoice_number") and p["ex"].get("invoice_number") == ex["invoice_number"])), None)
    chk("Rules engine", "Duplicate check", dup is None, "Already processed (same file or vendor + invoice number)" if dup else "No duplicate found", "DUPLICATE", "REJECT")
    try:
        age = (TODAY - dt.date.fromisoformat(ex["invoice_date"])).days
        chk("Rules engine", "Invoice date", 0 <= age <= P["stale_days"], f"Invoice date is {'in the future' if age < 0 else str(age) + ' days old'}" if not 0 <= age <= P["stale_days"] else f"Dated {age} days ago", "INVALID_DATE")
    except Exception: pass
    chk("Rules engine", "High-value check", not (tot and tot > P["high_value"]), f"{inr(tot or 0)} exceeds {inr(P['high_value'])}: manager sign-off needed" if tot and tot > P["high_value"] else "Below high-value threshold", "HIGH_VALUE")
    dec = "REJECT" if any(r["severity"] == "REJECT" for r in R) else "REVIEW" if R else "APPROVE"
    return dec, R, T, po, vid

def explain(dec, R, ex):
    v, no = ex.get("vendor_name") or "the vendor", ex.get("invoice_number") or "(no number)"
    if not R: return f"Invoice {no} from {v} for {inr(ex.get('total') or 0)} passed every check against the vendor master, PO and policy. Approved for payment.", ""
    why = "; ".join(r["detail"] for r in R)
    head = "Rejected" if dec == "REJECT" else "Held for human review"
    return f"{head}. Reasons: {why}.", f"Hello {v} team,\n\nWe could not process invoice {no} because: {why}.\nPlease reply with a corrected invoice or the missing details.\n\nAccounts Payable, Aravali Industries Pvt. Ltd."

# ---------- Gemini extraction (only for uploaded PDFs) ----------
def num(x):
    try: return float(str(x).replace(",", "").replace("₹", ""))
    except Exception: return None

def gemini_extract(pdf):
    key = os.getenv("GEMINI_API_KEY")
    if not key: raise RuntimeError("GEMINI_API_KEY not set; only the built-in samples can run")
    from google import genai; from google.genai import types
    cl = genai.Client(api_key=key); last = None
    prompt = ("Extract this Indian GST invoice as JSON with keys: vendor_name, vendor_gstin, invoice_number, invoice_date (YYYY-MM-DD), "
              "po_reference, currency, subtotal, tax_total, total (plain numbers in rupees). Use null for anything not visible. Never guess.")
    for m in (os.getenv("GEMINI_MODEL", "gemini-3.5-flash"), os.getenv("GEMINI_FALLBACK_MODEL", "gemini-3.5-flash-lite")):
        for a in range(2):
            try:
                r = cl.models.generate_content(model=m, contents=[types.Part.from_bytes(data=pdf, mime_type="application/pdf"), prompt],
                    config=types.GenerateContentConfig(response_mime_type="application/json", temperature=0))
                ex = json.loads(r.text)
                for k in ("subtotal", "tax_total", "total"): ex[k] = num(ex.get(k))
                return ex
            except Exception as e: last = e; time.sleep(2 * (a + 1))
    raise RuntimeError(f"Gemini extraction failed: {last}")

def load_prior(rid):
    rows = db().execute("select * from runs where id!=? and status='DONE' order by created", (rid,)).fetchall()
    return [dict(hash=r["hash"], final=r["final"], po=r["po"], vid=r["vid"], ex=json.loads(r["ex"])) for r in rows]

def pipeline(rid, name, h, ex, pdf):
    dec = "REVIEW"; R = []; T = []; po = vid = None; P, ver = policy()
    try:
        emit(rid, "Intake", "done", f"Saved {name} (SHA-256 {h[:10]}...), policy v{ver} locked for this run"); time.sleep(DELAY)
        emit(rid, "Extract", "running", "Reading document..."); time.sleep(DELAY)
        if ex is None: ex = gemini_extract(pdf)
        emit(rid, "Extract", "done", f"Extracted {sum(v is not None for v in ex.values())} of {len(ex)} fields", {"extracted": ex})
        dec, R, T, po, vid = decide(ex, P, load_prior(rid), h)
        for st in ("Completeness", "Vendor check", "PO match", "Rules engine"):
            emit(rid, st, "running", "Checking..."); time.sleep(DELAY)
            items = [t for t in T if t["stage"] == st]; bad = [t for t in items if not t["passed"]]
            emit(rid, st, "warning" if bad else "done", "; ".join(t["detail"] for t in (bad or items))[:300], {"checks": items})
        emit(rid, "Decision", "done", dec, {"reasons": R}); time.sleep(DELAY)
        expl, msg = explain(dec, R, ex); emit(rid, "Explain", "done", "Explanation and vendor message drafted")
    except Exception as e:
        ex = ex or {}; dec = "REVIEW"; R = [dict(code="EXTRACTION_FAILED", severity="REVIEW", detail=str(e))]
        emit(rid, "Extract", "failed", str(e)); expl, msg = f"Held for human review. Extraction failed: {e}", ""
    with LOCK, db() as c:
        c.execute("update runs set status='DONE',decision=?,final=?,ex=?,reasons=?,trace=?,explanation=?,message=?,po=?,vid=?,policy_version=?,finished=? where id=?",
                  (dec, dec, json.dumps(ex), json.dumps(R), json.dumps(T), expl, msg, po, vid, ver, time.time(), rid))

def start(name, h, ex, pdf):
    rid = uuid.uuid4().hex[:8]
    with LOCK, db() as c: c.execute("insert into runs(id,name,hash,status,ex,created) values(?,?,?,?,?,?)", (rid, name, h, "RUNNING", "{}", time.time()))
    threading.Thread(target=pipeline, args=(rid, name, h, ex, pdf), daemon=True).start(); return {"id": rid}

# ---------- API ----------
@app.get("/api/health")
def health(): return {"ok": True, "gemini": bool(os.getenv("GEMINI_API_KEY"))}

@app.get("/api/samples")
def samples(): return [dict(id=s[0], label=s[1], expected=s[2], vendor=s[3]["vendor_name"], total=s[3]["total"]) for s in SAMPLES]

@app.post("/api/runs/sample/{sid}")
def run_sample(sid: str):
    s = next((x for x in SAMPLES if x[0] == sid), None)
    if not s: raise HTTPException(404, "Unknown sample")
    return start(s[1], hashlib.sha256((sid + json.dumps(s[3], sort_keys=True)).encode()).hexdigest(), dict(s[3]), None)

@app.post("/api/runs")
async def run_upload(file: UploadFile = File(...)):
    b = await file.read()
    if not b.startswith(b"%PDF"): raise HTTPException(400, "Please upload a PDF file")
    if len(b) > 10_000_000: raise HTTPException(400, "File is larger than 10 MB")
    return start(file.filename, hashlib.sha256(b).hexdigest(), None, b)

def row(r):
    o = dict(r)
    for k in ("ex", "reasons", "trace"): o[k] = json.loads(o[k]) if o.get(k) else ([] if k != "ex" else {})
    return o

@app.get("/api/runs")
def runs():
    out = [row(r) for r in db().execute("select * from runs order by created desc").fetchall()]
    for o in out: o.pop("trace", None)
    return out

@app.get("/api/runs/{rid}")
def run_detail(rid: str):
    r = db().execute("select * from runs where id=?", (rid,)).fetchone()
    if not r: raise HTTPException(404, "Run not found")
    o = row(r); o["events"] = [dict(e) for e in db().execute("select * from events where run_id=? order by id", (rid,)).fetchall()]
    return o

@app.get("/api/runs/{rid}/stream")
def stream(rid: str):
    def gen():
        last = 0
        while True:
            st = db().execute("select status from runs where id=?", (rid,)).fetchone()
            ev = db().execute("select * from events where run_id=? and id>? order by id", (rid, last)).fetchall()
            for e in ev: last = e["id"]; yield f"data: {json.dumps(dict(e))}\n\n"
            if (not st or st["status"] == "DONE") and not ev: yield "event: end\ndata: {}\n\n"; return
            time.sleep(.25)
    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.post("/api/runs/{rid}/review")
def review(rid: str, body: dict):
    a, note = body.get("action"), (body.get("note") or "").strip()
    if a not in ("APPROVE", "REJECT") or not note: raise HTTPException(400, "Choose approve or reject and add a note")
    with LOCK, db() as c: c.execute("update runs set final=? where id=?", (a, rid))
    emit(rid, "Human review", "done", f"{a} by demo_user: {note}"); return {"final": a}

@app.post("/api/runs/{rid}/reevaluate")
def reevaluate(rid: str):
    r = row(db().execute("select * from runs where id=?", (rid,)).fetchone()); P, ver = policy()
    dec, R, _, _, _ = decide(r["ex"], P, load_prior(rid), r["hash"])
    emit(rid, "Re-evaluation", "done", f"Under policy v{ver}: {dec} (originally {r['decision']} under v{r['policy_version']})")
    return {"before": r["decision"], "after": dec, "version": ver, "reasons": R}

@app.get("/api/policy")
def get_policy():
    p, v = policy(); return {"version": v, "policy": p}

@app.put("/api/policy")
def put_policy(body: dict):
    p = {k: body["policy"].get(k, DEFAULT_POLICY[k]) for k in DEFAULT_POLICY}
    try:
        for k in ("ok_pct", "ok_abs", "review_pct", "high_value", "stale_days"): p[k] = float(p[k]); assert p[k] >= 0
    except Exception: raise HTTPException(400, "All thresholds must be non-negative numbers")
    if p["ok_pct"] > p["review_pct"]: raise HTTPException(400, "Tolerance % cannot exceed the review limit %")
    if p["unknown_vendor"] not in ("REVIEW", "REJECT") or p["no_po"] not in ("REVIEW", "REJECT"): raise HTTPException(400, "Action must be REVIEW or REJECT")
    with LOCK, db() as c: c.execute("insert into policy(cfg,note,created) values(?,?,?)", (json.dumps(p), body.get("note") or "Edited in UI", time.time()))
    return get_policy()

@app.get("/api/pos")
def pos():
    prior = load_prior("-")
    return [dict(po=k, vendor=VENDORS[v][0], total=t, remaining=remaining(k, prior)) for k, (v, t) in POS.items()]

@app.post("/api/reset")
def reset():
    with LOCK, db() as c: c.execute("delete from runs"); c.execute("delete from events")
    return {"ok": True}

app.mount("/", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static"), html=True), name="ui")
