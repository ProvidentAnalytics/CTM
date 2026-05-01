"""
transcribe_backfill.py
Bulk-transcribes CTM calls that have a recording but no transcript.
Saves results to data_backfill_transcripts.json, keyed by call ID.
Resumable: re-running skips already-completed calls.

Usage:
    DRY_RUN=1 python transcribe_backfill.py   # count + cost estimate, no API spend
    python transcribe_backfill.py              # full run
"""

import json, os, time, tempfile, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
AUTH         = os.environ.get("CTM_AUTH", "")
OPENAI_KEY   = os.environ.get("OPENAI_API_KEY", "")
BASE         = "https://api.calltrackingmetrics.com/api/v1/accounts/559323"
OUT_FILE     = "data_backfill_transcripts.json"
WORKERS      = 3
DRY_RUN      = os.environ.get("DRY_RUN", "0") == "1"

# Fetch all calls across all pages (no date filter — we want historical)
def ctm_fetch(path, params=""):
    url = f"{BASE}{path}?format=json&page_size=100&{params}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Basic {AUTH}",
        "User-Agent": "CTM-Backfill/1.0"
    })
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def pull_all_calls():
    print("Fetching call list from CTM (all pages)...")
    d = ctm_fetch("/calls", "page=1")
    calls = list(d.get("calls", []))
    total = d.get("total_pages", 1)
    for page in range(2, total + 1):
        for attempt in range(3):
            try:
                d2 = ctm_fetch("/calls", f"page={page}")
                calls.extend(d2.get("calls", []))
                break
            except Exception as e:
                if attempt == 2: print(f"  Page {page} failed: {e}")
                time.sleep(3)
        time.sleep(0.1)
        if page % 20 == 0:
            print(f"  Fetched page {page}/{total} ({len(calls)} calls so far)...")
    return calls

def needs_transcript(c):
    has_audio = bool(c.get("audio") or c.get("recording_url") or c.get("record_url"))
    has_trans = bool((c.get("transcription_text") or "").strip())
    return has_audio and not has_trans and c.get("direction") == "inbound"

def get_audio_url(c):
    return c.get("audio") or c.get("recording_url") or c.get("record_url") or ""

_debug_done = False  # print debug info for first call only

def whisper_transcribe(audio_url, call_id):
    """Download audio from CTM then transcribe via OpenAI Whisper SDK."""
    global _debug_done
    from openai import OpenAI

    # Download audio — no auth header (CTM audio URLs are pre-signed S3 links)
    req = urllib.request.Request(audio_url, headers={"User-Agent": "CTM-Backfill/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        audio_bytes = r.read()
        content_type = r.headers.get("Content-Type", "unknown")

    if not _debug_done:
        print(f"  DEBUG call {call_id}:")
        print(f"    audio_url    : {audio_url[:80]}")
        print(f"    content-type : {content_type}")
        print(f"    size (bytes) : {len(audio_bytes)}")
        print(f"    first 16 hex : {audio_bytes[:16].hex()}")
        _debug_done = True

    if len(audio_bytes) < 100:
        raise ValueError(f"Audio too small ({len(audio_bytes)} bytes) — likely empty or auth-gated")

    # Detect suffix from URL, fall back to content-type
    url_clean = audio_url.lower().split("?")[0]
    suffix = next((s for s in (".mp3", ".mp4", ".wav", ".m4a", ".ogg", ".webm") if url_clean.endswith(s)), None)
    if not suffix:
        if "wav"  in content_type: suffix = ".wav"
        elif "mp4" in content_type: suffix = ".mp4"
        elif "ogg" in content_type: suffix = ".ogg"
        else:                        suffix = ".mp3"

    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(audio_bytes)
        tmp_path = tmp.name

    try:
        client = OpenAI(api_key=OPENAI_KEY)
        for attempt in range(5):
            try:
                with open(tmp_path, "rb") as f:
                    result = client.audio.transcriptions.create(
                        model="whisper-1",
                        file=f,
                        language="en"
                    )
                return result.text.strip()
            except Exception as e:
                err_msg = str(e)
                if not _debug_done or attempt == 0:
                    print(f"  DEBUG OpenAI error: {err_msg}")
                if "429" in err_msg and attempt < 4:
                    wait = 20 * (2 ** attempt)
                    time.sleep(wait)
                else:
                    raise
    finally:
        os.unlink(tmp_path)

def process_one(c, existing):
    call_id = str(c.get("id", ""))
    if call_id in existing:
        return call_id, None, "skip"

    audio_url = get_audio_url(c)
    try:
        text = whisper_transcribe(audio_url, call_id)
        dur  = c.get("duration", 0) or 0
        entry = {
            "id":       call_id,
            "dt":       c.get("called_at", "")[:16],
            "duration": dur,
            "agent":    (c.get("agent") or {}).get("name", ""),
            "caller":   c.get("caller_number_format", ""),
            "name":     ((c.get("name") or c.get("cnam") or "") or "")[:30],
            "source":   c.get("source", ""),
            "audio":    audio_url,
            "transcript": text,
            "backfilled_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        return call_id, entry, "ok"
    except Exception as e:
        return call_id, None, f"error: {e}"

def main():
    if not AUTH:
        raise SystemExit("CTM_AUTH not set")
    if not OPENAI_KEY and not DRY_RUN:
        raise SystemExit("OPENAI_API_KEY not set (set DRY_RUN=1 to estimate only)")

    calls = pull_all_calls()
    targets = [c for c in calls if needs_transcript(c)]

    total_dur = sum(c.get("duration", 0) or 0 for c in targets)
    est_cost  = round(total_dur / 60 * 0.006, 2)
    print(f"\nCalls needing transcription : {len(targets):,}")
    print(f"Total audio duration        : {total_dur//60:,} min {total_dur%60}s")
    print(f"Estimated Whisper cost      : ~${est_cost}")

    if DRY_RUN:
        print("\nDRY RUN — no transcriptions sent. Set DRY_RUN=0 to run for real.")
        return

    # Load existing results (resumable)
    existing = {}
    if os.path.exists(OUT_FILE):
        with open(OUT_FILE) as f:
            existing = json.load(f)
        print(f"Resuming — {len(existing)} already done, {len(targets)-len(existing)} remaining")

    done = dict(existing)
    errors = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(process_one, c, done): c for c in targets}
        completed = 0
        for fut in as_completed(futures):
            call_id, entry, status = fut.result()
            completed += 1
            if status == "ok":
                done[call_id] = entry
            elif status != "skip":
                errors += 1
                print(f"  [{completed}/{len(targets)}] {call_id}: {status}")
            # Checkpoint every 50
            if completed % 50 == 0:
                with open(OUT_FILE, "w") as f:
                    json.dump(done, f, separators=(",", ":"))
                print(f"  [{completed}/{len(targets)}] checkpoint saved ({len(done)} transcribed, {errors} errors)")

    with open(OUT_FILE, "w") as f:
        json.dump(done, f, separators=(",", ":"))

    new_count = len(done) - len(existing)
    print(f"\nDone. {new_count} new transcripts added ({errors} errors).")
    print(f"Saved to {OUT_FILE} ({os.path.getsize(OUT_FILE)//1024} KB)")

if __name__ == "__main__":
    main()
