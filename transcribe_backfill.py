"""
transcribe_backfill.py
Bulk-transcribes CTM calls that have a recording but no transcript.
Saves results to data_backfill_transcripts.json, keyed by call ID.
Resumable: re-running skips already-completed calls.

Usage:
    DRY_RUN=1 python transcribe_backfill.py   # count + cost estimate, no API spend
    python transcribe_backfill.py              # full run
"""

import json, os, time, tempfile, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
AUTH         = os.environ.get("CTM_AUTH", "")
OPENAI_KEY   = os.environ.get("OPENAI_API_KEY", "")
BASE         = "https://api.calltrackingmetrics.com/api/v1/accounts/559323"
OUT_FILE     = "data_backfill_transcripts.json"
WORKERS      = 10
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

def whisper_transcribe(audio_url, call_id):
    """Download audio then transcribe via OpenAI Whisper API."""
    # Download to temp file
    req = urllib.request.Request(audio_url, headers={"User-Agent": "CTM-Backfill/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        audio_bytes = r.read()

    suffix = ".mp3" if "mp3" in audio_url.lower() else ".wav"
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(audio_bytes)
        tmp_path = tmp.name

    try:
        import urllib.parse
        boundary = "----WBoundary"
        with open(tmp_path, "rb") as f:
            audio_data = f.read()

        # Build multipart form manually (no external deps)
        parts = []
        parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nwhisper-1\r\n".encode())
        parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"language\"\r\n\r\nen\r\n".encode())
        parts.append(
            f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"audio{suffix}\"\r\n"
            f"Content-Type: audio/mpeg\r\n\r\n".encode() + audio_data + b"\r\n"
        )
        parts.append(f"--{boundary}--\r\n".encode())
        body = b"".join(parts)

        req2 = urllib.request.Request(
            "https://api.openai.com/v1/audio/transcriptions",
            data=body,
            headers={
                "Authorization": f"Bearer {OPENAI_KEY}",
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "User-Agent": "CTM-Backfill/1.0"
            }
        )
        with urllib.request.urlopen(req2, timeout=120) as r2:
            result = json.loads(r2.read())
        return result.get("text", "").strip()
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
