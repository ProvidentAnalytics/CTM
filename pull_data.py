import urllib.request, json, time, os
from datetime import datetime

AUTH = os.environ.get("CTM_AUTH", "")
BASE = "https://api.calltrackingmetrics.com/api/v1/accounts/559323"
START = "2026-01-01"
END   = datetime.now().strftime("%Y-%m-%d")

def fetch(path, params=""):
    url = f"{BASE}{path}?format=json&page_size=100&{params}"
    req = urllib.request.Request(url, headers={"Authorization": f"Basic {AUTH}", "User-Agent": "Strive/2.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def pull_all(direction=""):
    dir_param = f"&direction={direction}" if direction else ""
    d = fetch("/calls", f"start_date={START}&end_date={END}&page=1{dir_param}")
    items = list(d.get("calls", []))
    for page in range(2, d["total_pages"]+1):
        for attempt in range(3):
            try:
                d2 = fetch("/calls", f"start_date={START}&end_date={END}&page={page}{dir_param}")
                items.extend(d2.get("calls", []))
                break
            except:
                time.sleep(3)
        time.sleep(0.1)
    return items

print(f"Pulling calls {START} to {END}...")
calls = pull_all()
print(f"Got {len(calls)} calls")
with open("ctm_calls_raw.json", "w") as f:
    json.dump(calls, f, separators=(",",":"))

print("Pulling forms...")
forms = pull_all("form")
print(f"Got {len(forms)} forms")

# Process forms
def proc_form(f):
    tn = f.get("tracking_number_bare","") or ""
    fa = "Fort Wayne" if tn.startswith("260") else "Waterloo"
    fd = f.get("form",{}) or {}
    hw = next((c.get("value","") for c in (fd.get("custom",[]) or []) if c.get("id")=="how_can_we_help"), "")
    return {"dt":f["called_at"][:16],"n":(f.get("name","") or "")[:40],"e":f.get("email","") or "",
            "ph":f.get("caller_number_format","") or "","ci":f.get("city","") or "","st":f.get("state","") or "",
            "sr":f.get("source","") or "","fa":fa,"fn":(fd.get("form_name","") or ""),
            "nw":bool(f.get("is_new_caller",False)),"hw":(hw or "").strip()}

with open("ctm_forms_all_final.json","w") as f:
    json.dump([proc_form(x) for x in forms], f, separators=(",",":"))
print("Done.")
