import urllib.request, json, os
from datetime import datetime, timezone

TOKEN = os.environ["GH_TOKEN"]
OWNER = "ProvidentAnalytics"
REPO  = "Strive"
headers = {"Authorization":f"token {TOKEN}","Accept":"application/vnd.github.v3+json","User-Agent":"Strive/2.0","Content-Type":"application/json"}

def api(method, path, payload=None):
    url = f"https://api.github.com/repos/{OWNER}/{REPO}{path}"
    data = json.dumps(payload).encode() if payload else None
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())

ref       = api("GET","/git/refs/heads/main")
latest    = ref["object"]["sha"]
base_tree = api("GET",f"/git/commits/{latest}")["tree"]["sha"]

with open("strive_ctm_clean.html") as f: dashboard = f.read()

now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
new_tree = api("POST","/git/trees",{"base_tree":base_tree,"tree":[{"path":"ctm-calls/index.html","mode":"100644","type":"blob","content":dashboard}]})
new_commit = api("POST","/git/commits",{"message":f"Auto-refresh: {now}","tree":new_tree["sha"],"parents":[latest]})
api("PATCH","/git/refs/heads/main",{"sha":new_commit["sha"]})
print(f"Pushed: {now}")
