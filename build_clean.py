import json, os
from collections import defaultdict, Counter

def load(fname):
    with open(fname) as f: return f.read()

def build_extra_data():
    with open("ctm_calls_raw.json") as f:
        calls = json.load(f)

    agent_daily   = defaultdict(lambda: defaultdict(lambda: {"total":0,"inbound":0,"outbound":0,"answered":0,"missed":0,"dur":0,"dur_n":0}))
    returning_daily = Counter()
    nc_daily      = defaultdict(lambda: {"dow":Counter(),"hour":Counter(),"src":Counter()})
    hm_daily      = defaultdict(lambda: defaultdict(int))

    for c in calls:
        ag = (c.get("agent") or {}).get("name","")
        d  = c["called_at"][:10]
        if ag:
            s = agent_daily[ag][d]
            s["total"]+=1
            if c["direction"]=="inbound":  s["inbound"]+=1
            if c["direction"]=="outbound": s["outbound"]+=1
            st = c.get("dial_status","")
            dur = c.get("duration",0) or 0
            if st in ("answered","completed"):          s["answered"]+=1; s["dur"]+=dur; s["dur_n"]+=1
            elif st in ("no-answer","busy","no answer"): s["missed"]+=1
        if c["direction"]=="inbound" and not c.get("is_new_caller"):
            returning_daily[d]+=1
        if c.get("is_new_caller") and c["direction"]=="inbound":
            dd = nc_daily[d]
            dd["dow"][c.get("day","")]+=1; dd["hour"][c.get("hour","")]+=1; dd["src"][c.get("source","Unknown")]+=1
        if c["direction"]=="inbound":
            fac = "fw" if (c.get("tracking_number_bare","") or "").startswith("260") else "wl"
            day = c.get("day",""); hr = c.get("hour","")
            if day and hr:
                hm_daily[d][f"{day}|{hr}"]+=1
                hm_daily[d][f"_fac_{fac}_{day}|{hr}"]+=1

    sep = (",",":")
    with open("data_extra.json","w") as f:
        json.dump({"agent_daily":{ag:{d:dict(v) for d,v in days.items()} for ag,days in agent_daily.items()},"returning_daily":dict(returning_daily),"nc_daily_detail":{d:{"dow":dict(v["dow"]),"hour":dict(v["hour"]),"src":dict(v["src"])} for d,v in nc_daily.items()}}, f, separators=sep)
    with open("data_hm_daily.json","w") as f:
        json.dump({d:dict(v) for d,v in hm_daily.items()}, f, separators=sep)

def build():
    from process_data import process_all
    with open("ctm_calls_raw.json") as f:   calls = json.load(f)
    with open("ctm_forms_all_final.json") as f: forms = json.load(f)
    result = process_all(calls, forms)
    sep = (",",":")
    with open("data_split.json","w") as f:      json.dump(result["split"],       f, separators=sep)
    with open("data_daily_full.json","w") as f: json.dump(result["daily_full"],   f, separators=sep)
    with open("data_marketing.json","w") as f:  json.dump(result["marketing"],    f, separators=sep)
    with open("data_recordings.json","w") as f: json.dump(result["recordings"],   f, separators=sep)
    with open("data_log.json","w") as f:        json.dump(result["call_log"],     f, separators=sep)
    with open("data_forms.json","w") as f:      json.dump(result["forms"],        f, separators=sep)
    build_extra_data()

    with open("dashboard_template_v2.html") as f: html = f.read()
    for key in ["SD","DAILY","RECS","LOG","FORMS","MKT","EXTRA","HMD"]:
        fname = {"SD":"data_split.json","DAILY":"data_daily_full.json","RECS":"data_recordings.json","LOG":"data_log.json","FORMS":"data_forms.json","MKT":"data_marketing.json","EXTRA":"data_extra.json","HMD":"data_hm_daily.json"}[key]
        html = html.replace("/*INJECT_"+key+"*/null", load(fname), 1)

    with open("strive_ctm_clean.html","w") as f: f.write(html)
    print(f"Built: strive_ctm_clean.html — {len(html)//1024} KB")

if __name__ == "__main__":
    build()
