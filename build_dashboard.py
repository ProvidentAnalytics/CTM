#!/usr/bin/env python3
"""
Strive Recovery — CTM Dashboard Builder
Pulls fresh data from CTM API and rebuilds index.html
Runs nightly via GitHub Actions
"""

import urllib.request
import json
import time
import base64
import os
from collections import defaultdict, Counter
from datetime import datetime, timedelta

# ── Config ──────────────────────────────────────────────────────────────────
CTM_AUTH  = os.environ.get('CTM_AUTH', '')
CTM_ACCT  = '559323'
GH_TOKEN  = os.environ.get('GH_TOKEN', '')
GH_OWNER  = 'ProvidentAnalytics'
GH_REPO   = 'Strive'
GH_BRANCH = 'main'

BASE_URL  = f'https://api.calltrackingmetrics.com/api/v1/accounts/{CTM_ACCT}'

# Date range: Nov 1 2025 to today (full history)
END_DATE   = datetime.now().strftime('%Y-%m-%d')
START_DATE = '2025-11-01'

print(f"Building dashboard for {START_DATE} to {END_DATE}")

# ── CTM API helpers ──────────────────────────────────────────────────────────
def ctm_get(path, params=''):
    url = f'{BASE_URL}{path}?{params}'
    req = urllib.request.Request(url, headers={
        'Authorization': f'Basic {CTM_AUTH}',
        'User-Agent': 'StriveAnalytics/1.0'
    })
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def pull_all_calls():
    """Pull all calls for the date range."""
    all_calls = []
    # Get page count
    data = ctm_get('/calls', f'format=json&page_size=100&start_date={START_DATE}&end_date={END_DATE}&page=1')
    total_pages = data['total_pages']
    total_entries = data['total_entries']
    print(f"  Pulling {total_entries} calls across {total_pages} pages...")
    all_calls.extend(data.get('calls', []))

    for page in range(2, total_pages + 1):
        data = ctm_get('/calls', f'format=json&page_size=100&start_date={START_DATE}&end_date={END_DATE}&page={page}')
        all_calls.extend(data.get('calls', []))
        if page % 20 == 0:
            print(f"  Page {page}/{total_pages}...")
        time.sleep(0.1)

    print(f"  Done: {len(all_calls)} calls pulled")
    return all_calls

def pull_all_forms():
    """Pull all historical form submissions."""
    all_forms = []
    data = ctm_get('/calls', 'format=json&page_size=10&direction=form&page=1')
    total_pages = data['total_pages']
    all_forms.extend(data.get('calls', []))
    for page in range(2, total_pages + 1):
        data = ctm_get('/calls', f'format=json&page_size=10&direction=form&page={page}')
        all_forms.extend(data.get('calls', []))
        time.sleep(0.1)
    print(f"  Forms: {len(all_forms)} pulled")
    return all_forms

# ── Facility helper ──────────────────────────────────────────────────────────
def get_fac(c):
    return 'fw' if (c.get('tracking_number_bare', '') or '').startswith('260') else 'wl'

# ── Data processing ──────────────────────────────────────────────────────────
def process_calls(calls):
    DATES = sorted(set(c['called_at'][:10] for c in calls))

    # Daily full
    daily = defaultdict(lambda: {
        'total':0,'inbound':0,'outbound':0,'missed':0,'new':0,
        'fw_total':0,'fw_inbound':0,'fw_missed':0,'fw_new':0,
        'wl_total':0,'wl_inbound':0,'wl_missed':0,'wl_new':0,
        'yes':0,'no':0,'lead':0,'dur_sum':0,'dur_n':0
    })

    # Split data
    daily_fw  = {'total':Counter(),'inbound':Counter(),'missed':Counter(),'new':Counter()}
    daily_wl  = {'total':Counter(),'inbound':Counter(),'missed':Counter(),'new':Counter()}
    daily_all = {'total':Counter(),'inbound':Counter(),'missed':Counter(),'new':Counter()}
    dow_all = Counter(); dow_fw = Counter(); dow_wl = Counter()
    hm_all = Counter(); hm_fw = Counter(); hm_wl = Counter()
    src_all = Counter(); src_fw = Counter(); src_wl = Counter()
    nc_dow_all = Counter(); nc_dow_fw = Counter(); nc_dow_wl = Counter()
    nc_hr_all = Counter(); nc_hr_fw = Counter(); nc_hr_wl = Counter()
    missed_log = []
    agent_stats = defaultdict(lambda: {'total':0,'inbound':0,'outbound':0,'answered':0,'missed':0,'dur':0,'dur_n':0,'fw':0,'wl':0})
    disposition = Counter()
    disp_by_fac = defaultdict(Counter)
    sale_daily = defaultdict(lambda: {'yes':0,'no':0,'lead':0})
    pages = Counter()
    refs = Counter()
    notes = []

    for c in calls:
        d = c['called_at'][:10]
        fac = get_fac(c)
        is_ib  = c['direction'] == 'inbound'
        is_ob  = c['direction'] == 'outbound'
        is_miss = c.get('dial_status','') in ('no-answer','busy','no answer') and is_ib
        is_new  = bool(c.get('is_new_caller')) and is_ib
        src     = c.get('source','Unknown')
        day     = c.get('day','')
        hour    = c.get('hour','')
        dur     = c.get('duration',0) or 0
        sale    = (c.get('sale') or {}).get('name','').lower().strip()
        cf      = c.get('custom_fields') or {}

        # Daily full
        r = daily[d]
        r['total'] += 1
        if is_ib:   r['inbound'] += 1
        if is_ob:   r['outbound'] += 1
        if is_miss: r['missed'] += 1
        if is_new:  r['new'] += 1
        if dur > 0: r['dur_sum'] += dur; r['dur_n'] += 1
        r[f'{fac}_total'] += 1
        if is_ib:   r[f'{fac}_inbound'] += 1
        if is_miss: r[f'{fac}_missed'] += 1
        if is_new:  r[f'{fac}_new'] += 1
        if sale == 'yes': r['yes'] += 1
        elif sale == 'no': r['no'] += 1
        elif 'lead' in sale: r['lead'] += 1

        # Split data
        for ds, f_ds in [(daily_all, None), (daily_fw if fac=='fw' else daily_wl, None)]:
            ds['total'][d] += 1
            if is_ib:   ds['inbound'][d] += 1
            if is_miss: ds['missed'][d] += 1
            if is_new:  ds['new'][d] += 1

        # DOW
        dow_all[day] += 1
        (dow_fw if fac=='fw' else dow_wl)[day] += 1

        # Heatmap
        if is_ib and day and hour:
            key = f'{day}|{hour}'
            hm_all[key] += 1
            (hm_fw if fac=='fw' else hm_wl)[key] += 1

        # Sources
        if is_ib:
            src_all[src] += 1
            (src_fw if fac=='fw' else src_wl)[src] += 1

        # New callers
        if is_new:
            nc_dow_all[day] += 1; (nc_dow_fw if fac=='fw' else nc_dow_wl)[day] += 1
            nc_hr_all[hour] += 1; (nc_hr_fw if fac=='fw' else nc_hr_wl)[hour] += 1

        # Missed log
        if is_miss:
            missed_log.append({
                'time': c['called_at'][:16],
                'caller': c.get('caller_number_format',''),
                'source': src,
                'fa': 'Fort Wayne' if fac=='fw' else 'Waterloo'
            })

        # Agents
        ag = c.get('agent',{}).get('name','') if c.get('agent') else ''
        if ag:
            s = agent_stats[ag]
            s['total'] += 1; s[fac] += 1
            if is_ib: s['inbound'] += 1
            if is_ob: s['outbound'] += 1
            status = c.get('dial_status','')
            if status in ('answered','completed'):
                s['answered'] += 1
                s['dur'] += dur; s['dur_n'] += 1
            elif status in ('no-answer','busy','no answer'):
                s['missed'] += 1

        # Disposition
        disp = cf.get('disposition_save_to_contact','')
        if disp:
            disposition[disp] += 1
            disp_by_fac['Fort Wayne' if fac=='fw' else 'Waterloo'][disp] += 1

        # Sale daily
        sd = sale_daily[d]
        if sale == 'yes': sd['yes'] += 1
        elif sale == 'no': sd['no'] += 1
        elif 'lead' in sale: sd['lead'] += 1

        # Landing pages
        loc = (c.get('last_location','') or '').split('?')[0].rstrip('/')
        if loc:
            label = loc.replace('https://www.striverehabfortwayne.com','FW').replace('https://www.striverehabwaterloo.com','WL')
            pages[label] += 1

        # Referrers
        ref = c.get('referrer','') or ''
        if ref:
            try:
                domain = ref.split('/')[2] if '//' in ref else ref
                refs[domain] += 1
            except: pass

        # Notes
        note = (c.get('notes','') or '').strip()
        if note:
            notes.append({'dt': c['called_at'][:16], 'ag': ag, 'n': note[:150], 'fa': 'Fort Wayne' if fac=='fw' else 'Waterloo'})

    return {
        'daily_full': {k: dict(v) for k, v in daily.items()},
        'split': {
            'daily_all': {k: dict(v) for k, v in daily_all.items()},
            'daily_fw':  {k: dict(v) for k, v in daily_fw.items()},
            'daily_wl':  {k: dict(v) for k, v in daily_wl.items()},
            'dow_all': dict(dow_all), 'dow_fw': dict(dow_fw), 'dow_wl': dict(dow_wl),
            'hm_all': dict(hm_all), 'hm_fw': dict(hm_fw), 'hm_wl': dict(hm_wl),
            'src_all': dict(src_all), 'src_fw': dict(src_fw), 'src_wl': dict(src_wl),
            'nc_dow_all': dict(nc_dow_all), 'nc_dow_fw': dict(nc_dow_fw), 'nc_dow_wl': dict(nc_dow_wl),
            'nc_hr_all': dict(nc_hr_all), 'nc_hr_fw': dict(nc_hr_fw), 'nc_hr_wl': dict(nc_hr_wl),
            'missed_all': missed_log,
            'agents': {k: dict(v) for k, v in agent_stats.items()}
        },
        'marketing': {
            'disposition': dict(disposition),
            'disp_by_fac': {k: dict(v) for k, v in disp_by_fac.items()},
            'sale_daily': {k: dict(v) for k, v in sale_daily.items()},
            'pages': dict(pages.most_common(12)),
            'refs': dict(refs.most_common(10)),
            'notes': notes
        }
    }

def process_forms(raw_forms):
    forms = []
    for f in raw_forms:
        tn = f.get('tracking_number_bare', '') or ''
        facility = 'Fort Wayne' if tn.startswith('260') else 'Waterloo'
        form_data = f.get('form', {}) or {}
        custom = form_data.get('custom', []) or []
        how_help = ''
        for c in custom:
            if c.get('id') == 'how_can_we_help':
                how_help = (c.get('value', '') or '').strip()
        forms.append({
            'dt': f['called_at'][:16],
            'n':  (f.get('name','') or '')[:40],
            'e':  f.get('email','') or '',
            'ph': f.get('caller_number_format','') or '',
            'ci': f.get('city','') or '',
            'st': f.get('state','') or '',
            'sr': f.get('source','') or '',
            'fa': facility,
            'fn': form_data.get('form_name','') or '',
            'nw': bool(f.get('is_new_caller', False)),
            'hw': how_help
        })
    return forms

def process_recordings(calls):
    recs = []
    for c in calls:
        if not (c.get('transcription_text','') or '').strip():
            continue
        if c['direction'] != 'inbound':
            continue
        fac = get_fac(c)
        dur_s = c.get('duration', 0) or 0
        dur_str = f"{dur_s//60}m {dur_s%60}s" if dur_s >= 60 else f"{dur_s}s"
        recs.append({
            'c':  (c.get('name','') or c.get('caller_number_format','') or '')[:28],
            'p':  c.get('caller_number_format',''),
            't':  c['called_at'][:16],
            'd':  dur_str,
            'ds': dur_s,
            'a':  c.get('agent',{}).get('name','') if c.get('agent') else '',
            's':  c.get('source',''),
            'f':  'Fort Wayne' if fac=='fw' else 'Waterloo',
            'su': (c.get('summary','') or '')[:120],
            'tr': (c.get('transcription_text','') or '')[:300],
            'au': c.get('audio','') or ''
        })
    return recs

def build_call_log(calls):
    log = []
    for c in calls:
        fac = get_fac(c)
        log.append({
            'dt': c['called_at'][:16],
            'di': c['direction'],
            'st': c.get('dial_status', c.get('call_status','')),
            'cl': c.get('caller_number_format',''),
            'nm': (c.get('name','') or c.get('cnam','') or '')[:30],
            'ci': c.get('city','') or '',
            'sa': c.get('state','') or '',
            'sr': c.get('source',''),
            'ag': c.get('agent',{}).get('name','') if c.get('agent') else '',
            'du': c.get('duration',0) or 0,
            'nw': bool(c.get('is_new_caller', False)),
            'tr': bool((c.get('transcription_text','') or '').strip()),
            'fa': 'Fort Wayne' if fac=='fw' else 'Waterloo'
        })
    return log

# ── GitHub push helper ───────────────────────────────────────────────────────
def github_push(path, content_bytes, message):
    """Push a file to GitHub, creating or updating as needed."""
    url = f'https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/contents/{path}'
    headers = {
        'Authorization': f'token {GH_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'StriveAnalytics/1.0'
    }
    # Get existing SHA
    sha = None
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as r:
            sha = json.loads(r.read()).get('sha')
    except: pass

    payload = {
        'message': message,
        'content': base64.b64encode(content_bytes).decode(),
        'branch': GH_BRANCH
    }
    if sha:
        payload['sha'] = sha

    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, method='PUT', headers={**headers, 'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=60) as r:
        result = json.loads(r.read())
    print(f"  Pushed: {path} — commit {result['commit']['sha'][:10]}")
    return result

# ── HTML template injection ──────────────────────────────────────────────────
def inject_data(split_json, daily_json, recs_json, log_json, forms_json, mkt_json, start_date, end_date):
    """Read the HTML template and inject fresh data."""
    with open('dashboard_template.html', 'r') as f:
        html = f.read()

    # Replace data placeholders
    replacements = {
        'const SD = /*INJECT_SD*/;':          f'const SD = {split_json};',
        'const DAILY_FULL = /*INJECT_DAILY*/;': f'const DAILY_FULL = {daily_json};',
        'const RECS = /*INJECT_RECS*/;':      f'const RECS = {recs_json};',
        'const LOG = /*INJECT_LOG*/;':        f'const LOG = {log_json};',
        'const FORMS = /*INJECT_FORMS*/;':    f'const FORMS = {forms_json};',
        'const MKT = /*INJECT_MKT*/;':        f'const MKT = {mkt_json};',
    }
    for placeholder, value in replacements.items():
        html = html.replace(placeholder, value)

    return html

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(f"Strive Dashboard Builder — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Period: {START_DATE} → {END_DATE}")
    print(f"{'='*60}\n")

    # 1. Pull data
    print("1. Pulling calls from CTM...")
    calls = pull_all_calls()

    print("2. Pulling form submissions...")
    raw_forms = pull_all_forms()

    # 2. Process
    print("3. Processing data...")
    processed = process_calls(calls)
    forms     = process_forms(raw_forms)
    recs      = process_recordings(calls)
    log       = build_call_log(calls)

    # 3. Serialize
    print("4. Serializing...")
    split_json = json.dumps(processed['split'],    separators=(',',':'))
    daily_json = json.dumps(processed['daily_full'],separators=(',',':'))
    mkt_json   = json.dumps(processed['marketing'],separators=(',',':'))
    recs_json  = json.dumps(recs,                  separators=(',',':'))
    log_json   = json.dumps(log,                   separators=(',',':'))
    forms_json = json.dumps(forms,                 separators=(',',':'))

    print(f"   Data sizes — split:{len(split_json)//1024}KB daily:{len(daily_json)//1024}KB recs:{len(recs_json)//1024}KB log:{len(log_json)//1024}KB forms:{len(forms_json)//1024}KB mkt:{len(mkt_json)//1024}KB")

    # 4. Build HTML
    print("5. Building HTML...")
    html = inject_data(split_json, daily_json, recs_json, log_json, forms_json, mkt_json, START_DATE, END_DATE)
    print(f"   HTML size: {len(html)//1024}KB")

    # 5. Push to GitHub
    print("6. Pushing to GitHub...")
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M UTC')
    github_push('ctm-calls/index.html', html.encode(), f'Auto-refresh CTM: {now_str}')

    print(f"\n✓ Done! Dashboard updated at https://providentanalytics.github.io/Strive")
    print(f"{'='*60}\n")

if __name__ == '__main__':
    main()
