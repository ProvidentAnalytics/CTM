"""
process_data.py — CI/CD version
Pulls fresh data from CTM API and writes all data JSON files.
Called by the GitHub Actions workflow before build_clean.py.
"""
import urllib.request, json, time, os
from collections import defaultdict, Counter
from datetime import datetime, timedelta

# ── Config ──────────────────────────────────────────────────────────────
CTM_AUTH = os.environ.get('CTM_AUTH', '')
CTM_ACCT = '559323'
BASE_URL = f'https://api.calltrackingmetrics.com/api/v1/accounts/{CTM_ACCT}'

END_DATE   = datetime.now().strftime('%Y-%m-%d')
START_DATE = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

print(f"Period: {START_DATE} → {END_DATE}")

# ── CTM fetch ────────────────────────────────────────────────────────────
def ctm_get(path, params=''):
    url = f'{BASE_URL}{path}?{params}'
    req = urllib.request.Request(url, headers={'Authorization': f'Basic {CTM_AUTH}', 'User-Agent': 'StriveAnalytics/2.0'})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def pull_calls():
    data = ctm_get('/calls', f'format=json&page_size=100&start_date={START_DATE}&end_date={END_DATE}&page=1')
    total_pages = data['total_pages']
    print(f"Pulling {data['total_entries']} calls across {total_pages} pages...")
    calls = data.get('calls', [])
    for page in range(2, total_pages + 1):
        data = ctm_get('/calls', f'format=json&page_size=100&start_date={START_DATE}&end_date={END_DATE}&page={page}')
        calls.extend(data.get('calls', []))
        if page % 20 == 0: print(f"  Page {page}/{total_pages}...")
        time.sleep(0.1)
    print(f"  Done: {len(calls)} calls")
    return calls

def pull_forms():
    data = ctm_get('/calls', 'format=json&page_size=10&direction=form&page=1')
    forms = data.get('calls', [])
    for page in range(2, data['total_pages'] + 1):
        forms.extend(ctm_get('/calls', f'format=json&page_size=10&direction=form&page={page}').get('calls', []))
        time.sleep(0.1)
    print(f"  Forms: {len(forms)}")
    return forms

# ── Import and run processing ─────────────────────────────────────────────
from process_data import process_all

def get_fac(c):
    return 'fw' if (c.get('tracking_number_bare', '') or '').startswith('260') else 'wl'

def process_forms_raw(forms_raw):
    forms_clean = []
    for f in forms_raw:
        tn = f.get('tracking_number_bare', '') or ''
        facility = 'Fort Wayne' if tn.startswith('260') else 'Waterloo'
        form_data = f.get('form', {}) or {}
        custom = form_data.get('custom', []) or []
        how_help = ''
        for c in custom:
            if c.get('id') == 'how_can_we_help':
                how_help = (c.get('value', '') or '').strip()
        forms_clean.append({
            'dt': f['called_at'][:16], 'n': (f.get('name','') or '')[:40],
            'e': f.get('email','') or '', 'ph': f.get('caller_number_format','') or '',
            'ci': f.get('city','') or '', 'st': f.get('state','') or '',
            'sr': f.get('source','') or '', 'fa': facility,
            'fn': form_data.get('form_name','') or '',
            'nw': bool(f.get('is_new_caller', False)), 'hw': how_help
        })
    return forms_clean

def build_extra(calls):
    agent_daily = defaultdict(lambda: defaultdict(lambda: {'total':0,'inbound':0,'outbound':0,'answered':0,'missed':0,'dur':0,'dur_n':0}))
    returning_daily = Counter()
    nc_daily_detail = defaultdict(lambda: {'dow':Counter(),'hour':Counter(),'src':Counter()})
    for c in calls:
        ag = (c.get('agent') or {}).get('name','')
        d  = c['called_at'][:10]
        if ag:
            s = agent_daily[ag][d]
            s['total'] += 1
            if c['direction']=='inbound':  s['inbound']  += 1
            if c['direction']=='outbound': s['outbound'] += 1
            status = c.get('dial_status','')
            dur = c.get('duration',0) or 0
            if status in ('answered','completed'):   s['answered'] += 1; s['dur'] += dur; s['dur_n'] += 1
            elif status in ('no-answer','busy','no answer'): s['missed'] += 1
        if c['direction']=='inbound' and not c.get('is_new_caller'):
            returning_daily[d] += 1
        if c.get('is_new_caller') and c['direction']=='inbound':
            dd = nc_daily_detail[d]
            dd['dow'][c.get('day','')] += 1
            dd['hour'][c.get('hour','')] += 1
            dd['src'][c.get('source','Unknown')] += 1
    return {
        'agent_daily': {ag: {d: dict(v) for d,v in days.items()} for ag,days in agent_daily.items()},
        'returning_daily': dict(returning_daily),
        'nc_daily_detail': {d: {'dow':dict(v['dow']),'hour':dict(v['hour']),'src':dict(v['src'])} for d,v in nc_daily_detail.items()}
    }

def build_hm_daily(calls):
    hm_daily = defaultdict(lambda: defaultdict(int))
    for c in calls:
        if c['direction'] != 'inbound': continue
        d   = c['called_at'][:10]
        fac = 'fw' if (c.get('tracking_number_bare','') or '').startswith('260') else 'wl'
        day = c.get('day',''); hr = c.get('hour','')
        if day and hr:
            hm_daily[d][f'{day}|{hr}'] += 1
            hm_daily[d][f'_fac_{fac}_{day}|{hr}'] += 1
    return {d: dict(v) for d,v in hm_daily.items()}

# ── Main ─────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print("1. Pulling calls...")
    calls = pull_calls()
    print("2. Pulling forms...")
    forms_raw = pull_forms()
    print("3. Processing...")
    result  = process_all(calls, forms_raw)
    extra   = build_extra(calls)
    hm_d    = build_hm_daily(calls)
    forms_c = process_forms_raw(forms_raw)

    sep = (',', ':')
    files = {
        'data_split.json':      result['split'],
        'data_daily_full.json': result['daily_full'],
        'data_marketing.json':  result['marketing'],
        'data_recordings.json': result['recordings'],
        'data_log.json':        result['call_log'],
        'data_forms.json':      forms_c,
        'data_extra.json':      extra,
        'data_hm_daily.json':   hm_d,
    }
    for fname, data in files.items():
        with open(fname, 'w') as f:
            json.dump(data, f, separators=sep)
        print(f"  {fname}: {os.path.getsize(fname)//1024} KB")

    print(f"Done. Period: {START_DATE} → {END_DATE}")
