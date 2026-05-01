"""
nightly_pull.py
Pulls ALL available CTM data (Dec 2025 to today) and rebuilds the dashboard.
Called by GitHub Actions nightly workflow.
"""
import urllib.request, json, time, os
from datetime import datetime, timedelta
from collections import defaultdict, Counter

CTM_AUTH = os.environ.get('CTM_AUTH', '')
GH_TOKEN = os.environ.get('GH_TOKEN', '')
CTM_ACCT = '559323'
BASE     = f'https://api.calltrackingmetrics.com/api/v1/accounts/{CTM_ACCT}'
OWNER    = 'ProvidentAnalytics'
REPO     = 'Strive'

# Pull from the beginning of available data
DATA_START = '2025-12-01'
DATA_END   = datetime.now().strftime('%Y-%m-%d')

print(f'Period: {DATA_START} to {DATA_END}')

def ctm_fetch(path, params):
    url = f'{BASE}{path}?{params}'
    req = urllib.request.Request(url, headers={
        'Authorization': f'Basic {CTM_AUTH}',
        'User-Agent': 'StriveAnalytics/3.0'
    })
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.loads(r.read())

def pull_range(start, end, direction=''):
    calls, page = [], 1
    dir_param = f'&direction={direction}' if direction else ''
    while True:
        params = f'format=json&page_size=100&start_date={start}&end_date={end}&page={page}{dir_param}'
        d = ctm_fetch('/calls', params)
        calls.extend(d.get('calls', []))
        if page >= d['total_pages']: break
        page += 1
        time.sleep(0.12)
    return calls

def pull_all_calls():
    """Pull all calls in monthly chunks to avoid timeouts."""
    all_calls = []
    start = datetime.strptime(DATA_START, '%Y-%m-%d')
    end   = datetime.strptime(DATA_END, '%Y-%m-%d')
    
    current = start
    while current <= end:
        month_end = (current.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        if month_end > end:
            month_end = end
        
        s = current.strftime('%Y-%m-%d')
        e = month_end.strftime('%Y-%m-%d')
        calls = pull_range(s, e)
        all_calls.extend(calls)
        print(f'  {s[:7]}: {len(calls)} calls')
        
        current = month_end + timedelta(days=1)
    
    # Deduplicate
    seen, deduped = set(), []
    for c in all_calls:
        cid = str(c.get('id',''))
        if cid and cid not in seen:
            seen.add(cid)
            deduped.append(c)
        elif not cid:
            deduped.append(c)
    return deduped

def pull_all_forms():
    return pull_range(DATA_START, DATA_END, direction='form')

def get_fac(c):
    return 'fw' if (c.get('tracking_number_bare','') or '').startswith('260') else 'wl'

def process_calls(calls, forms_raw):
    """Full data processing — returns all data dicts."""
    from collections import defaultdict, Counter

    daily = defaultdict(lambda: {
        'total':0,'inbound':0,'outbound':0,'missed':0,'new':0,
        'fw_total':0,'fw_inbound':0,'fw_missed':0,'fw_new':0,
        'wl_total':0,'wl_inbound':0,'wl_missed':0,'wl_new':0,
        'yes':0,'no':0,'lead':0,'dur_sum':0,'dur_n':0,'src':{}
    })
    daily_split = {
        'all':{'total':Counter(),'inbound':Counter(),'missed':Counter(),'new':Counter()},
        'fw': {'total':Counter(),'inbound':Counter(),'missed':Counter(),'new':Counter()},
        'wl': {'total':Counter(),'inbound':Counter(),'missed':Counter(),'new':Counter()},
    }
    dow_all=Counter(); dow_fw=Counter(); dow_wl=Counter()
    hm_daily = defaultdict(lambda: defaultdict(int))
    src_all=Counter(); src_fw=Counter(); src_wl=Counter()
    nc_dow_all=Counter(); nc_dow_fw=Counter(); nc_dow_wl=Counter()
    nc_hr_all=Counter(); nc_hr_fw=Counter(); nc_hr_wl=Counter()
    missed_log = []
    agent_stats = defaultdict(lambda: {'total':0,'inbound':0,'outbound':0,'answered':0,'missed':0,'dur':0,'dur_n':0,'fw':0,'wl':0})
    agent_daily = defaultdict(lambda: defaultdict(lambda: {'total':0,'inbound':0,'outbound':0,'answered':0,'missed':0,'dur':0,'dur_n':0}))
    returning_daily = Counter()
    nc_daily_detail = defaultdict(lambda: {'dow':Counter(),'hour':Counter(),'src':Counter()})
    disposition = Counter()
    disp_by_fac = defaultdict(Counter)
    sale_daily  = defaultdict(lambda: {'yes':0,'no':0,'lead':0})
    pages_ctr   = Counter()
    refs_ctr    = Counter()
    notes       = []

    for c in calls:
        d   = c['called_at'][:10]
        fac = get_fac(c)
        ib  = c['direction'] == 'inbound'
        ob  = c['direction'] == 'outbound'
        ms  = c.get('dial_status','') in ('no-answer','busy','no answer') and ib
        nw  = bool(c.get('is_new_caller')) and ib
        src = c.get('source','Unknown')
        day = c.get('day','')
        hr  = c.get('hour','')
        dur = c.get('duration',0) or 0
        sale_raw = c.get('sale') or {}
        sale = (sale_raw.get('name') or '').lower().strip()
        cf   = c.get('custom_fields') or {}

        r = daily[d]
        r['total']+=1
        if ib: r['inbound']+=1
        if ob: r['outbound']+=1
        if ms: r['missed']+=1
        if nw: r['new']+=1
        if dur>0: r['dur_sum']+=dur; r['dur_n']+=1
        r[f'{fac}_total']+=1
        if ib: r[f'{fac}_inbound']+=1
        if ms: r[f'{fac}_missed']+=1
        if nw: r[f'{fac}_new']+=1
        if sale=='yes': r['yes']+=1
        elif sale=='no': r['no']+=1
        elif 'lead' in sale: r['lead']+=1
        r['src'][src] = r['src'].get(src,0)+1

        for ds_fac, ds in [('all',daily_split['all']),(fac,daily_split[fac])]:
            ds['total'][d]+=1
            if ib: ds['inbound'][d]+=1
            if ms: ds['missed'][d]+=1
            if nw: ds['new'][d]+=1

        dow_all[day]+=1
        (dow_fw if fac=='fw' else dow_wl)[day]+=1

        if ib and day and hr:
            hm_daily[d][day+'|'+hr]+=1
            hm_daily[d]['_fac_'+fac+'_'+day+'|'+hr]+=1
        if ib:
            src_all[src]+=1
            (src_fw if fac=='fw' else src_wl)[src]+=1
        if nw:
            nc_dow_all[day]+=1; (nc_dow_fw if fac=='fw' else nc_dow_wl)[day]+=1
            nc_hr_all[hr]+=1;   (nc_hr_fw  if fac=='fw' else nc_hr_wl)[hr]+=1
        if ms:
            missed_log.append({'time':c['called_at'][:16],'caller':c.get('caller_number_format',''),'source':src,'fa':'Fort Wayne' if fac=='fw' else 'Waterloo'})
        ag = (c.get('agent') or {}).get('name','')
        if ag:
            s = agent_stats[ag]; s['total']+=1; s[fac]+=1
            if ib: s['inbound']+=1
            if ob: s['outbound']+=1
            status = c.get('dial_status','')
            if status in ('answered','completed'): s['answered']+=1; s['dur']+=dur; s['dur_n']+=1
            elif status in ('no-answer','busy','no answer'): s['missed']+=1
            as2 = agent_daily[ag][d]
            as2['total']+=1
            if ib: as2['inbound']+=1
            if ob: as2['outbound']+=1
            if status in ('answered','completed'): as2['answered']+=1; as2['dur']+=dur; as2['dur_n']+=1
            elif status in ('no-answer','busy','no answer'): as2['missed']+=1

        if ib and not nw: returning_daily[d]+=1
        if nw:
            nc_daily_detail[d]['dow'][day]+=1
            nc_daily_detail[d]['hour'][hr]+=1
            nc_daily_detail[d]['src'][src]+=1

        disp = cf.get('disposition_save_to_contact','')
        if disp:
            disposition[disp]+=1
            disp_by_fac['Fort Wayne' if fac=='fw' else 'Waterloo'][disp]+=1
        sd = sale_daily[d]
        if sale=='yes': sd['yes']+=1
        elif sale=='no': sd['no']+=1
        elif 'lead' in sale: sd['lead']+=1
        loc = (c.get('last_location','') or '').split('?')[0].rstrip('/')
        if loc:
            lbl = loc.replace('https://www.striverehabfortwayne.com','FW').replace('https://www.striverehabwaterloo.com','WL')
            pages_ctr[lbl]+=1
        ref = c.get('referrer','') or ''
        if ref:
            try: refs_ctr[ref.split('/')[2] if '//' in ref else ref]+=1
            except: pass
        note = (c.get('notes','') or '').strip()
        if note: notes.append({'dt':c['called_at'][:16],'ag':ag,'n':note[:150],'fa':'Fort Wayne' if fac=='fw' else 'Waterloo'})

    # Recordings
    recordings = []
    for c in calls:
        if c['direction']!='inbound': continue
        tr = (c.get('transcription_text','') or '').strip()
        if not tr: continue
        fac=get_fac(c); dur_s=c.get('duration',0) or 0
        recordings.append({'c':((c.get('name','') or c.get('cnam','') or c.get('caller_number_format','')) or '')[:28],
            'p':c.get('caller_number_format',''),'t':c['called_at'][:16],
            'd':f"{dur_s//60}m {dur_s%60}s" if dur_s>=60 else f"{dur_s}s",'ds':dur_s,
            'a':(c.get('agent') or {}).get('name',''),'s':c.get('source',''),
            'f':'Fort Wayne' if fac=='fw' else 'Waterloo',
            'su':(c.get('summary','') or '')[:120],'tr':tr[:300],'au':c.get('audio',''  ) or ''})

    # Call log
    call_log = []
    for c in calls:
        fac=get_fac(c)
        call_log.append({'dt':c['called_at'][:16],'di':c['direction'],
            'st':c.get('dial_status',c.get('call_status','')),'cl':c.get('caller_number_format',''),
            'nm':((c.get('name','') or c.get('cnam','')) or '')[:30],
            'ci':c.get('city','') or '','sa':c.get('state','') or '','sr':c.get('source',''),
            'ag':(c.get('agent') or {}).get('name',''),'du':c.get('duration',0) or 0,
            'nw':bool(c.get('is_new_caller',False)),
            'tr':bool((c.get('transcription_text','') or '').strip()),
            'fa':'Fort Wayne' if fac=='fw' else 'Waterloo'})

    # Forms
    forms_clean = []
    for f in forms_raw:
        fac2 = get_fac(f)
        fd = f.get('form',{}) or {}
        hw = ''
        for cf2 in (fd.get('custom',[]) or []):
            if cf2.get('id')=='how_can_we_help': hw=(cf2.get('value','') or '').strip()
        forms_clean.append({'dt':f.get('called_at','')[:16],'n':(f.get('name','') or '')[:40],
            'e':f.get('email','') or '','ph':f.get('caller_number_format','') or '',
            'ci':f.get('city','') or '','st':f.get('state','') or '','sr':f.get('source','') or '',
            'fa':'Fort Wayne' if fac2=='fw' else 'Waterloo','fn':fd.get('form_name','') or '',
            'nw':bool(f.get('is_new_caller',False)),'hw':hw})

    sep = (',',':')
    files = {
        'data_daily_full.json': {k:dict(v) for k,v in daily.items()},
        'data_split.json': {
            'daily_all':{k:dict(v) for k,v in daily_split['all'].items()},
            'daily_fw': {k:dict(v) for k,v in daily_split['fw'].items()},
            'daily_wl': {k:dict(v) for k,v in daily_split['wl'].items()},
            'dow_all':dict(dow_all),'dow_fw':dict(dow_fw),'dow_wl':dict(dow_wl),
            'src_all':dict(src_all),'src_fw':dict(src_fw),'src_wl':dict(src_wl),
            'nc_dow_all':dict(nc_dow_all),'nc_dow_fw':dict(nc_dow_fw),'nc_dow_wl':dict(nc_dow_wl),
            'nc_hr_all':dict(nc_hr_all),'nc_hr_fw':dict(nc_hr_fw),'nc_hr_wl':dict(nc_hr_wl),
            'missed_all':missed_log,'agents':{k:dict(v) for k,v in agent_stats.items()}
        },
        'data_marketing.json': {'disposition':dict(disposition),'disp_by_fac':{k:dict(v) for k,v in disp_by_fac.items()},
            'sale_daily':{k:dict(v) for k,v in sale_daily.items()},
            'pages':dict(pages_ctr.most_common(12)),'refs':dict(refs_ctr.most_common(10)),'notes':notes},
        'data_recordings.json': recordings,
        'data_log.json': call_log,
        'data_forms.json': forms_clean,
        'data_extra.json': {
            'agent_daily':{ag:{d:dict(v) for d,v in days.items()} for ag,days in agent_daily.items()},
            'returning_daily':dict(returning_daily),
            'nc_daily_detail':{d:{'dow':dict(v['dow']),'hour':dict(v['hour']),'src':dict(v['src'])} for d,v in nc_daily_detail.items()}
        },
        'data_hm_daily.json': {d:dict(v) for d,v in hm_daily.items()},
    }
    for fname, data in files.items():
        with open(fname,'w') as f:
            json.dump(data, f, separators=sep)
        print(f'  {fname}: {os.path.getsize(fname)//1024} KB')

def push_to_github(dashboard_html):
    """Push updated dashboard to GitHub."""
    headers = {'Authorization':f'token {GH_TOKEN}','Accept':'application/vnd.github.v3+json',
                'User-Agent':'StriveAnalytics/3.0','Content-Type':'application/json'}
    def api(method, path, payload=None):
        url = f'https://api.github.com/repos/{OWNER}/{REPO}{path}'
        data = json.dumps(payload).encode() if payload else None
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        with urllib.request.urlopen(req, timeout=60) as r: return json.loads(r.read())
    ref       = api('GET','/git/refs/heads/main')
    latest    = ref['object']['sha']
    base_tree = api('GET',f'/git/commits/{latest}')['tree']['sha']
    from datetime import datetime, timezone
    now_str   = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    new_tree  = api('POST','/git/trees',{'base_tree':base_tree,'tree':[
        {'path':'ctm-calls/index.html','mode':'100644','type':'blob','content':dashboard_html}]})
    new_commit= api('POST','/git/commits',{'message':f'Auto-refresh: {now_str}','tree':new_tree['sha'],'parents':[latest]})
    api('PATCH','/git/refs/heads/main',{'sha':new_commit['sha']})
    print(f'Pushed to GitHub: {now_str}')

def build_dashboard():
    """Build dashboard from template + data files."""
    with open('dashboard_template_v2.html') as f: tmpl = f.read()
    def load(fname):
        with open(fname) as f: return f.read()
    replacements = {
        'INJECT_SD':    load('data_split.json'),
        'INJECT_DAILY': load('data_daily_full.json'),
        'INJECT_RECS':  load('data_recordings.json'),
        'INJECT_LOG':   load('data_log.json'),
        'INJECT_FORMS': load('data_forms.json'),
        'INJECT_MKT':   load('data_marketing.json'),
        'INJECT_EXTRA': load('data_extra.json'),
        'INJECT_HMD':   load('data_hm_daily.json'),
    }
    for key, data in replacements.items():
        tmpl = tmpl.replace('/*' + key + '*/null', data, 1)
    with open('strive_ctm_clean.html','w') as f: f.write(tmpl)
    print(f'Dashboard built: {len(tmpl)//1024} KB')
    return tmpl

if __name__ == '__main__':
    print('1. Pulling calls...')
    calls = pull_all_calls()
    print(f'   Total: {len(calls)} calls')
    dates = sorted(set(c["called_at"][:10] for c in calls))
    print(f'   Range: {dates[0]} to {dates[-1]}')

    print('2. Pulling forms...')
    forms = pull_all_forms()
    print(f'   Total: {len(forms)} forms')

    print('3. Processing data...')
    process_calls(calls, forms)

    print('4. Building dashboard...')
    html = build_dashboard()

    if GH_TOKEN:
        print('5. Pushing to GitHub...')
        push_to_github(html)
    else:
        print('5. No GH_TOKEN — skipping push (local build only)')

    print('Done!')
