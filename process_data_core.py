"""
process_data.py
Processes raw CTM data into clean JSON blobs for the dashboard.
Run once to generate all data files, or call from build_dashboard.py
"""
import json, os
from collections import defaultdict, Counter

def get_fac(c):
    return 'fw' if (c.get('tracking_number_bare','') or '').startswith('260') else 'wl'

def process_all(calls, forms_raw):
    """Master processing function. Returns all data dicts needed by dashboard."""

    # ── 1. DAILY FULL (one row per day, all metrics) ──────────────────────
    daily = defaultdict(lambda: {
        'total':0,'inbound':0,'outbound':0,'missed':0,'new':0,
        'fw_total':0,'fw_inbound':0,'fw_missed':0,'fw_new':0,
        'wl_total':0,'wl_inbound':0,'wl_missed':0,'wl_new':0,
        'yes':0,'no':0,'lead':0,'dur_sum':0,'dur_n':0,'src':{}
    })

    # ── 2. SPLIT DATA (pre-aggregated for charts) ─────────────────────────
    daily_all = {'total':Counter(),'inbound':Counter(),'missed':Counter(),'new':Counter()}
    daily_fw  = {'total':Counter(),'inbound':Counter(),'missed':Counter(),'new':Counter()}
    daily_wl  = {'total':Counter(),'inbound':Counter(),'missed':Counter(),'new':Counter()}

    dow_all = Counter(); dow_fw = Counter(); dow_wl = Counter()
    hm_all  = Counter(); hm_fw  = Counter(); hm_wl  = Counter()
    src_all = Counter(); src_fw = Counter(); src_wl = Counter()
    nc_dow_all = Counter(); nc_dow_fw = Counter(); nc_dow_wl = Counter()
    nc_hr_all  = Counter(); nc_hr_fw  = Counter(); nc_hr_wl  = Counter()
    missed_log = []
    agent_stats = defaultdict(lambda: {
        'total':0,'inbound':0,'outbound':0,'answered':0,'missed':0,
        'dur':0,'dur_n':0,'fw':0,'wl':0
    })

    # ── 3. MARKETING DATA ─────────────────────────────────────────────────
    disposition  = Counter()
    disp_by_fac  = defaultdict(Counter)
    sale_daily   = defaultdict(lambda: {'yes':0,'no':0,'lead':0})
    pages        = Counter()
    refs         = Counter()
    notes        = []

    for c in calls:
        d    = c['called_at'][:10]
        fac  = get_fac(c)
        is_ib   = c['direction'] == 'inbound'
        is_ob   = c['direction'] == 'outbound'
        is_miss = c.get('dial_status','') in ('no-answer','busy','no answer') and is_ib
        is_new  = bool(c.get('is_new_caller')) and is_ib
        src     = c.get('source','Unknown')
        day     = c.get('day','')
        hour    = c.get('hour','')
        dur     = c.get('duration',0) or 0
        sale    = (c.get('sale') or {}).get('name','').lower().strip()
        cf      = c.get('custom_fields') or {}

        # ── Daily full ──
        r = daily[d]
        r['total'] += 1
        if is_ib:   r['inbound']  += 1
        if is_ob:   r['outbound'] += 1
        if is_miss: r['missed']   += 1
        if is_new:  r['new']      += 1
        if dur > 0: r['dur_sum']  += dur; r['dur_n'] += 1
        r[f'{fac}_total'] += 1
        if is_ib:   r[f'{fac}_inbound'] += 1
        if is_miss: r[f'{fac}_missed']  += 1
        if is_new:  r[f'{fac}_new']     += 1
        if sale == 'yes':    r['yes']  += 1
        elif sale == 'no':   r['no']   += 1
        elif 'lead' in sale: r['lead'] += 1
        r['src'][src] = r['src'].get(src, 0) + 1

        # ── Split data daily ──
        for ds in [daily_all, daily_fw if fac=='fw' else daily_wl]:
            ds['total'][d]   += 1
            if is_ib:   ds['inbound'][d]  += 1
            if is_miss: ds['missed'][d]   += 1
            if is_new:  ds['new'][d]      += 1

        # ── DOW ──
        dow_all[day] += 1
        (dow_fw if fac=='fw' else dow_wl)[day] += 1

        # ── Heatmap (inbound only) ──
        if is_ib and day and hour:
            key = f'{day}|{hour}'
            hm_all[key] += 1
            (hm_fw if fac=='fw' else hm_wl)[key] += 1

        # ── Sources (inbound only) ──
        if is_ib:
            src_all[src] += 1
            (src_fw if fac=='fw' else src_wl)[src] += 1

        # ── New callers breakdown ──
        if is_new:
            nc_dow_all[day] += 1
            (nc_dow_fw if fac=='fw' else nc_dow_wl)[day] += 1
            nc_hr_all[hour] += 1
            (nc_hr_fw if fac=='fw' else nc_hr_wl)[hour] += 1

        # ── Missed log ──
        if is_miss:
            missed_log.append({
                'time':   c['called_at'][:16],
                'caller': c.get('caller_number_format',''),
                'source': src,
                'fa':     'Fort Wayne' if fac=='fw' else 'Waterloo'
            })

        # ── Agent stats ──
        ag = (c.get('agent') or {}).get('name','')
        if ag:
            s = agent_stats[ag]
            s['total'] += 1; s[fac] += 1
            if is_ib: s['inbound']  += 1
            if is_ob: s['outbound'] += 1
            status = c.get('dial_status','')
            if status in ('answered','completed'):
                s['answered'] += 1; s['dur'] += dur; s['dur_n'] += 1
            elif status in ('no-answer','busy','no answer'):
                s['missed'] += 1

        # ── Disposition ──
        disp = cf.get('disposition_save_to_contact','')
        if disp:
            disposition[disp] += 1
            disp_by_fac['Fort Wayne' if fac=='fw' else 'Waterloo'][disp] += 1

        # ── Sale daily ──
        sd = sale_daily[d]
        if sale == 'yes':    sd['yes']  += 1
        elif sale == 'no':   sd['no']   += 1
        elif 'lead' in sale: sd['lead'] += 1

        # ── Landing pages ──
        loc = (c.get('last_location','') or '').split('?')[0].rstrip('/')
        if loc:
            label = (loc
                .replace('https://www.striverehabfortwayne.com','FW')
                .replace('https://www.striverehabwaterloo.com','WL'))
            pages[label] += 1

        # ── Referrers ──
        ref = c.get('referrer','') or ''
        if ref:
            try:
                domain = ref.split('/')[2] if '//' in ref else ref
                refs[domain] += 1
            except: pass

        # ── Notes ──
        note = (c.get('notes','') or '').strip()
        if note:
            notes.append({
                'dt': c['called_at'][:16],
                'ag': ag,
                'n':  note[:150],
                'fa': 'Fort Wayne' if fac=='fw' else 'Waterloo'
            })

    # ── 4. RECORDINGS ────────────────────────────────────────────────────
    # Load backfilled transcripts (keyed by call ID string)
    backfill = {}
    if os.path.exists('data_backfill_transcripts.json'):
        import json as _json
        with open('data_backfill_transcripts.json') as _f:
            backfill = _json.load(_f)

    recordings = []
    for c in calls:
        if c['direction'] != 'inbound': continue
        tr = (c.get('transcription_text','') or '').strip()
        # Fall back to backfilled transcript if CTM has none
        if not tr:
            bf = backfill.get(str(c.get('id','')))
            if bf:
                tr = (bf.get('transcript','') or '').strip()
        if not tr: continue
        fac   = get_fac(c)
        dur_s = c.get('duration',0) or 0
        recordings.append({
            'c':  ((c.get('name','') or c.get('cnam','') or c.get('caller_number_format','')) or '')[:28],
            'p':  c.get('caller_number_format',''),
            't':  c['called_at'][:16],
            'd':  f"{dur_s//60}m {dur_s%60}s" if dur_s >= 60 else f"{dur_s}s",
            'ds': dur_s,
            'a':  (c.get('agent') or {}).get('name',''),
            's':  c.get('source',''),
            'f':  'Fort Wayne' if fac=='fw' else 'Waterloo',
            'su': (c.get('summary','') or '')[:120],
            'tr': tr[:300],
            'au': c.get('audio','') or ''
        })

    # ── 5. CALL LOG ──────────────────────────────────────────────────────
    call_log = []
    for c in calls:
        fac = get_fac(c)
        call_log.append({
            'dt': c['called_at'][:16],
            'di': c['direction'],
            'st': c.get('dial_status', c.get('call_status','')),
            'cl': c.get('caller_number_format',''),
            'nm': ((c.get('name','') or c.get('cnam','')) or '')[:30],
            'ci': c.get('city','') or '',
            'sa': c.get('state','') or '',
            'sr': c.get('source',''),
            'ag': (c.get('agent') or {}).get('name',''),
            'du': c.get('duration',0) or 0,
            'nw': bool(c.get('is_new_caller',False)),
            'tr': bool(tr),
            'fa': 'Fort Wayne' if fac=='fw' else 'Waterloo'
        })

    # ── 6. FORMS ─────────────────────────────────────────────────────────
    forms_clean = []
    for f in forms_raw:
        fa = 'Fort Wayne' if (f.get('fa','') == 'Fort Wayne') else 'Waterloo'
        forms_clean.append({
            'dt': f.get('dt',''),
            'n':  (f.get('n','') or '')[:40],
            'e':  f.get('e','') or '',
            'ph': f.get('ph','') or '',
            'ci': f.get('ci','') or '',
            'st': f.get('st','') or '',
            'sr': f.get('sr','') or '',
            'fa': fa,
            'fn': f.get('fn','') or '',
            'nw': bool(f.get('nw',False)),
            'hw': f.get('hw','') or ''
        })

    # ── Assemble output ──────────────────────────────────────────────────
    return {
        'daily_full': {k: dict(v) for k,v in daily.items()},
        'split': {
            'daily_all': {k: dict(v) for k,v in daily_all.items()},
            'daily_fw':  {k: dict(v) for k,v in daily_fw.items()},
            'daily_wl':  {k: dict(v) for k,v in daily_wl.items()},
            'dow_all': dict(dow_all), 'dow_fw': dict(dow_fw), 'dow_wl': dict(dow_wl),
            'hm_all': dict(hm_all),   'hm_fw':  dict(hm_fw),  'hm_wl':  dict(hm_wl),
            'src_all': dict(src_all), 'src_fw': dict(src_fw),  'src_wl': dict(src_wl),
            'nc_dow_all': dict(nc_dow_all), 'nc_dow_fw': dict(nc_dow_fw), 'nc_dow_wl': dict(nc_dow_wl),
            'nc_hr_all':  dict(nc_hr_all),  'nc_hr_fw':  dict(nc_hr_fw),  'nc_hr_wl':  dict(nc_hr_wl),
            'missed_all': missed_log,
            'agents': {k: dict(v) for k,v in agent_stats.items()}
        },
        'marketing': {
            'disposition':  dict(disposition),
            'disp_by_fac':  {k: dict(v) for k,v in disp_by_fac.items()},
            'sale_daily':   {k: dict(v) for k,v in sale_daily.items()},
            'pages':        dict(pages.most_common(12)),
            'refs':         dict(refs.most_common(10)),
            'notes':        notes
        },
        'recordings': recordings,
        'call_log':   call_log,
        'forms':      forms_clean
    }

if __name__ == '__main__':
    print("Loading raw data...")
    with open('ctm_calls_raw.json') as f:     calls = json.load(f)
    with open('ctm_forms_all_final.json') as f: forms_raw = json.load(f)

    print(f"Processing {len(calls)} calls + {len(forms_raw)} forms...")
    result = process_all(calls, forms_raw)

    # Save individual files
    sep = (',',':')
    with open('data_daily_full.json','w') as f: json.dump(result['daily_full'],  f, separators=sep)
    with open('data_split.json','w')      as f: json.dump(result['split'],       f, separators=sep)
    with open('data_marketing.json','w')  as f: json.dump(result['marketing'],   f, separators=sep)
    with open('data_recordings.json','w') as f: json.dump(result['recordings'],  f, separators=sep)
    with open('data_log.json','w')        as f: json.dump(result['call_log'],    f, separators=sep)
    with open('data_forms.json','w')      as f: json.dump(result['forms'],       f, separators=sep)

    for fname in ['data_daily_full.json','data_split.json','data_marketing.json',
                  'data_recordings.json','data_log.json','data_forms.json']:
        import os
        sz = os.path.getsize(fname)
        print(f"  {fname}: {sz//1024} KB")

    print("Done.")
