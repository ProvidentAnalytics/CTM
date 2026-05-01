"""
refresh_dashboard.py
Self-contained pipeline: Pull from CTM → Process → Score → Build → Push to GitHub.
Run this in GitHub Actions or locally — no other Python files needed beyond the template.
"""
import urllib.request, json, time, os, sys, re
from datetime import datetime, timezone
from collections import defaultdict, Counter

# ── CONFIG ────────────────────────────────────────────────────────────
CTM_AUTH = os.environ.get('CTM_AUTH', '')
GH_TOKEN = os.environ.get('GH_TOKEN', '')
OWNER    = 'ProvidentAnalytics'
REPO     = 'Strive'
BASE     = 'https://api.calltrackingmetrics.com/api/v1/accounts/559323'
START    = '2026-01-01'
END      = datetime.now().strftime('%Y-%m-%d')

# ── 1. PULL FROM CTM ──────────────────────────────────────────────────

def fetch(page, direction=''):
    dir_p = f'&direction={direction}' if direction else ''
    url = f'{BASE}/calls?format=json&page_size=100&start_date={START}&end_date={END}&page={page}{dir_p}'
    req = urllib.request.Request(url, headers={'Authorization': f'Basic {CTM_AUTH}', 'User-Agent': 'Strive/2.0'})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def pull_all(direction=''):
    d = fetch(1, direction)
    items = list(d.get('calls', []))
    print(f'  {direction or "all"}: {d["total_entries"]} items, {d["total_pages"]} pages', flush=True)
    for page in range(2, d['total_pages'] + 1):
        for attempt in range(3):
            try:
                d2 = fetch(page, direction)
                items.extend(d2.get('calls', []))
                break
            except Exception as e:
                if attempt == 2: print(f'  page {page} failed: {e}')
                time.sleep(3)
        time.sleep(0.1)
    return items

print(f'[1/5] Pulling CTM data {START} to {END}...', flush=True)
calls = pull_all()
forms_raw = pull_all('form')
print(f'  Calls: {len(calls)} | Forms: {len(forms_raw)}', flush=True)

# ── 2. PROCESS ────────────────────────────────────────────────────────

print('[2/5] Processing data...', flush=True)

def fac_of(c):
    return 'Fort Wayne' if (c.get('tracking_number_bare','') or '').startswith('260') else 'Waterloo'

# Daily aggregations
daily_full = {}
for c in calls:
    d = c['called_at'][:10]
    if d not in daily_full:
        daily_full[d] = {'total':0,'inbound':0,'outbound':0,'missed':0,'new':0,'yes':0,'no':0,'lead':0,
                         'fw_total':0,'fw_inbound':0,'fw_outbound':0,'fw_missed':0,'fw_new':0,
                         'wl_total':0,'wl_inbound':0,'wl_outbound':0,'wl_missed':0,'wl_new':0,
                         'src':{}}
    r = daily_full[d]
    fac = fac_of(c); fp = 'fw_' if fac=='Fort Wayne' else 'wl_'
    r['total']+=1; r[fp+'total']+=1
    if c['direction']=='inbound':
        r['inbound']+=1; r[fp+'inbound']+=1
        src = c.get('source','Unknown') or 'Unknown'
        r['src'][src] = r['src'].get(src, 0) + 1
    elif c['direction']=='outbound':
        r['outbound']+=1; r[fp+'outbound']+=1
    if c.get('dial_status') in ('no-answer','busy','no answer'):
        r['missed']+=1; r[fp+'missed']+=1
    if c.get('is_new_caller') and c['direction']=='inbound':
        r['new']+=1; r[fp+'new']+=1
    sale = (c.get('sale') or {}).get('name','') or ''
    if sale=='Yes': r['yes']+=1
    elif sale=='No': r['no']+=1
    elif 'lead' in sale.lower(): r['lead']+=1

# Split data
dow_all, dow_fw, dow_wl = Counter(), Counter(), Counter()
agents = defaultdict(lambda: {'total':0,'fw':0,'wl':0,'inbound':0,'outbound':0,'answered':0,'missed':0,'dur':0,'dur_n':0})
missed_all = []
for c in calls:
    day = c.get('day','')
    if day:
        dow_all[day]+=1
        if fac_of(c)=='Fort Wayne': dow_fw[day]+=1
        else: dow_wl[day]+=1
    ag = (c.get('agent') or {}).get('name','')
    if ag:
        s = agents[ag]
        s['total']+=1
        if fac_of(c)=='Fort Wayne': s['fw']+=1
        else: s['wl']+=1
        if c['direction']=='inbound': s['inbound']+=1
        if c['direction']=='outbound': s['outbound']+=1
        st = c.get('dial_status',''); dur = c.get('duration',0) or 0
        if st in ('answered','completed'):
            s['answered']+=1; s['dur']+=dur; s['dur_n']+=1
        elif st in ('no-answer','busy','no answer'):
            s['missed']+=1
    if c['direction']=='inbound' and c.get('dial_status') in ('no-answer','busy','no answer'):
        missed_all.append({
            'time': c['called_at'][:16],
            'caller': c.get('caller_number_format','') + (' / ' + c.get('name','') if c.get('name','') else ''),
            'source': c.get('source','Unknown') or 'Unknown',
            'fa': fac_of(c)
        })
missed_all = missed_all[:80]

split = {'dow_all':dict(dow_all),'dow_fw':dict(dow_fw),'dow_wl':dict(dow_wl),
         'agents':{k:dict(v) for k,v in agents.items()},
         'missed_all':missed_all}

# Marketing
disp = Counter(); disp_by_fac = {'Fort Wayne':Counter(),'Waterloo':Counter()}
pages = Counter(); refs = Counter(); notes = []
for c in calls:
    cf = c.get('custom_fields') or {}
    d = cf.get('disposition_save_to_contact','')
    if d:
        disp[d]+=1
        disp_by_fac[fac_of(c)][d]+=1
    pg = c.get('landing_page','')
    if pg: pages[pg[:80]]+=1
    rf = c.get('referrer','')
    if rf:
        try:
            from urllib.parse import urlparse
            host = urlparse(rf).netloc
            if host: refs[host]+=1
        except: pass
    note = (cf.get('agent_notes','') or '').strip()
    if note:
        notes.append({'dt': c['called_at'][:16], 'ag':(c.get('agent') or {}).get('name',''), 'fa':fac_of(c), 'n':note[:1500]})

marketing = {
    'disposition': dict(disp),
    'disp_by_fac': {k:dict(v) for k,v in disp_by_fac.items()},
    'pages': dict(pages.most_common(20)),
    'refs': dict(refs.most_common(15)),
    'notes': sorted(notes, key=lambda x: x['dt'], reverse=True)[:200]
}

# Recordings & call log
recordings = []
log = []
for c in calls:
    if c['direction']=='form': continue
    fac = fac_of(c)
    # IVR routing: admissions / building / direct / outbound
    if c['direction'] == 'outbound':
        ivr = 'outbound'
    else:
        inputs = c.get('inputs', {}) or {}
        ivr = 'direct'
        if isinstance(inputs, dict):
            menu = inputs.get('menu_inputs', []) or []
            if menu:
                first = str(menu[0])
                if first == '1': ivr = 'admissions'
                elif first == '2': ivr = 'building'
        if ivr == 'direct':
            cp = c.get('call_path', []) or []
            if isinstance(cp, list):
                for hop in cp:
                    if isinstance(hop, dict):
                        rn = (hop.get('route_name','') or '').lower()
                        if 'press 1' in rn or ('admissions' in rn and 'press' in rn): ivr = 'admissions'; break
                        if 'press 2' in rn: ivr = 'building'; break
    log.append({
        'dt': c['called_at'][:16],
        'di': c['direction'],
        'cl': c.get('caller_number_format',''),
        'nm': (c.get('name','') or c.get('cnam','') or '')[:40],
        'ci': c.get('city','') or '',
        'sa': c.get('state','') or '',
        'sr': (c.get('source','') or '')[:50],
        'ag': (c.get('agent') or {}).get('name','') or '',
        'du': c.get('duration',0) or 0,
        'st': c.get('dial_status','') or 'unknown',
        'fa': fac,
        'nw': bool(c.get('is_new_caller')),
        'tr': bool((c.get('transcription_text','') or '').strip()),
        'ivr': ivr
    })
    if c['direction']=='inbound' and (c.get('transcription_text','') or '').strip():
        recordings.append({
            't': c['called_at'][:16],
            'c': c.get('name','') or c.get('cnam','') or '',
            'p': c.get('caller_number_format',''),
            'a': (c.get('agent') or {}).get('name','') or '',
            'd': f"{(c.get('duration',0) or 0)//60}m {(c.get('duration',0) or 0)%60}s",
            's': c.get('source','Unknown') or 'Unknown',
            'f': fac,
            'su': (c.get('summary','') or '')[:400],
            'tr': (c.get('transcription_text','') or '')[:1200],
            'au': c.get('audio','')
        })

# Forms
forms = []
for f in forms_raw:
    fd = f.get('form',{}) or {}
    hw = next((c.get('value','') for c in (fd.get('custom',[]) or []) if c.get('id')=='how_can_we_help'), '')
    forms.append({
        'dt': f['called_at'][:16],
        'n': (f.get('name','') or '')[:40],
        'e': f.get('email','') or '',
        'ph': f.get('caller_number_format','') or '',
        'ci': f.get('city','') or '',
        'st': f.get('state','') or '',
        'sr': f.get('source','') or '',
        'fa': fac_of(f),
        'fn': fd.get('form_name','') or '',
        'nw': bool(f.get('is_new_caller')),
        'hw': (hw or '').strip()
    })

# Extra: per-agent daily, returning, new caller breakdowns
agent_daily = defaultdict(lambda: defaultdict(lambda: {'total':0,'inbound':0,'outbound':0,'answered':0,'missed':0,'dur':0,'dur_n':0}))
returning_daily = Counter()
nc_daily = defaultdict(lambda: {'dow':Counter(),'hour':Counter(),'src':Counter()})
hm_daily = defaultdict(lambda: defaultdict(int))

for c in calls:
    ag = (c.get('agent') or {}).get('name',''); d = c['called_at'][:10]
    if ag:
        s = agent_daily[ag][d]
        s['total']+=1
        if c['direction']=='inbound': s['inbound']+=1
        if c['direction']=='outbound': s['outbound']+=1
        st = c.get('dial_status',''); dur = c.get('duration',0) or 0
        if st in ('answered','completed'): s['answered']+=1; s['dur']+=dur; s['dur_n']+=1
        elif st in ('no-answer','busy','no answer'): s['missed']+=1
    if c['direction']=='inbound' and not c.get('is_new_caller'):
        returning_daily[d]+=1
    if c.get('is_new_caller') and c['direction']=='inbound':
        dd = nc_daily[d]
        dd['dow'][c.get('day','')]+=1
        dd['hour'][c.get('hour','')]+=1
        dd['src'][c.get('source','Unknown') or 'Unknown']+=1
    if c['direction']=='inbound':
        fp = 'fw' if fac_of(c)=='Fort Wayne' else 'wl'
        day = c.get('day',''); hr = c.get('hour','')
        if day and hr:
            hm_daily[d][f'{day}|{hr}']+=1
            hm_daily[d][f'_fac_{fp}_{day}|{hr}']+=1

extra = {
    'agent_daily': {ag:{d:dict(v) for d,v in days.items()} for ag,days in agent_daily.items()},
    'returning_daily': dict(returning_daily),
    'nc_daily_detail': {d:{'dow':dict(v['dow']),'hour':dict(v['hour']),'src':dict(v['src'])} for d,v in nc_daily.items()}
}
hmd = {d:dict(v) for d,v in hm_daily.items()}

# ── 3. SCORE CALLS ────────────────────────────────────────────────────

print('[3/5] Scoring calls...', flush=True)

RAPPORT_POSITIVE = [r'\bI understand\b', r'\bI hear you\b', r'\bI\'m sorry\b', r'\bI know that\b',
    r'\bthat must\b', r'\bI can imagine\b', r'\bwe\'re here\b', r'\byou\'re not alone\b',
    r'\btake your time\b', r'\bit\'s ok\b', r"\bit's okay\b", r'\bthat\'s great\b',
    r'\bgood for you\b', r'\bI appreciate\b', r'\bthank you for\b',
    r'\bI\'m glad\b', r"\bbrave\b", r'\bproud of you\b', r'\bwe got you\b',
    r'\bdon\'t worry\b', r'\babsolutely\b', r'\bof course\b']
RAPPORT_NEGATIVE = [r'\bcalm down\b', r'\bjust listen\b', r'\bwhatever\b', r'\blike I said\b', r'\bI told you\b']
NEEDS = [r'\bsubstance\b', r'\balcohol\b', r'\bopioid\b', r'\bheroin\b', r'\bfentanyl\b',
    r'\bcocaine\b', r'\bmeth\b', r'\bbenzo\b', r'\bxanax\b', r'\bperc[ao]cet\b',
    r'\bsuboxone\b', r'\bmethadone\b', r'\bvivitrol\b', r'\bdetox\b', r'\binpatient\b',
    r'\boutpatient\b', r'\bintensive outpatient\b', r'\biop\b', r'\bphp\b',
    r'\bhow long have you\b', r'\bwhen did you last\b', r'\bdaily use\b',
    r'\bwithdrawal\b', r'\bhistory of\b', r'\bprior treatment\b',
    r'\bmental health\b', r'\bdepression\b', r'\banxiety\b', r'\bptsd\b',
    r'\bsuicid', r'\bhomicid', r'\bself.harm\b']
INSURANCE = [r'\binsurance\b', r'\bmedicaid\b', r'\bmedicare\b', r'\bblue cross\b', r'\bbcbs\b',
    r'\baetna\b', r'\bcigna\b', r'\bunited\b', r'\bhumana\b', r'\banthem\b',
    r'\bmember id\b', r'\bgroup number\b', r'\bpolicy\b', r'\bcoverage\b',
    r'\bself.pay\b', r'\bout.of.pocket\b', r'\bdeductible\b', r'\bcopay\b',
    r'\bprior auth\b', r'\bauthorization\b', r'\bverify your benefits\b',
    r'\bdo you have insurance\b', r'\bwhat insurance\b']
NEXT_STEPS = [r'\bI\'ll call you back\b', r'\bcall you back\b', r'\bcall back\b',
    r'\bschedule\b', r'\bappointment\b', r'\btomorrow\b', r'\bmonday\b', r'\btuesday\b',
    r'\bwednesday\b', r'\bthursday\b', r'\bfriday\b', r'\bnext week\b',
    r'\bbring with you\b', r'\bdirections\b', r'\baddress\b', r'\bcheck.?in\b',
    r'\bintake\b', r'\bassessment\b', r'\b\d+\s?(am|pm)\b', r'\bo\'clock\b',
    r'\bpre.?assessment\b', r'\badmission\b']
COMPLIANCE_FLAGS = [r'\bsocial security\b.{0,40}\b\d{3}.\d{2}.\d{4}\b']
COMPLIANCE_OK = [r'\bconfidential\b', r'\bprivacy\b', r'\bhipaa\b', r'\bconsent\b',
    r'\brelease of information\b', r'\bthis call may be recorded\b']
URGENCY = [r'\bemergency\b', r'\bcrisis\b', r'\bsuicid', r'\boverdos',
    r'\bdetoxing\b', r'\bwithdrawing\b', r'\bneed help now\b', r'\btonight\b']
OB_OPENING = [r'\bthis is\b.{0,40}\bfrom\b', r'\bcalling from strive\b', r'\bcalling from\b',
    r'\bfollow.?up\b', r'\bchecking in\b', r'\breaching out\b', r'\breturning your call\b']
OB_CLOSE = [r'\bschedule\b', r'\bappointment\b', r'\btomorrow\b', r'\btoday at\b',
    r'\bgive me a call\b', r"\bI'll call\b"]

def cm(text, pats): return sum(1 for p in pats if re.search(p, text, re.IGNORECASE))

# Conversion Discovery patterns — for Admissions Inquiry calls (Press 1 but not Full Intake)
CONVERSION_DISCOVERY = [
    r"\bwhat\s+(brings|brought)\s+you\b", r"\bwhat\s+can\s+I\s+(help|do)\b",
    r"\btell me (about|more|what)\b", r"\bare you\s+(the|calling for)\b",
    r"\bis (this|it)\s+for\s+(you|yourself)\b", r"\bcan I (ask|get)\b",
    r"\bwhat\s+(kind|type)\s+of\b", r"\bhow\s+can\s+(I|we)\s+help\b",
    r"\blet me\s+(get|connect|transfer)\s+you\b", r"\btransfer you to\b",
    r"\bI can\s+(have|get)\s+someone\b", r"\bwould you like\b",
    r"\bbest (number|time) to (reach|call)\b", r"\bcall (you|them) back\b",
    r"\bcan I\s+(take|get)\s+your\b", r"\bwhat\'s your\s+(name|number)\b"
]
INTAKE_CALLER_IS_PATIENT = [
    r"\bI need\b", r"\bI\'m struggling\b", r"\bI\'ve been (using|drinking)\b",
    r"\bI want\s+(to|help|treatment)\b", r"\bI\'m\s+(an?|in)\s+(addict|recovery)\b",
    r"\bI\'m calling for myself\b", r"\bget me\s+(in|help)\b"
]

def detect_ivr_route(call):
    """Determine if caller pressed 1 (admissions), 2 (building), or direct dial.
    Returns: 'admissions', 'building', or 'direct' """
    inputs = call.get('inputs', {}) or {}
    if isinstance(inputs, dict):
        menu = inputs.get('menu_inputs', []) or []
        if menu:
            first = str(menu[0]) if menu else ''
            if first == '1': return 'admissions'
            if first == '2': return 'building'
    # Check call_path for routing hints
    cp = call.get('call_path', []) or []
    if isinstance(cp, list):
        for hop in cp:
            if isinstance(hop, dict):
                rn = (hop.get('route_name','') or '').lower()
                if 'press 1' in rn or 'admissions' in rn:
                    return 'admissions'
                if 'press 2' in rn or 'building' in rn:
                    return 'building'
    return 'direct'

def detect_full_intake(transcript, ivr_route, duration):
    """Auto-detect Full Intake — no human input required.
    Full Intake = Admissions line + 3+ minutes + 3+ clinical signals """
    if ivr_route == 'building':
        return False
    if duration < 180:
        return False
    text = transcript.lower()

    # Count clinical depth signals (need 3+)
    signals = 0
    if cm(text, [r'\b(alcohol|opioid|heroin|fentanyl|cocaine|meth|benzo|xanax|perc[ao]cet|suboxone|methadone|drug|substance)\b']) >= 1:
        signals += 1
    if cm(text, [r'\b(detox|inpatient|outpatient|iop|php|residential|partial hospital)\b']) >= 1:
        signals += 1
    if cm(text, [r'\b(daily|every day|for\s+\d+\s+(years|months|weeks)|last (used|drank)|when did you last)\b']) >= 1:
        signals += 1
    if cm(text, [r'\b(prior treatment|been to treatment|previous (rehab|treatment)|history of)\b']) >= 1:
        signals += 1
    if cm(text, [r'\b(insurance|medicaid|medicare|self.pay|coverage|policy|deductible)\b']) >= 1:
        signals += 1
    if cm(text, [r'\b(when can|available|schedule|tomorrow|monday|tuesday|wednesday|thursday|friday|admit|admission)\b']) >= 1:
        signals += 1
    if cm(text, INTAKE_CALLER_IS_PATIENT) >= 1:
        signals += 1

    return signals >= 3

def split_sp(t):
    lines = t.split('\n')
    if len(lines)<2: return [], [t]
    a, c = [], []
    for ln in lines:
        ln = ln.strip()
        if not ln: continue
        m = re.match(r'^([^:]{1,30}):\s*(.*)$', ln)
        if m:
            sp, ct = m.group(1).lower(), m.group(2)
            if not ct: continue
            if (re.match(r'^[01]$', sp) or any(s in sp for s in ['britney','brittney','shreenda','gabrielle','louisse','irvin','heath','vadim','agent'])):
                a.append(ct)
            else:
                c.append(ct)
        else:
            (a if len(a)<=len(c) else c).append(ln)
    return (a, c) if (a or c) else ([], [t])

def analyze(c):
    t = (c.get('transcription_text','') or '').strip()
    if not t: return None
    dur = c.get('duration',0) or 0
    direction = c['direction']
    al, cl = split_sp(t)

    # IVR routing detection (only meaningful for inbound)
    ivr_route = detect_ivr_route(c) if direction == 'inbound' else 'outbound'

    # Voicemail detection
    is_vm = len(al)==0 or dur<45 or any(p in t.lower() for p in ['leave a message','after the tone','will get back','call you back at','callback at'])

    # Three-category classification
    # Category drives WHICH dimensions are scored
    if direction == 'outbound':
        category = 'outbound'
    elif is_vm:
        category = 'voicemail'
    elif ivr_route == 'building':
        category = 'non_admissions'
    elif detect_full_intake(t, ivr_route, dur):
        category = 'full_intake'
    else:
        category = 'admissions_inquiry'

    # Type label (kept for backward-compat with existing UI)
    if is_vm:
        ct = 'voicemail_out' if direction=='outbound' else 'voicemail'
    elif dur<90 and len(t)<600:
        ct = 'quick_followup' if direction=='outbound' else 'quick_inquiry'
    elif dur>=180 or len(t)>1500:
        ct = 'consultation_out' if direction=='outbound' else 'consultation'
    else:
        ct = 'standard_out' if direction=='outbound' else 'standard'

    # Score all dimensions (we'll selectively include them based on category)
    al_text = ' '.join(al).lower()
    rapport_n, needs_n, ins_n, next_n, comp_n, conv_n = [], [], [], [], [], []

    # Rapport (always scored)
    pos = cm(al_text, RAPPORT_POSITIVE); neg = cm(al_text, RAPPORT_NEGATIVE)
    rapport_s = max(0, min(10, 5 + ((pos*1.5) - (neg*2)) * 0.7)) if al_text else 5.0
    if pos>=3: rapport_n.append('Strong empathetic language')
    if pos==0 and al_text: rapport_n.append('No empathy phrases detected')

    # Needs Assessment
    nm = cm(t.lower(), NEEDS)
    needs_s = min(10, nm * 1.2)
    if nm>=6: needs_n.append('Comprehensive needs assessment')
    elif nm<2: needs_n.append('Limited needs assessment')

    # Insurance
    im = cm(t.lower(), INSURANCE)
    ins_s_raw = min(10, im * 1.5)
    if im==0: ins_n.append('Insurance not addressed')
    elif im>=4: ins_n.append('Insurance verified thoroughly')

    # Next Steps
    sm = cm(t.lower(), NEXT_STEPS)
    has_time = bool(re.search(r'\b\d+\s?(am|pm)\b', t.lower()))
    has_day = bool(re.search(r'\b(monday|tuesday|wednesday|thursday|friday|tomorrow|today)\b', t.lower()))
    next_s = min(10, sm*0.8 + (3 if has_time and has_day else 1.5 if has_time or has_day else 0) + (2 if 'schedule' in t.lower() or 'appointment' in t.lower() else 0))
    if has_time and has_day: next_n.append('Concrete time + day given')
    elif sm<2: next_n.append('Vague next steps')

    # Compliance — DEFAULT TO N/A unless signals trigger
    flags = cm(t, COMPLIANCE_FLAGS)
    ok = cm(t.lower(), COMPLIANCE_OK)
    if flags == 0 and ok == 0:
        comp_s_raw = None  # N/A — no signal either way
    else:
        comp_s_raw = max(0, min(10, 8 - flags*3 + ok*0.5))
        if flags>0: comp_n.append('WARNING: PHI exposure risk')
        if ok>0: comp_n.append('Compliance language used')

    # Conversion Discovery — for Admissions Inquiry calls
    cd_matches = cm(al_text, CONVERSION_DISCOVERY)
    conv_s = min(10, cd_matches * 1.2) if al_text else 0
    if cd_matches >= 4: conv_n.append('Strong discovery and conversion attempt')
    elif cd_matches >= 2: conv_n.append('Adequate discovery questions')
    elif cd_matches < 1 and al_text: conv_n.append('No discovery questions — missed conversion opportunity')

    # Outbound bonuses
    if direction=='outbound':
        op = cm(t.lower(), OB_OPENING); cl_m = cm(t.lower(), OB_CLOSE)
        rapport_s = min(10, rapport_s + op*0.8)
        next_s = min(10, next_s + cl_m*1.0)
        if op >= 1: rapport_n.append('Identified self/company on opening')
        if cl_m >= 2: next_n.append('Strong call-to-action close')

    # ─── CATEGORY-DRIVEN SCORING ───────────────────────────────────────
    # Each category has different applicable dimensions and weights
    scored_dims = {}  # only includes dimensions that actually apply

    if category == 'voicemail':
        # Inbound voicemail — caller-side only, agent didn't take the call
        # Only score what's possible to evaluate from a voicemail
        rapport_s = max(rapport_s, 6.0)
        scored_dims = {'rapport': rapport_s, 'needs': needs_s, 'next_steps': next_s + 3}
        if comp_s_raw is not None: scored_dims['compliance'] = comp_s_raw
        rapport_n.append('Inbound voicemail (caller-side only)')

    elif category == 'non_admissions':
        # Press 2 / Building — receptionist or BHT routing
        # Insurance N/A; Conversion Discovery N/A; just rapport, next steps (routing), compliance
        scored_dims = {'rapport': rapport_s, 'next_steps': next_s}
        if comp_s_raw is not None: scored_dims['compliance'] = comp_s_raw

    elif category == 'admissions_inquiry':
        # Press 1, but not Full Intake — score Conversion Discovery instead of Insurance
        scored_dims = {
            'rapport': rapport_s,
            'needs': needs_s,
            'conversion': conv_s,
            'next_steps': next_s
        }
        if comp_s_raw is not None: scored_dims['compliance'] = comp_s_raw

    elif category == 'full_intake':
        # Press 1, real intake conversation — full 5-dimension framework
        scored_dims = {
            'rapport': rapport_s,
            'needs': needs_s,
            'insurance': ins_s_raw,
            'next_steps': next_s
        }
        if comp_s_raw is not None: scored_dims['compliance'] = comp_s_raw

    elif category == 'outbound':
        # Outbound — staff initiating; emphasis on opening + close + clear next steps
        scored_dims = {'rapport': rapport_s, 'next_steps': next_s}
        if needs_s >= 2: scored_dims['needs'] = needs_s  # only count needs if actually probed
        if im >= 2: scored_dims['insurance'] = ins_s_raw  # only if actually discussed
        if comp_s_raw is not None: scored_dims['compliance'] = comp_s_raw

    # Overall = average of scored dimensions only (N/A excluded entirely, not zeroed)
    if scored_dims:
        overall = sum(scored_dims.values()) / len(scored_dims)
    else:
        overall = 5.0

    # Outcome boosters
    dispo = (c.get('custom_fields') or {}).get('disposition_save_to_contact','') or ''
    sale = (c.get('sale') or {}).get('name','') or ''
    if 'scheduled admission' in dispo.lower(): overall = min(10, overall + 1.5)
    if sale=='Yes': overall = min(10, overall + 1.0)

    # Build scores object — N/A for dimensions not scored
    def na_or(key, raw):
        return round(min(10, raw), 1) if key in scored_dims else 'N/A'

    scores = {
        'rapport':    round(min(10, rapport_s), 1) if 'rapport' in scored_dims else 'N/A',
        'needs':      round(min(10, needs_s), 1) if 'needs' in scored_dims else 'N/A',
        'insurance':  round(min(10, ins_s_raw), 1) if 'insurance' in scored_dims else 'N/A',
        'conversion': round(min(10, conv_s), 1) if 'conversion' in scored_dims else 'N/A',
        'next_steps': round(min(10, next_s), 1) if 'next_steps' in scored_dims else 'N/A',
        'compliance': round(min(10, comp_s_raw), 1) if (comp_s_raw is not None and 'compliance' in scored_dims) else 'N/A',
        'overall':    round(min(10, overall), 1)
    }

    # Recommendations
    worked, didnt, better = [], [], []
    if scores['rapport'] != 'N/A':
        if scores['rapport']>=7: worked.append('Strong empathetic tone established trust')
        elif scores['rapport']<5:
            didnt.append('Limited warmth — caller may have felt rushed')
            better.append('Open with: "I hear you, that sounds difficult. We\'re here to help."')
    if scores['needs'] != 'N/A':
        if scores['needs']>=7: worked.append('Thorough needs assessment captured key clinical info')
        elif scores['needs']<5 and category in ('full_intake', 'admissions_inquiry'):
            didnt.append('Insufficient probing on substance/frequency/treatment history')
            better.append('Ask: "When was your last use? How long? Have you been to treatment before?"')
    if scores['insurance'] != 'N/A':
        if scores['insurance']>=7: worked.append('Insurance verified, removed financial barrier')
        elif scores['insurance']<4:
            didnt.append('Insurance not addressed in a Full Intake call')
            better.append('Always confirm: "What insurance? Can I get the member ID and DOB?"')
    if scores['conversion'] != 'N/A':
        if scores['conversion']>=7: worked.append('Strong discovery questions, attempted to convert inquiry')
        elif scores['conversion']<4:
            didnt.append('Few discovery questions — caller intent not explored')
            better.append('Ask: "What brings you to call today? Is this for yourself or a loved one?"')
    if scores['next_steps'] != 'N/A':
        if scores['next_steps']>=7: worked.append('Concrete next step with time/day given')
        elif scores['next_steps']<5:
            didnt.append('Next steps too vague')
            better.append('State: "I will call you tomorrow at 10:00 AM. If you need us before, call (260) 261-2663."')
    if scores['compliance'] != 'N/A' and scores['compliance']<6:
        didnt.append('Compliance concerns detected')
    if 'scheduled admission' in dispo.lower(): worked.append('Successfully scheduled admission')
    if 'hung-up' in dispo.lower() or 'abandoned' in dispo.lower():
        didnt.append('Caller hung up before resolution')
    if sale=='No': didnt.append('Not converted to lead — review what was missed')
    elif sale=='Yes': worked.append('Successfully qualified as lead')

    return {
        'id': c.get('id'), 'dt': c['called_at'][:16], 'duration': dur,
        'agent': (c.get('agent') or {}).get('name','') or 'Unanswered',
        'caller': c.get('caller_number_format',''),
        'name': ((c.get('name','') or c.get('cnam','')) or '')[:30],
        'fac': fac_of(c), 'src': c.get('source',''),
        'dispo': dispo, 'sale': sale,
        'urgent': cm(t.lower(), URGENCY)>=2,
        'type': ct, 'dir': direction,
        'category': category,         # NEW: full_intake / admissions_inquiry / non_admissions / voicemail / outbound
        'ivr': ivr_route,             # NEW: admissions / building / direct / outbound
        'scores': scores,
        'notes': {'rapport':rapport_n,'needs':needs_n,'insurance':ins_n,'conversion':conv_n,'next_steps':next_n,'compliance':comp_n},
        'worked': worked, 'didnt': didnt, 'better': better,
        'summary': (c.get('summary','') or '')[:500],
        'transcript_preview': t[:600], 'transcript_full': t,
        'audio': c.get('audio','')
    }

call_analysis = []
for c in calls:
    if c['direction'] not in ('inbound','outbound'): continue
    a = analyze(c)
    if a: call_analysis.append(a)
print(f'  Scored {len(call_analysis)} calls', flush=True)

# ── 4. INJECT INTO TEMPLATE ───────────────────────────────────────────

print('[4/5] Building dashboard...', flush=True)
sep = (',', ':')
data_blobs = {
    'INJECT_SD':    json.dumps(split, separators=sep),
    'INJECT_DAILY': json.dumps(daily_full, separators=sep),
    'INJECT_RECS':  json.dumps(recordings, separators=sep),
    'INJECT_LOG':   json.dumps(log, separators=sep),
    'INJECT_FORMS': json.dumps(forms, separators=sep),
    'INJECT_MKT':   json.dumps(marketing, separators=sep),
    'INJECT_EXTRA': json.dumps(extra, separators=sep),
    'INJECT_HMD':   json.dumps(hmd, separators=sep),
    'INJECT_CA':    json.dumps(call_analysis, separators=sep),
}

with open('dashboard_template_v2.html') as f:
    html = f.read()
for key, blob in data_blobs.items():
    html = html.replace(f'/*{key}*/null', blob, 1)
print(f'  Built: {len(html)//1024} KB', flush=True)

# ── 5. PUSH TO GITHUB ─────────────────────────────────────────────────

print('[5/5] Pushing to GitHub...', flush=True)
gh_headers = {'Authorization':f'token {GH_TOKEN}','Accept':'application/vnd.github.v3+json',
              'User-Agent':'Strive-Refresh','Content-Type':'application/json'}

def gh(method, path, payload=None):
    url = f'https://api.github.com/repos/{OWNER}/{REPO}{path}'
    data = json.dumps(payload).encode() if payload else None
    req = urllib.request.Request(url, data=data, method=method, headers=gh_headers)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())

ref = gh('GET','/git/refs/heads/main')
latest = ref['object']['sha']
base_tree = gh('GET',f'/git/commits/{latest}')['tree']['sha']

now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
new_tree = gh('POST','/git/trees',{'base_tree':base_tree,'tree':[
    {'path':'ctm-calls/index.html','mode':'100644','type':'blob','content':html}
]})
new_commit = gh('POST','/git/commits',{
    'message':f'Auto-refresh: {now} ({len(calls)} calls, {len(call_analysis)} scored)',
    'tree':new_tree['sha'],'parents':[latest]
})
gh('PATCH','/git/refs/heads/main',{'sha':new_commit['sha']})
print(f'  Pushed: {new_commit["sha"][:10]} at {now}', flush=True)
print('Done.', flush=True)
