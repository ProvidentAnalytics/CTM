"""
score_calls.py
Scores all CTM transcripts using treatment-center industry standard framework.
Outputs data_call_analysis.json for the dashboard.

Framework dimensions (each scored 0-10):
1. Rapport — warmth, empathy, active listening
2. Needs Assessment — identifying caller's situation, substance, urgency
3. Insurance Verification — confirming coverage, payer, eligibility
4. Next Steps Clarity — concrete actions, callback times, scheduled steps
5. Compliance — HIPAA awareness, no PHI mishandling, proper disclosures
"""
import json
import re
from collections import defaultdict, Counter

# ── Pattern Banks ─────────────────────────────────────────────────────

RAPPORT_POSITIVE = [
    r'\bI understand\b', r'\bI hear you\b', r'\bI\'m sorry\b', r'\bI know that\b',
    r'\bthat must\b', r'\bI can imagine\b', r'\bwe\'re here\b', r'\byou\'re not alone\b',
    r'\btake your time\b', r'\bit\'s ok\b', r"\bit's okay\b", r'\bthat\'s great\b',
    r'\bgood for you\b', r'\bI appreciate\b', r'\bthank you for\b',
    r'\bI\'m glad\b', r"\bbrave\b", r'\bproud of you\b', r'\bwe got you\b',
    r'\bdon\'t worry\b', r'\babsolutely\b', r'\bof course\b'
]
RAPPORT_NEGATIVE = [
    r'\bcalm down\b', r'\bjust listen\b', r'\bwhatever\b',
    r'\blike I said\b', r'\bI told you\b'
]

NEEDS_ASSESSMENT = [
    r'\bsubstance\b', r'\balcohol\b', r'\bopioid\b', r'\bheroin\b', r'\bfentanyl\b',
    r'\bcocaine\b', r'\bmeth\b', r'\bbenzo\b', r'\bxanax\b', r'\bperc[ao]cet\b',
    r'\bsuboxone\b', r'\bmethadone\b', r'\bvivitrol\b',
    r'\bdetox\b', r'\binpatient\b', r'\boutpatient\b', r'\bintensive outpatient\b',
    r'\biop\b', r'\bphp\b', r'\bpartial hospital',
    r'\bhow long have you\b', r'\bwhen did you last\b', r'\bdaily use\b',
    r'\bwithdrawal\b', r'\bhistory of\b', r'\bprior treatment\b',
    r'\bmental health\b', r'\bdepression\b', r'\banxiety\b', r'\bptsd\b',
    r'\bsuicid', r'\bhomicid', r'\bself.harm\b'
]

INSURANCE_VERIFICATION = [
    r'\binsurance\b', r'\bmedicaid\b', r'\bmedicare\b', r'\bblue cross\b', r'\bbcbs\b',
    r'\baetna\b', r'\bcigna\b', r'\bunited\b', r'\bhumana\b', r'\banthem\b',
    r'\bmember id\b', r'\bgroup number\b', r'\bpolicy\b', r'\bcoverage\b',
    r'\bself.pay\b', r'\bout.of.pocket\b', r'\bdeductible\b', r'\bcopay\b',
    r'\bprior auth\b', r'\bauthorization\b', r'\bverify your benefits\b',
    r'\bdo you have insurance\b', r'\bwhat insurance\b'
]

NEXT_STEPS = [
    r'\bI\'ll call you back\b', r'\bcall you back\b', r"\bcall back\b",
    r'\bschedule\b', r'\bappointment\b', r'\btomorrow\b', r'\bmonday\b', r'\btuesday\b',
    r'\bwednesday\b', r'\bthursday\b', r'\bfriday\b', r'\bnext week\b',
    r'\bbring with you\b', r'\bwhat to bring\b', r'\bdirections\b', r'\baddress\b',
    r'\barrival\b', r'\bcheck.?in\b', r'\bintake\b', r'\bassessment\b',
    r'\b\d+\s?(am|pm)\b', r'\bo\'clock\b', r'\bemergency contact\b',
    r'\bpre.?assessment\b', r'\badmission\b'
]

COMPLIANCE_RED_FLAGS = [
    r'\bsocial security\b.*\b\d{3}.\d{2}.\d{4}\b',  # SSN exposed
    r'\bdate of birth\b.*\b\d{1,2}.\d{1,2}.\d{2,4}\b',  # DOB read aloud
    r'\bI\'m not allowed\b', r'\bI can\'t share\b'
]

COMPLIANCE_POSITIVE = [
    r'\bconfidential\b', r'\bprivacy\b', r'\bhipaa\b',
    r'\bconsent\b', r'\bauthorize\b', r'\brelease of information\b',
    r'\brecorded for quality\b', r'\bthis call may be recorded\b'
]

URGENCY_INDICATORS = [
    r'\bemergency\b', r'\bcrisis\b', r'\bsuicid', r'\boverdos',
    r'\bdetoxing\b', r'\bwithdrawing\b', r'\bin pain\b', r'\bcan\'t stop\b',
    r'\bneed help now\b', r'\btoday\b', r'\btonight\b', r'\bright now\b'
]

# ── Scoring Functions ─────────────────────────────────────────────────

def count_matches(text, patterns):
    text = text.lower()
    return sum(1 for p in patterns if re.search(p, text, re.IGNORECASE))

def score_rapport(transcript, agent_lines):
    """Rapport: warmth in agent's actual lines"""
    if not agent_lines:
        return 5.0, []
    agent_text = ' '.join(agent_lines).lower()
    pos = count_matches(agent_text, RAPPORT_POSITIVE)
    neg = count_matches(agent_text, RAPPORT_NEGATIVE)
    raw = (pos * 1.5) - (neg * 2)
    score = max(0, min(10, 5 + raw * 0.7))
    notes = []
    if pos >= 3: notes.append('Strong empathetic language')
    if pos == 0: notes.append('No empathy phrases detected')
    if neg > 0:  notes.append(f'{neg} dismissive phrases used')
    return score, notes

def score_needs(transcript):
    text = transcript.lower()
    matches = count_matches(text, NEEDS_ASSESSMENT)
    score = min(10, matches * 1.2)
    notes = []
    if matches >= 6:  notes.append('Comprehensive needs assessment')
    elif matches >= 3: notes.append('Adequate needs probing')
    elif matches < 2:  notes.append('Limited needs assessment')
    if any(re.search(p, text) for p in [r'\bsuicid', r'\bhomicid']):
        notes.append('CRISIS: Risk indicators discussed')
    return score, notes

def score_insurance(transcript):
    text = transcript.lower()
    matches = count_matches(text, INSURANCE_VERIFICATION)
    score = min(10, matches * 1.5)
    notes = []
    if matches >= 4: notes.append('Insurance verified thoroughly')
    elif matches >= 2: notes.append('Basic insurance discussed')
    elif matches == 0: notes.append('Insurance not addressed')
    return score, notes

def score_next_steps(transcript):
    text = transcript.lower()
    matches = count_matches(text, NEXT_STEPS)
    has_time   = bool(re.search(r'\b\d+\s?(am|pm)\b|\bo\'clock\b', text))
    has_day    = bool(re.search(r'\b(monday|tuesday|wednesday|thursday|friday|tomorrow|today)\b', text))
    has_action = bool(re.search(r'\b(schedule|appointment|callback|call you back|admission|intake)\b', text))
    score = min(10, matches * 0.8 + (3 if has_time and has_day else 1.5 if has_time or has_day else 0) + (2 if has_action else 0))
    notes = []
    if has_time and has_day:  notes.append('Concrete time + day given')
    elif has_action:           notes.append('Clear next action stated')
    else:                       notes.append('Vague next steps')
    return score, notes

def score_compliance(transcript):
    text = transcript
    flags = count_matches(text, COMPLIANCE_RED_FLAGS)
    pos   = count_matches(text.lower(), COMPLIANCE_POSITIVE)
    score = max(0, 8 - flags * 3 + pos * 0.5)
    score = min(10, score)
    notes = []
    if flags > 0: notes.append(f'WARNING: {flags} potential PHI exposure')
    if pos > 0:   notes.append('Compliance language used appropriately')
    return score, notes

def detect_urgency(transcript):
    text = transcript.lower()
    matches = count_matches(text, URGENCY_INDICATORS)
    return matches >= 2

def split_speakers(transcript):
    """Try to split transcript into agent vs caller lines.
    CTM transcripts often look like:
        Agent: text
        Caller Name: text
    Or numbered like "0: text" "1: text"
    """
    lines = transcript.split('\n')
    agent_lines, caller_lines = [], []
    if len(lines) < 2:
        # Single block: just use full text as both
        return [], [transcript]

    # Try pattern: "Name:" or "0:" prefix
    for line in lines:
        line = line.strip()
        if not line: continue
        match = re.match(r'^([^:]{1,30}):\s*(.*)$', line)
        if match:
            speaker = match.group(1).lower()
            content = match.group(2)
            if not content: continue
            # Heuristic: if speaker is short like "0" or contains "agent"/staff name → agent
            if (re.match(r'^[01]$', speaker) or
                any(s in speaker for s in ['britney','brittney','shreenda','gabrielle','louisse','irvin','heath','agent'])):
                agent_lines.append(content)
            else:
                caller_lines.append(content)
        else:
            # No speaker label — alternate as best guess
            (agent_lines if len(agent_lines) <= len(caller_lines) else caller_lines).append(line)

    if not agent_lines and not caller_lines:
        return [], [transcript]
    return agent_lines, caller_lines

# ── Recommendations ───────────────────────────────────────────────────

def generate_recommendations(scores, transcript, summary, dispo, sale):
    """What worked / didn't / could've been better"""
    worked, didnt, better = [], [], []

    if scores['rapport'] >= 7:
        worked.append('Strong empathetic tone established trust quickly')
    elif scores['rapport'] < 5:
        didnt.append('Limited warmth — caller may have felt rushed or dismissed')
        better.append('Open with: "I hear you, that sounds really difficult. We\'re here to help."')

    if scores['needs'] >= 7:
        worked.append('Thorough needs assessment captured key clinical info')
    elif scores['needs'] < 5:
        didnt.append('Insufficient probing on substance, frequency, or treatment history')
        better.append('Ask: "When was your last use? How long has this been going on? Have you been to treatment before?"')

    if scores['insurance'] >= 7:
        worked.append('Insurance verified, removed financial barrier')
    elif scores['insurance'] < 4:
        didnt.append('Insurance not addressed — risk of admission delay')
        better.append('Always confirm: "What insurance do you have? Can I get the member ID and date of birth?"')

    if scores['next_steps'] >= 7:
        worked.append('Concrete next step with specific time/day given')
    elif scores['next_steps'] < 5:
        didnt.append('Next steps too vague — caller may not follow through')
        better.append('State explicitly: "I will call you tomorrow at 10:00 AM. If you need us before then, call (260) 261-2663."')

    if scores['compliance'] < 6:
        didnt.append('Compliance concerns detected in transcript')
        better.append('Review for PHI exposure; reinforce HIPAA training')

    # Disposition-based notes
    if 'voicemail' in dispo and 'pre-assessment' not in dispo:
        didnt.append('Call ended at voicemail — opportunity for warm handoff missed')
    if 'scheduled admission' in dispo:
        worked.append('Successfully scheduled admission')
    if 'hung-up' in dispo or 'abandoned' in dispo:
        didnt.append('Caller hung up before resolution')
        better.append('Reduce hold time; if transferring, brief the receiving party first')

    if sale == 'No':
        didnt.append('Not converted to lead — review what gap caused the loss')
    elif sale == 'Yes':
        worked.append('Successfully qualified as lead')

    return worked, didnt, better

# ── Main ──────────────────────────────────────────────────────────────

def detect_call_type(transcript, duration, agent_lines, caller_lines, direction='inbound'):
    """Categorize the call to apply appropriate scoring weights"""
    text = transcript.lower()
    is_voicemail = (
        len(agent_lines) == 0
        or duration < 45
        or any(p in text for p in ['leave a message', 'after the tone', 'will get back', 'call you back at', 'callback at'])
    )
    if is_voicemail:
        return 'voicemail_out' if direction == 'outbound' else 'voicemail'
    is_quick = duration < 90 and len(transcript) < 600
    if is_quick:
        return 'quick_followup' if direction == 'outbound' else 'quick_inquiry'
    is_long = duration >= 180 or len(transcript) > 1500
    if is_long:
        return 'consultation_out' if direction == 'outbound' else 'consultation'
    return 'standard_out' if direction == 'outbound' else 'standard'

# Outbound-specific patterns
OUTBOUND_OPENING = [
    r'\bthis is\b.{0,40}\bfrom\b', r'\bcalling from strive\b', r'\bcalling from\b',
    r'\bfollow.?up\b', r'\bchecking in\b', r'\breaching out\b', r'\bwanted to follow\b',
    r'\bgot your message\b', r'\breturning your call\b', r'\bsaw you called\b',
    r'\bcalling to\b', r'\bjust calling\b'
]
OUTBOUND_CLOSE = [
    r'\bschedule\b', r'\bappointment\b', r'\bcome in\b', r'\bget started\b',
    r'\btomorrow\b', r'\btoday at\b', r'\bsee you\b',
    r'\bgive me a call\b', r'\breach out\b', r'\bcall me back\b',
    r'\bI will call\b', r"\bI'll call\b"
]

def analyze_call(c):
    transcript = (c.get('transcription_text','') or '').strip()
    if not transcript:
        return None

    summary  = (c.get('summary','') or '').strip()
    dispo    = (c.get('custom_fields') or {}).get('disposition_save_to_contact','')
    sale     = (c.get('sale') or {}).get('name','')
    duration = c.get('duration', 0) or 0
    fac      = 'fw' if (c.get('tracking_number_bare','') or '').startswith('260') else 'wl'
    direction = c['direction']

    agent_lines, caller_lines = split_speakers(transcript)
    call_type = detect_call_type(transcript, duration, agent_lines, caller_lines, direction)

    rapport_s, rapport_n     = score_rapport(transcript, agent_lines)
    needs_s,   needs_n       = score_needs(transcript)
    ins_s,     ins_n         = score_insurance(transcript)
    next_s,    next_n        = score_next_steps(transcript)
    comp_s,    comp_n        = score_compliance(transcript)

    # Outbound-specific scoring boost: reward strong opening + close
    if direction == 'outbound':
        text_lower = transcript.lower()
        opening_matches = count_matches(text_lower, OUTBOUND_OPENING)
        close_matches   = count_matches(text_lower, OUTBOUND_CLOSE)
        rapport_s = min(10, rapport_s + opening_matches * 0.8)
        next_s    = min(10, next_s    + close_matches   * 1.0)
        if opening_matches >= 1:
            rapport_n.append('Identified self/company on opening')
        if close_matches >= 2:
            next_n.append('Strong call-to-action close')

    # Adaptive weighting by call type
    if call_type in ('voicemail', 'voicemail_out'):
        # Voicemails: boost dimensions that can't apply (no live conversation)
        rapport_s = max(rapport_s, 6.0)
        needs_s   = max(needs_s + 3, needs_s)
        ins_s     = max(ins_s + 4, ins_s)
        next_s    = max(next_s + 3, next_s)
        overall = (needs_s*0.3 + next_s*0.3 + rapport_s*0.2 + comp_s*0.2)
        if call_type == 'voicemail_out':
            rapport_n.append('Outbound voicemail left for caller')
        else:
            rapport_n.append('Inbound voicemail (caller-side only)')
    elif call_type == 'quick_followup':
        # Outbound quick callbacks: opening + close matter most
        overall = (rapport_s*0.25 + needs_s*0.15 + ins_s*0.15 + next_s*0.35 + comp_s*0.10)
    elif call_type == 'quick_inquiry':
        overall = (rapport_s*0.30 + needs_s*0.20 + ins_s*0.15 + next_s*0.25 + comp_s*0.10)
    elif call_type in ('consultation', 'consultation_out'):
        overall = (rapport_s*0.20 + needs_s*0.25 + ins_s*0.25 + next_s*0.20 + comp_s*0.10)
    elif call_type == 'standard_out':
        overall = (rapport_s*0.20 + needs_s*0.15 + ins_s*0.20 + next_s*0.35 + comp_s*0.10)
    else:
        overall = (rapport_s*0.20 + needs_s*0.20 + ins_s*0.25 + next_s*0.25 + comp_s*0.10)

    # Boost any call where outcome was successful
    if 'scheduled admission' in dispo.lower():
        overall = min(10, overall + 1.5)
    if sale == 'Yes':
        overall = min(10, overall + 1.0)

    scores = {
        'rapport':    round(min(10, rapport_s), 1),
        'needs':      round(min(10, needs_s), 1),
        'insurance':  round(min(10, ins_s), 1),
        'next_steps': round(min(10, next_s), 1),
        'compliance': round(min(10, comp_s), 1),
        'overall':    round(min(10, overall), 1)
    }

    worked, didnt, better = generate_recommendations(scores, transcript, summary, dispo, sale)

    return {
        'id':       c.get('id'),
        'dt':       c['called_at'][:16],
        'duration': duration,
        'agent':    (c.get('agent') or {}).get('name','') or 'Unanswered',
        'caller':   c.get('caller_number_format',''),
        'name':     ((c.get('name','') or c.get('cnam','')) or '')[:30],
        'fac':      'Fort Wayne' if fac=='fw' else 'Waterloo',
        'src':      c.get('source',''),
        'dispo':    dispo,
        'sale':     sale,
        'urgent':   detect_urgency(transcript),
        'type':     call_type,
        'dir':      direction,
        'scores':   scores,
        'notes': {
            'rapport':    rapport_n,
            'needs':      needs_n,
            'insurance':  ins_n,
            'next_steps': next_n,
            'compliance': comp_n
        },
        'worked':   worked,
        'didnt':    didnt,
        'better':   better,
        'summary':  summary[:500],
        'transcript_preview': transcript[:600],
        'transcript_full': transcript,
        'audio':    c.get('audio','')
    }

if __name__ == '__main__':
    print('Loading calls...')
    with open('ctm_calls_raw.json') as f:
        calls = json.load(f)

    print(f'Analyzing {len(calls)} calls...')
    analyses = []
    for i, c in enumerate(calls):
        if c['direction'] not in ('inbound', 'outbound'): continue
        a = analyze_call(c)
        if a:
            analyses.append(a)
        if (i+1) % 500 == 0:
            print(f'  Processed {i+1}/{len(calls)}...')

    print(f'\nTotal analyzed: {len(analyses)}')

    # Aggregate stats
    by_agent = defaultdict(list)
    overall_scores = []
    for a in analyses:
        if a['agent'] and a['agent'] != 'Unanswered':
            by_agent[a['agent']].append(a['scores']['overall'])
        overall_scores.append(a['scores']['overall'])

    print(f'\nOverall avg score: {sum(overall_scores)/len(overall_scores):.1f}')
    print(f'\nBy agent:')
    for ag, scores in sorted(by_agent.items(), key=lambda x: -sum(x[1])/len(x[1])):
        if len(scores) >= 5:
            avg = sum(scores)/len(scores)
            print(f'  {ag:25s}  n={len(scores):3d}  avg={avg:.1f}')

    # Save
    with open('data_call_analysis.json', 'w') as f:
        json.dump(analyses, f, separators=(',',':'))

    import os
    print(f'\nSaved data_call_analysis.json ({os.path.getsize("data_call_analysis.json")//1024} KB)')
