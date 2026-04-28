import json, math, os, pandas as pd

# ── Load general report data ──────────────────────────────────────────────────
with open('report_data.json', 'r') as f:
    raw_data = json.load(f)

DATE_FIELDS = {
    'Census': 'Admission Date', 'Census Active': 'Admission Date',
    'Census_Admitted': 'Admission Date', 'Census_Discharge': 'Discharge Date',
    'GroupNotes': 'session_date', 'Incident Report': 'incident_reports.date_of_incident',
    'Opportunities Active': 'created_on', 'Opportunities by Created Date': 'created_on',
    'Opportunities': 'created_on', 'Patients': 'created_on',
    'Payment Report Payment Date': 'payment_date', 'Payment Report Deposit Date': 'deposit_date',
    'Referral Active': 'created_on', 'Report Auth': 'admission_date',
    'Report Deleted Form': 'deleted_on', 'Report Diagnois Changes': 'date_from',
    'Report Form Modified': 'modified_on', 'Report Program Change': 'start_on',
    'Report UR Changes': 'admission_date', 'Users': 'created_on',
}
tab_config = {}
for sheet, info in raw_data.items():
    dc = DATE_FIELDS.get(sheet, '')
    tab_config[sheet] = info['columns'].index(dc) if dc and dc in info['columns'] else -1

# ── Load billing data ─────────────────────────────────────────────────────────
df = pd.read_excel('MASTER_Sunwave_New_PowerQuerry.xlsx', sheet_name='Payment Report Deposit Date')
df['deposit_date'] = pd.to_datetime(df['deposit_date'], errors='coerce')
for c in ['line_charge_amount','line_paid_amount','line_adjusted','line_allocated_amount','line_allowed']:
    df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

billing_rows = []
for _, r in df.iterrows():
    dep = r['deposit_date']
    billing_rows.append({
        'deposit_date': dep.strftime('%m/%d/%Y') if pd.notna(dep) else '',
        'payer_name': str(r['payer_name']) if pd.notna(r['payer_name']) else '',
        'level_of_care': str(r['level_of_care']) if pd.notna(r['level_of_care']) else '',
        'adjustment_type': str(r['adjustment_type']) if pd.notna(r['adjustment_type']) else '',
        'service_facility': str(r['service_facility']) if pd.notna(r['service_facility']) else '',
        'service_name': str(r['service_name']) if pd.notna(r['service_name']) else '',
        'payment_type': str(r['payment_type']) if pd.notna(r['payment_type']) else '',
        'line_charge_amount': round(float(r['line_charge_amount']), 2),
        'line_paid_amount': round(float(r['line_paid_amount']), 2),
        'line_adjusted': round(float(r['line_adjusted']), 2),
        'line_allocated_amount': round(float(r['line_allocated_amount']), 2),
        'line_patient_name': str(r['line_patient_name']) if pd.notna(r['line_patient_name']) else '',
        'procedure_code': str(r['procedure_code']) if pd.notna(r['procedure_code']) else '',
    })

# ── Load census data ─────────────────────────────────────────────────────────
cdf = pd.read_excel('MASTER_Sunwave_New_PowerQuerry.xlsx', sheet_name='Census')
cdf['Admission Date'] = pd.to_datetime(cdf['Admission Date'], errors='coerce')
cdf['Discharge Date'] = pd.to_datetime(cdf['Discharge Date'], errors='coerce')
cdf['Age']            = pd.to_numeric(cdf['Age'], errors='coerce')
cdf['Length Of Stay'] = pd.to_numeric(cdf['Length Of Stay'], errors='coerce')

census_rows = []
for _, r in cdf.iterrows():
    adm = r['Admission Date']; dis = r['Discharge Date']
    census_rows.append({
        'adm':   adm.strftime('%m/%d/%Y') if pd.notna(adm) else '',
        'dis':   dis.strftime('%m/%d/%Y') if pd.notna(dis) else '',
        'loc':   str(r['Admission Level Of Care']).strip() if pd.notna(r['Admission Level Of Care']) else '',
        'cloc':  str(r['Current Level Of Care']).strip()   if pd.notna(r['Current Level Of Care'])   else '',
        'gen':   str(r['Patient Gender Code']).strip()     if pd.notna(r['Patient Gender Code'])     else '',
        'age':   int(r['Age']) if pd.notna(r['Age']) else None,
        'drug':  str(r['Primary Drug Of Choice ']).strip() if pd.notna(r['Primary Drug Of Choice ']) else '',
        'ref':   str(r['Referral Source']).strip()         if pd.notna(r['Referral Source'])         else '',
        'dtype': str(r['Discharge Type']).strip()          if pd.notna(r['Discharge Type'])          else '',
        'los':   int(r['Length Of Stay']) if pd.notna(r['Length Of Stay']) else None,
        'name':  str(r['Patient Name']).strip()            if pd.notna(r['Patient Name'])            else '',
    })

# ── Opportunities data (Marketing + Opportunities sections) ──────────────────
odf = pd.read_excel('MASTER_Sunwave_New_PowerQuerry.xlsx', sheet_name='Opportunities by Created Date')
odf['created_on']    = pd.to_datetime(odf['created_on'],    errors='coerce')
odf['admission_date']= pd.to_datetime(odf['admission_date'],errors='coerce')
opp_rows = []
for _, r in odf.iterrows():
    co = r['created_on']; ad = r['admission_date']
    opp_rows.append({
        'co':          co.strftime('%m/%d/%Y') if pd.notna(co) else '',
        'adm':         ad.strftime('%m/%d/%Y') if pd.notna(ad) and ad.year>2000 else '',
        'outcome':     str(r['outcome']).strip()           if pd.notna(r['outcome'])            else '',
        'stage':       str(r['stage']).strip()             if pd.notna(r['stage'])              else '',
        'loc':         str(r['level_of_care']).strip()     if pd.notna(r['level_of_care'])      else '',
        'ins':         str(r['insurance provider']).strip()if pd.notna(r['insurance provider']) else '',
        'ref':         str(r['referral name']).strip()     if pd.notna(r['referral name'])      else '',
        'lost_r':      str(r['lost reason']).strip()       if pd.notna(r['lost reason'])        else '',
        'aband_r':     str(r['abandoned reason']).strip()  if pd.notna(r['abandoned reason'])   else '',
        'name':        str(r['patient name']).strip()      if pd.notna(r['patient name'])       else '',
    })

# ── Report Auth data (Utilization Review) ────────────────────────────────────
adf = pd.read_excel('MASTER_Sunwave_New_PowerQuerry.xlsx', sheet_name='Report Auth')
adf['admission_date']  = pd.to_datetime(adf['admission_date'],  errors='coerce')
adf['next_review_date']= pd.to_datetime(adf['next_review_date'],errors='coerce')
for c in ['authorized_units','billed_units_total']:
    adf[c] = pd.to_numeric(adf[c], errors='coerce').fillna(0)
auth_rows = []
for _, r in adf.iterrows():
    ad = r['admission_date']; nrd = r['next_review_date']
    auth_rows.append({
        'adm':      ad.strftime('%m/%d/%Y')  if pd.notna(ad)  else '',
        'nrd':      nrd.strftime('%m/%d/%Y') if pd.notna(nrd) else '',
        'code':     str(r['authorization_code']).strip() if pd.notna(r['authorization_code']) else '',
        'au':       round(float(r['authorized_units']), 1),
        'bu':       round(float(r['billed_units_total']), 1),
        'ins':      str(r['insurance_provider']).strip()  if pd.notna(r['insurance_provider'])  else '',
        'reviewer': str(r['ur_reviewer']).strip()         if pd.notna(r['ur_reviewer'])         else '',
        'patient':  str(r['patient_name']).strip()        if pd.notna(r['patient_name'])        else '',
        'facility': str(r['service_facility']).strip()    if pd.notna(r['service_facility'])    else '',
    })

# ── Census_Admitted data (Operations) ────────────────────────────────────────
opdf = pd.read_excel('MASTER_Sunwave_New_PowerQuerry.xlsx', sheet_name='Census_Admitted')
opdf['Admission Date'] = pd.to_datetime(opdf['Admission Date'], errors='coerce')
ops_rows = []
for _, r in opdf.iterrows():
    ad = r['Admission Date']
    at = r.get('Admission Time')
    hr = at.hour if (pd.notna(at) if not isinstance(at, float) else False) and hasattr(at,'hour') else -1
    dow_pandas = int(ad.dayofweek) if pd.notna(ad) else -1   # 0=Mon pandas → convert to JS 0=Sun later
    ops_rows.append({
        'date':      ad.strftime('%m/%d/%Y') if pd.notna(ad) else '',
        'hour':      hr,
        'dow':       (dow_pandas + 1) % 7 if dow_pandas >= 0 else -1,  # JS 0=Sun
        'rep':       str(r['Admissions Rep']).strip()          if pd.notna(r['Admissions Rep'])        else '',
        'therapist': str(r['Assigned Therapist']).strip()      if pd.notna(r['Assigned Therapist'])    else '',
        'ins':       str(r['Insurance Name']).strip()          if pd.notna(r['Insurance Name'])        else '',
        'loc':       str(r['Admission Level Of Care']).strip() if pd.notna(r['Admission Level Of Care']) else '',
        'name':      str(r['Patient Name']).strip()            if pd.notna(r['Patient Name'])          else '',
    })

# ── GroupNotes data (Clinical) ───────────────────────────────────────────────
gndf = pd.read_excel('MASTER_Sunwave_New_PowerQuerry.xlsx', sheet_name='GroupNotes')
gndf['session_date'] = pd.to_datetime(gndf['session_date'], errors='coerce')
gndf['length_time']  = pd.to_numeric(gndf['length_time'],  errors='coerce').fillna(0)
gnotes_rows = []
for _, r in gndf.iterrows():
    sd = r['session_date']
    gnotes_rows.append({
        'date':   sd.strftime('%m/%d/%Y') if pd.notna(sd) else '',
        'title':  str(r['group_title']).strip() if pd.notna(r['group_title']) else '',
        'status': str(r['status']).strip()      if pd.notna(r['status'])      else '',
        'mins':   int(r['length_time']),
    })

# ── Serialize ─────────────────────────────────────────────────────────────────
general_js  = json.dumps(raw_data,     separators=(',',':'), ensure_ascii=True).replace('</', '<\\/')
config_js   = json.dumps(tab_config,   separators=(',',':'))
billing_js  = json.dumps(billing_rows, separators=(',',':'), ensure_ascii=True).replace('</', '<\\/')
census_js   = json.dumps(census_rows,  separators=(',',':'), ensure_ascii=True).replace('</', '<\\/')
opp_js      = json.dumps(opp_rows,     separators=(',',':'), ensure_ascii=True).replace('</', '<\\/')
auth_js     = json.dumps(auth_rows,    separators=(',',':'), ensure_ascii=True).replace('</', '<\\/')
ops_js      = json.dumps(ops_rows,     separators=(',',':'), ensure_ascii=True).replace('</', '<\\/')
gnotes_js   = json.dumps(gnotes_rows,  separators=(',',':'), ensure_ascii=True).replace('</', '<\\/')

# ── CSS ───────────────────────────────────────────────────────────────────────
CSS = """
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100%; }
body { font-family: Arial, sans-serif; background: #eaecf0; color: #333; font-size: 13px; display: flex; flex-direction: column; overflow: hidden; }

/* ── Layout ── */
#app { display: flex; flex: 1; overflow: hidden; }
#sidebar {
  width: 210px; min-width: 210px;
  background: linear-gradient(180deg, #0f2540 0%, #1a3a5c 100%);
  display: flex; flex-direction: column;
  overflow: hidden; box-shadow: 3px 0 12px rgba(0,0,0,.45);
  z-index: 10;
}
#content { flex: 1; overflow-y: auto; background: #eaecf0; }

/* ── Sidebar header ── */
.sb-header {
  padding: 16px 14px 10px;
  border-bottom: 1px solid rgba(255,255,255,.1);
  background: rgba(0,0,0,.2);
}
.sb-header h1 { color: #fff; font-size: 14px; font-weight: 700; line-height: 1.3; }
.sb-header p  { color: #8ab0d0; font-size: 10px; margin-top: 2px; }

/* ── Refresh button ── */
.refresh-btn {
  margin: 10px 10px 6px;
  padding: 8px 10px;
  width: calc(100% - 20px);
  background: linear-gradient(to bottom, #2e7d32, #1b5e20);
  border: 1px solid #145214;
  border-bottom: 3px solid #0d3b0d;
  border-radius: 6px;
  color: #fff;
  font-size: 11px; font-weight: 700; letter-spacing: .4px;
  cursor: pointer; text-align: center;
  box-shadow: 0 4px 8px rgba(0,0,0,.5), inset 0 1px 0 rgba(255,255,255,.2);
  transition: all .15s; text-transform: uppercase;
}
.refresh-btn:hover  { background: linear-gradient(to bottom, #388e3c, #2e7d32); transform: translateY(-1px); box-shadow: 0 6px 10px rgba(0,0,0,.5), inset 0 1px 0 rgba(255,255,255,.25); }
.refresh-btn:active { background: linear-gradient(to bottom, #1b5e20, #0d3b0d); transform: translateY(2px); box-shadow: 0 1px 3px rgba(0,0,0,.5), inset 0 2px 4px rgba(0,0,0,.4); border-bottom-width: 1px; }

/* ── Nav scroll area ── */
.sb-nav { flex: 1; overflow-y: auto; padding: 4px 0 12px; }
.sb-nav::-webkit-scrollbar { width: 4px; }
.sb-nav::-webkit-scrollbar-thumb { background: rgba(255,255,255,.2); border-radius: 2px; }

/* ── Section labels ── */
.sb-section {
  color: #6a99bf; font-size: 10px; font-weight: 700; letter-spacing: 1px;
  text-transform: uppercase; padding: 10px 14px 4px; user-select: none;
}

/* ── 3D Nav buttons ── */
.nav-item {
  display: block; width: calc(100% - 16px); margin: 3px 8px;
  padding: 9px 12px;
  background: linear-gradient(to bottom, #254e78, #1a3a5c);
  border: 1px solid #0f2540;
  border-bottom: 3px solid #0a1c30;
  border-radius: 6px;
  color: #c8dff0;
  font-size: 11.5px; font-weight: 600;
  cursor: pointer; text-align: left;
  box-shadow: 0 4px 6px rgba(0,0,0,.45), inset 0 1px 0 rgba(255,255,255,.12);
  transition: all .15s;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.nav-item:hover {
  background: linear-gradient(to bottom, #336699, #254e78);
  color: #fff;
  box-shadow: 0 6px 10px rgba(0,0,0,.5), inset 0 1px 0 rgba(255,255,255,.18);
  transform: translateY(-1px);
}
.nav-item:active {
  background: linear-gradient(to bottom, #1a3a5c, #0f2540);
  transform: translateY(2px);
  box-shadow: 0 1px 3px rgba(0,0,0,.5), inset 0 2px 4px rgba(0,0,0,.35);
  border-bottom-width: 1px;
}
.nav-item.active {
  background: linear-gradient(to bottom, #1a6ec0, #1252a0);
  border-color: #0d3f7a; border-bottom-color: #082d58;
  color: #fff;
  box-shadow: 0 2px 4px rgba(0,0,0,.5), inset 0 2px 4px rgba(0,0,0,.25), inset 0 1px 0 rgba(255,255,255,.15);
  transform: translateY(1px);
}

/* ── Page header ── */
.page-header {
  background: linear-gradient(135deg, #1a3a5c 0%, #1a6ec0 100%);
  color: #fff; padding: 12px 20px;
  display: flex; align-items: center; justify-content: space-between;
}
.page-header h2 { font-size: 16px; font-weight: 700; }
.page-header small { font-size: 11px; opacity: .75; }
.main { padding: 14px 18px; }

/* ── Controls bar ── */
.controls {
  background: #fff; border-radius: 8px; padding: 11px 14px;
  margin-bottom: 12px; display: flex; flex-wrap: wrap; align-items: center;
  gap: 10px; box-shadow: 0 1px 4px rgba(0,0,0,.1);
}
.view-btns { display: flex; gap: 4px; }
.view-btn {
  padding: 6px 15px;
  background: linear-gradient(to bottom, #ffffff, #e8edf2);
  border: 1px solid #aabcce; border-bottom: 2px solid #8aabbe;
  border-radius: 5px; color: #1a3a5c; font-size: 12px; font-weight: 700;
  cursor: pointer;
  box-shadow: 0 2px 4px rgba(0,0,0,.15), inset 0 1px 0 rgba(255,255,255,.9);
  transition: all .12s;
}
.view-btn:hover  { background: linear-gradient(to bottom, #f0f5fa, #dde6ef); transform: translateY(-1px); box-shadow: 0 3px 6px rgba(0,0,0,.2); }
.view-btn:active { transform: translateY(1px); box-shadow: 0 1px 2px rgba(0,0,0,.2); border-bottom-width: 1px; }
.view-btn.active {
  background: linear-gradient(to bottom, #1252a0, #1a6ec0);
  border-color: #0d3f7a; border-bottom-color: #082d58;
  color: #fff;
  box-shadow: 0 1px 3px rgba(0,0,0,.4), inset 0 2px 3px rgba(0,0,0,.2);
  transform: translateY(1px);
}
.nav-btns { display: flex; align-items: center; gap: 8px; }
.period-nav-btn {
  padding: 5px 13px;
  background: linear-gradient(to bottom, #f5f7fa, #e0e8f0);
  border: 1px solid #b0c4d8; border-bottom: 2px solid #8aabbe;
  border-radius: 5px; font-size: 16px; font-weight: 900;
  cursor: pointer; color: #1a3a5c;
  box-shadow: 0 2px 4px rgba(0,0,0,.15), inset 0 1px 0 rgba(255,255,255,.9);
  transition: all .12s;
}
.period-nav-btn:hover  { background: linear-gradient(to bottom, #fff, #d0dce8); transform: translateY(-1px); }
.period-nav-btn:active { transform: translateY(1px); border-bottom-width: 1px; box-shadow: inset 0 2px 3px rgba(0,0,0,.15); }
.period-label {
  font-weight: 700; font-size: 13px; min-width: 175px; text-align: center;
  color: #1a3a5c; background: #eef3f9;
  padding: 5px 12px; border-radius: 5px;
  border: 1px solid #c5d8ec;
}
.date-input  { padding: 5px 9px; border: 1px solid #ccc; border-radius: 5px; font-size: 12px; }
.search-box  { padding: 6px 10px; border: 1px solid #ccc; border-radius: 5px; font-size: 12px; flex: 1; min-width: 160px; }
.export-btn {
  padding: 7px 15px;
  background: linear-gradient(to bottom, #2e9c5a, #217346);
  border: 1px solid #185a35; border-bottom: 2px solid #0f3d24;
  border-radius: 5px; color: #fff; font-size: 12px; font-weight: 700;
  cursor: pointer;
  box-shadow: 0 2px 5px rgba(0,0,0,.25), inset 0 1px 0 rgba(255,255,255,.2);
  transition: all .12s; white-space: nowrap;
}
.export-btn:hover  { background: linear-gradient(to bottom, #39b86a, #2e9c5a); transform: translateY(-1px); box-shadow: 0 3px 7px rgba(0,0,0,.3); }
.export-btn:active { transform: translateY(1px); border-bottom-width: 1px; box-shadow: inset 0 2px 3px rgba(0,0,0,.2); }

/* ── Stat / KPI cards ── */
.stats-bar  { display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 12px; }
.stat-card  {
  background: #fff; border-radius: 8px; padding: 13px 18px;
  min-width: 130px; flex: 1;
  box-shadow: 0 2px 6px rgba(0,0,0,.1);
  border-left: 4px solid #1a3a5c;
  transition: transform .15s, box-shadow .15s;
}
.stat-card:hover { transform: translateY(-2px); box-shadow: 0 4px 10px rgba(0,0,0,.15); }
.stat-card.green  { border-left-color: #217346; }
.stat-card.orange { border-left-color: #c86a00; }
.stat-card.purple { border-left-color: #6a3a9c; }
.stat-card .val { font-size: 24px; font-weight: 700; color: #1a3a5c; line-height: 1.2; }
.stat-card .lbl { font-size: 11px; color: #777; margin-top: 3px; text-transform: uppercase; letter-spacing: .4px; }

/* ── Spot table (billing) ── */
.spot-wrap  { background: #fff; border-radius: 8px; box-shadow: 0 1px 4px rgba(0,0,0,.1); overflow: auto; margin-bottom: 16px; }
.spot-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.spot-table thead th { background: #1a3a5c; color: #fff; padding: 9px 14px; text-align: center; white-space: nowrap; font-weight: 600; }
.spot-table thead th:first-child { text-align: left; }
.spot-table tbody tr { border-bottom: 1px solid #eee; }
.spot-table tbody tr:hover { background: #f0f5fb; }
.spot-table tbody tr:nth-child(even) { background: #fafbfd; }
.spot-table tbody tr:nth-child(even):hover { background: #f0f5fb; }
.spot-table td { padding: 8px 14px; white-space: nowrap; }
.spot-table td.metric-name { font-weight: 600; }
.spot-table td.num { text-align: right; font-variant-numeric: tabular-nums; color: #1a3a5c; }

/* ── Break / trend grid ── */
.break-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-bottom: 14px; }
.break-card { background: #fff; border-radius: 8px; padding: 13px 15px; box-shadow: 0 1px 4px rgba(0,0,0,.1); }
.break-card h3 { font-size: 12px; font-weight: 700; color: #1a3a5c; margin-bottom: 8px; }
.break-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.break-table thead th { background: #eef3f9; color: #1a3a5c; padding: 7px 10px; text-align: left; font-weight: 700; border-bottom: 2px solid #c5d8ec; }
.break-table tbody tr { border-bottom: 1px solid #eee; }
.break-table tbody tr:hover { background: #f0f5fb; }
.break-table td { padding: 6px 10px; }
.break-table td.num { text-align: right; }
.break-table tr.total-row td { font-weight: 700; border-top: 2px solid #ccc; background: #f5f8fc; }
.trend-wrap { background: #fff; border-radius: 8px; padding: 13px 15px; box-shadow: 0 1px 4px rgba(0,0,0,.1); overflow: auto; margin-bottom: 14px; }
.trend-wrap h3 { font-size: 12px; font-weight: 700; color: #1a3a5c; margin-bottom: 8px; }
.trend-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.trend-table thead th { background: #1a3a5c; color: #fff; padding: 8px 14px; text-align: center; white-space: nowrap; }
.trend-table thead th:first-child { text-align: left; }
.trend-table tbody tr { border-bottom: 1px solid #eee; }
.trend-table tbody tr:hover { background: #f0f5fb; }
.trend-table tbody tr:nth-child(even) { background: #fafbfd; }
.trend-table td { padding: 7px 14px; white-space: nowrap; }
.trend-table td.metric-name { font-weight: 600; }
.trend-table td.num { text-align: right; }

/* ── Data table ── */
.table-wrap { background: #fff; border-radius: 8px; box-shadow: 0 1px 4px rgba(0,0,0,.1); overflow: auto; max-height: 480px; }
table { width: 100%; border-collapse: collapse; font-size: 12px; }
thead th { background: #1a3a5c; color: #fff; padding: 8px 10px; text-align: left; position: sticky; top: 0; z-index: 2; white-space: nowrap; font-weight: 600; cursor: pointer; user-select: none; }
thead th:hover { background: #244d73; }
tbody tr { border-bottom: 1px solid #eee; }
tbody tr:hover { background: #edf3fa; }
tbody td { padding: 6px 10px; max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
tbody tr:nth-child(even) { background: #fafbfd; }
.no-data { text-align: center; padding: 40px; color: #999; font-size: 14px; }
.pagination { display: flex; align-items: center; gap: 6px; margin-top: 10px; flex-wrap: wrap; }
.page-info { font-size: 12px; color: #666; margin-right: 4px; }
.page-btn { padding: 4px 10px; border: 1px solid #ccc; background: #fff; border-radius: 4px; cursor: pointer; font-size: 11px; }
.page-btn:hover { background: #eee; }
.page-btn.active { background: #1a3a5c; color: #fff; border-color: #1a3a5c; }

h2.section-title {
  font-size: 13px; font-weight: 700; color: #1a3a5c;
  margin: 16px 0 8px; padding-bottom: 5px;
  border-bottom: 2px solid #1a3a5c;
}
.date-field-label { font-size: 11px; color: #999; font-style: italic; }
.spot-section { background: #1a3a5c; color: #fff; font-weight: 700; font-size: 11px; padding: 6px 14px; letter-spacing: .4px; text-transform: uppercase; }
/* Heatmap */
.heatmap-wrap { overflow-x: auto; background:#fff; border-radius:8px; padding:14px; box-shadow:0 1px 4px rgba(0,0,0,.1); margin-bottom:14px; }
.heatmap-table { border-collapse:collapse; font-size:11px; }
.heatmap-table th { padding:5px 8px; background:#1a3a5c; color:#fff; white-space:nowrap; text-align:center; }
.heatmap-table th.row-hdr { background:#eef3f9; color:#1a3a5c; text-align:right; }
.heatmap-table td { width:38px; height:26px; text-align:center; border:1px solid #e0e6ee; font-size:11px; font-weight:600; cursor:default; }
.hm-0{background:#f5f8fc;color:#999}.hm-1{background:#d4e8fb;color:#1a3a5c}.hm-2{background:#a8d0f7;color:#1a3a5c}.hm-3{background:#6fb5f0;color:#fff}.hm-4{background:#3694e0;color:#fff}.hm-5{background:#1a6ec0;color:#fff}.hm-6{background:#0d3f7a;color:#fff}
/* Funnel */
.funnel-bar { display:flex; align-items:center; gap:10px; margin:4px 0; }
.funnel-label { width:120px; text-align:right; font-size:12px; font-weight:600; color:#333; }
.funnel-track { flex:1; background:#eef3f9; border-radius:4px; height:22px; position:relative; overflow:hidden; }
.funnel-fill { height:100%; border-radius:4px; background:linear-gradient(to right,#1a6ec0,#3694e0); transition:width .4s; }
.funnel-val { position:absolute; right:8px; top:3px; font-size:11px; font-weight:700; color:#fff; }
.funnel-val.dark { color:#1a3a5c; }
"""

# ── JavaScript ────────────────────────────────────────────────────────────────
JS = r"""
/* ===== Data ===== */
const RAW      = JSON.parse(document.getElementById('generalData').textContent);
const DATE_IDX = JSON.parse(document.getElementById('dateIdx').textContent);
const BROWS    = JSON.parse(document.getElementById('billingData').textContent);
const SHEETS   = Object.keys(RAW);

/* ===== Navigation state ===== */
let curPage = 'billing';
let gView={}, gOffset={}, gSearch={}, gDate={}, gSort={};
const PAGE_SIZE = 100;
SHEETS.forEach(s=>{
  gView[s]='month'; gOffset[s]=0; gSearch[s]=''; gDate[s]=null; gSort[s]={col:-1,asc:true};
});
let bNavView='month', bNavDate=null, bNavSearch='', bSortCol='deposit_date', bSortAsc=false;
for(const r of BROWS){ const d=pd(r.deposit_date); if(d){bNavDate=d;break;} }
if(!bNavDate) bNavDate=new Date();

/* ===== Helpers ===== */
function pd(str){
  if(!str||str===''||str==='nan')return null;
  const m=str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if(m){const d=new Date(+m[3],+m[1]-1,+m[2]);d.setHours(0,0,0,0);return d;}
  return null;
}
function fd(d){ if(!d)return''; return String(d.getMonth()+1).padStart(2,'0')+'/'+String(d.getDate()).padStart(2,'0')+'/'+d.getFullYear(); }
function fmtMoney(v){ return '$'+Math.round(+v||0).toLocaleString(); }
function fmtPct(v){ if(!v||isNaN(v)||!isFinite(v))return'0.0%'; return(v*100).toFixed(1)+'%'; }
function fmtAvg(v){ if(!v||isNaN(v)||!isFinite(v))return'$0.00'; return'$'+v.toFixed(2); }
function esc(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
function gwk(d){const c=new Date(d);c.setDate(c.getDate()-c.getDay());c.setHours(0,0,0,0);return c;}
function smm(a,b){return a.getFullYear()===b.getFullYear()&&a.getMonth()===b.getMonth();}
function swk(a,b){return gwk(a).getTime()===gwk(b).getTime();}
function sdy(a,b){return a.getFullYear()===b.getFullYear()&&a.getMonth()===b.getMonth()&&a.getDate()===b.getDate();}

/* ===== Sidebar nav ===== */
function showPage(id){
  curPage=id;
  document.querySelectorAll('.nav-item').forEach(b=>b.classList.remove('active'));
  const btn=document.getElementById('btn-'+id.replace(/[^a-zA-Z0-9]/g,'_'));
  if(btn) btn.classList.add('active');
  document.querySelectorAll('.page-section').forEach(s=>s.style.display='none');
  const sec=document.getElementById('sec-'+id.replace(/[^a-zA-Z0-9]/g,'_'));
  if(sec){ sec.style.display='block'; document.getElementById('content').scrollTop=0; }
  // Update header
  const titles={
    'billing':'AR / Billing Dashboard',
    'census':'Census Dashboard \u2014 Strive Fort Wayne, IN',
    'marketing':'Marketing Dashboard',
    'opportunities':'Opportunities Detail',
    'ur':'Utilization Review',
    'clinical':'Clinical \u2014 Group Notes',
    'operations':'Operations Dashboard',
    'fieldexplorer':'Field Explorer',
  };
  const subs={
    'billing':'Payment Report Deposit Date \u2014 MASTER_Sunwave_New_PowerQuerry.xlsx',
    'census':'Census \u2014 MASTER_Sunwave_New_PowerQuerry.xlsx',
    'marketing':'Opportunities by Created Date \u2014 MASTER_Sunwave_New_PowerQuerry.xlsx',
    'opportunities':'Opportunities by Created Date \u2014 MASTER_Sunwave_New_PowerQuerry.xlsx',
    'ur':'Report Auth \u2014 MASTER_Sunwave_New_PowerQuerry.xlsx',
    'clinical':'GroupNotes \u2014 MASTER_Sunwave_New_PowerQuerry.xlsx',
    'operations':'Census_Admitted \u2014 MASTER_Sunwave_New_PowerQuerry.xlsx',
    'fieldexplorer':'Census \u2014 MASTER_Sunwave_New_PowerQuerry.xlsx',
  };
  const specialPages=new Set(['billing','census','marketing','opportunities','ur','clinical','operations','fieldexplorer']);
  document.getElementById('pageTitle').textContent = titles[id]||id;
  document.getElementById('pageSub').textContent   = subs[id]||'MASTER_Sunwave_New_PowerQuerry.xlsx';
  if(!specialPages.has(id)) renderGeneral(id);
}

function doRefresh(){
  const btn=document.querySelector('.refresh-btn');
  btn.textContent='Refreshing…';
  setTimeout(()=>{
    if(curPage==='billing'){ renderBillingSpot();renderBillingBreakdowns();renderBillingTrend();renderBillingDetail(); }
    else if(curPage==='census'){ renderCensusSpot();renderCensusTrend();renderCensusBreakdowns(); }
    else if(curPage==='marketing'){ renderMarketingSpot();renderMarketingTrend();renderMarketingDetail(); }
    else if(curPage==='opportunities'){ renderOpportunities(); }
    else if(curPage==='ur'){ renderURSpot();renderURTrend(); }
    else if(curPage==='clinical'){ renderClinicalSpot();renderClinical(); }
    else if(curPage==='operations'){ renderOpsHeatmap();renderOpsDetail();renderOpsMonthlyIns(); }
    else if(curPage==='fieldexplorer'){ renderFieldExplorer(); }
    else renderGeneral(curPage);
    btn.textContent='\u27f3  Refresh';
  },100);
}

/* ═══════════════════════════════════════════════════════════════
   BILLING LOGIC
═══════════════════════════════════════════════════════════════ */
const TODAY=new Date(); TODAY.setHours(0,0,0,0);
function daysAgo(n){const d=new Date(TODAY);d.setDate(d.getDate()-n);return d;}
function som(d){return new Date(d.getFullYear(),d.getMonth(),1);}
function eom(d){return new Date(d.getFullYear(),d.getMonth()+1,0);}

function bFilter(rows,from,to){
  return rows.filter(r=>{const d=pd(r.deposit_date);return d&&d>=from&&d<=to;});
}
function bMetrics(rows){
  const pr=rows.filter(r=>r.adjustment_type==='Allowed');
  const paid=pr.reduce((s,r)=>s+r.line_paid_amount,0);
  const charged=rows.reduce((s,r)=>s+r.line_charge_amount,0);
  const allowed=pr.reduce((s,r)=>s+r.line_allocated_amount,0);
  const contractual=rows.filter(r=>r.adjustment_type==='Contractual').reduce((s,r)=>s+r.line_adjusted,0);
  const writeoff=rows.filter(r=>r.adjustment_type==='Write Off'||r.adjustment_type==='Administrative Write Off').reduce((s,r)=>s+r.line_adjusted,0);
  const noncov=rows.filter(r=>r.adjustment_type==='Non Covered Service').reduce((s,r)=>s+r.line_charge_amount,0);
  const lines=pr.length;
  return{paid,charged,allowed,contractual,writeoff,noncov,lines,
    cr:allowed>0?paid/allowed:0,nr:charged>0?paid/charged:0,avg:lines>0?paid/lines:0};
}
const BPERIODS=[
  {label:'Today',from:TODAY,to:TODAY},
  {label:'Yesterday',from:daysAgo(1),to:daysAgo(1)},
  {label:'Last 7 Days',from:daysAgo(6),to:TODAY},
  {label:'Last 31 Days',from:daysAgo(30),to:TODAY},
  {label:'Last 90 Days',from:daysAgo(89),to:TODAY},
  {label:'Last 12 Mo',from:daysAgo(364),to:TODAY},
  {label:'YTD',from:new Date(TODAY.getFullYear(),0,1),to:TODAY},
];
const BMETRICS=[
  {key:'lines',      label:'# Payment Lines',                    fmt:v=>v.toLocaleString()},
  {key:'charged',    label:'Charged $',                          fmt:fmtMoney},
  {key:'allowed',    label:'Allowed $ (contractual)',             fmt:fmtMoney},
  {key:'paid',       label:'Paid $',                             fmt:fmtMoney},
  {key:'contractual',label:'Contractual Adjustments $',           fmt:fmtMoney},
  {key:'writeoff',   label:'Write-Offs $',                       fmt:fmtMoney},
  {key:'noncov',     label:'Non-Covered / Denied $',             fmt:fmtMoney},
  {key:'cr',         label:'Collection Rate (Paid \u00f7 Allowed)',fmt:fmtPct},
  {key:'nr',         label:'Net Realization (Paid \u00f7 Charged)',fmt:fmtPct},
  {key:'avg',        label:'Avg $ / Line',                       fmt:fmtAvg},
];

function renderBillingSpot(){
  let h='<table class="spot-table"><thead><tr><th>Metric</th>';
  BPERIODS.forEach(p=>h+='<th>'+p.label+'</th>');
  h+='</tr></thead><tbody>';
  BMETRICS.forEach(m=>{
    h+='<tr><td class="metric-name">'+m.label+'</td>';
    BPERIODS.forEach(p=>{const c=bMetrics(bFilter(BROWS,p.from,p.to));h+='<td class="num">'+m.fmt(c[m.key])+'</td>';});
    h+='</tr>';
  });
  h+='</tbody></table>';
  document.getElementById('bSpotTable').innerHTML=h;
}

function bNavLabel(){
  if(bNavView==='month')return bNavDate.toLocaleString('default',{month:'long'})+' '+bNavDate.getFullYear();
  if(bNavView==='week'){const ws=gwk(bNavDate),we=new Date(ws);we.setDate(we.getDate()+6);return fd(ws)+' \u2013 '+fd(we);}
  return fd(bNavDate);
}
function bNavRange(){
  if(bNavView==='month')return{from:som(bNavDate),to:eom(bNavDate)};
  if(bNavView==='week'){const ws=gwk(bNavDate),we=new Date(ws);we.setDate(we.getDate()+6);return{from:ws,to:we};}
  const d=new Date(bNavDate);return{from:d,to:d};
}
function bNavigate(dir){
  if(bNavView==='month')bNavDate=new Date(bNavDate.getFullYear(),bNavDate.getMonth()+dir,1);
  else if(bNavView==='week')bNavDate=new Date(bNavDate.getTime()+dir*7*86400000);
  else bNavDate=new Date(bNavDate.getTime()+dir*86400000);
  renderBillingPeriod();
}
function bSetView(v){bNavView=v;renderBillingPeriod();}
function bJump(val){if(!val)return;const p=val.split('-');bNavDate=new Date(+p[0],+p[1]-1,+p[2]);renderBillingPeriod();}

function gbg(rows,key,fn){
  const m={};
  rows.forEach(r=>{const k=r[key]||'(blank)';m[k]=(m[k]||0)+fn(r);});
  return Object.entries(m).sort((a,b)=>b[1]-a[1]);
}

function renderBillingBreakdowns(){
  const{from,to}=bNavRange();
  const rows=bFilter(BROWS,from,to);
  const pr=rows.filter(r=>r.adjustment_type==='Allowed');
  const totalPaid=pr.reduce((s,r)=>s+r.line_paid_amount,0);
  const totalCharged=rows.reduce((s,r)=>s+r.line_charge_amount,0);

  const mkPayerTable=()=>{
    const byP=gbg(pr,'payer_name',r=>r.line_paid_amount);
    const top=byP.slice(0,9),other=byP.slice(9);
    let h='<table class="break-table"><thead><tr><th>Payer</th><th>Paid $</th><th>Lines</th><th>% Total</th></tr></thead><tbody>';
    top.forEach(([k,v])=>{
      const ln=pr.filter(r=>r.payer_name===k).length;
      h+='<tr><td>'+esc(k)+'</td><td class="num">'+fmtMoney(v)+'</td><td class="num">'+ln+'</td><td class="num">'+(totalPaid>0?(v/totalPaid*100).toFixed(1)+'%':'0%')+'</td></tr>';
    });
    if(other.length){const ov=other.reduce((s,[,v])=>s+v,0),ol=other.reduce((s,[k])=>s+pr.filter(r=>r.payer_name===k).length,0);
      h+='<tr><td><em>Others</em></td><td class="num">'+fmtMoney(ov)+'</td><td class="num">'+ol+'</td><td class="num">'+(totalPaid>0?(ov/totalPaid*100).toFixed(1)+'%':'0%')+'</td></tr>';}
    h+='<tr class="total-row"><td>Total</td><td class="num">'+fmtMoney(totalPaid)+'</td><td class="num">'+pr.length+'</td><td class="num">100%</td></tr></tbody></table>';
    return h;
  };
  const mkLocTable=()=>{
    const byL=gbg(pr,'level_of_care',r=>r.line_paid_amount);
    let h='<table class="break-table"><thead><tr><th>Level of Care</th><th>Paid $</th><th>Lines</th><th>% Total</th></tr></thead><tbody>';
    byL.forEach(([k,v])=>{
      const ln=pr.filter(r=>r.level_of_care===k).length;
      h+='<tr><td>'+esc(k)+'</td><td class="num">'+fmtMoney(v)+'</td><td class="num">'+ln+'</td><td class="num">'+(totalPaid>0?(v/totalPaid*100).toFixed(1)+'%':'0%')+'</td></tr>';
    });
    h+='<tr class="total-row"><td>Total</td><td class="num">'+fmtMoney(totalPaid)+'</td><td class="num">'+pr.length+'</td><td class="num">100%</td></tr></tbody></table>';
    return h;
  };
  const mkAdjTable=()=>{
    const ats=['Contractual','Non Covered Service','Write Off','Administrative Write Off'];
    let h='<table class="break-table"><thead><tr><th>Adjustment Type</th><th>Amount $</th><th>Lines</th><th>% Charged</th></tr></thead><tbody>';
    let tot=0;
    ats.forEach(at=>{
      const ar=rows.filter(r=>r.adjustment_type===at);
      const amt=ar.reduce((s,r)=>s+(at==='Non Covered Service'?r.line_charge_amount:r.line_adjusted),0);
      tot+=amt;
      h+='<tr><td>'+esc(at)+'</td><td class="num">'+fmtMoney(amt)+'</td><td class="num">'+ar.length+'</td><td class="num">'+(totalCharged>0?(amt/totalCharged*100).toFixed(1)+'%':'0%')+'</td></tr>';
    });
    h+='<tr class="total-row"><td>Total Written Off</td><td class="num">'+fmtMoney(tot)+'</td><td class="num">'+rows.filter(r=>r.adjustment_type!=='Allowed').length+'</td><td class="num">'+(totalCharged>0?(tot/totalCharged*100).toFixed(1)+'%':'0%')+'</td></tr></tbody></table>';
    return h;
  };
  document.getElementById('bPayerTable').innerHTML=mkPayerTable();
  document.getElementById('bLocTable').innerHTML=mkLocTable();
  document.getElementById('bAdjTable').innerHTML=mkAdjTable();
}

function renderBillingTrend(){
  const months=[];
  for(let i=5;i>=0;i--){const d=new Date(TODAY.getFullYear(),TODAY.getMonth()-i,1);months.push({label:d.toLocaleString('default',{month:'short',year:'2-digit'}),from:d,to:eom(d)});}
  const tm=[
    {label:'Paid $',fn:rs=>fmtMoney(rs.filter(r=>r.adjustment_type==='Allowed').reduce((s,r)=>s+r.line_paid_amount,0))},
    {label:'Charged $',fn:rs=>fmtMoney(rs.reduce((s,r)=>s+r.line_charge_amount,0))},
    {label:'# Lines',fn:rs=>rs.filter(r=>r.adjustment_type==='Allowed').length.toLocaleString()},
    {label:'Contractual Adj $',fn:rs=>fmtMoney(rs.filter(r=>r.adjustment_type==='Contractual').reduce((s,r)=>s+r.line_adjusted,0))},
    {label:'Non-Covered / Denied $',fn:rs=>fmtMoney(rs.filter(r=>r.adjustment_type==='Non Covered Service').reduce((s,r)=>s+r.line_charge_amount,0))},
    {label:'Avg $ / Line',fn:rs=>{const p=rs.filter(r=>r.adjustment_type==='Allowed');return fmtAvg(p.length>0?p.reduce((s,r)=>s+r.line_paid_amount,0)/p.length:0);}},
  ];
  let h='<table class="trend-table"><thead><tr><th>Metric</th>';
  months.forEach(m=>h+='<th>'+m.label+'</th>');
  h+='</tr></thead><tbody>';
  tm.forEach(m=>{
    h+='<tr><td class="metric-name">'+m.label+'</td>';
    months.forEach(mo=>{h+='<td class="num">'+m.fn(bFilter(BROWS,mo.from,mo.to))+'</td>';});
    h+='</tr>';
  });
  h+='</tbody></table>';
  document.getElementById('bTrendTable').innerHTML=h;
}

function renderBillingDetail(){
  const{from,to}=bNavRange();
  let rows=bFilter(BROWS,from,to);
  if(bNavSearch){const s=bNavSearch.toLowerCase();rows=rows.filter(r=>Object.values(r).some(v=>String(v).toLowerCase().includes(s)));}
  rows=rows.slice().sort((a,b)=>{
    const av=String(a[bSortCol]||''),bv=String(b[bSortCol]||'');
    const ad=pd(av),bd=pd(bv);
    if(ad&&bd)return bSortAsc?(ad-bd):(bd-ad);
    const an=parseFloat(av),bn=parseFloat(bv);
    if(!isNaN(an)&&!isNaN(bn))return bSortAsc?(an-bn):(bn-an);
    return bSortAsc?av.localeCompare(bv):bv.localeCompare(av);
  });
  const m=bMetrics(rows);
  document.getElementById('bKpiLines').textContent=m.lines.toLocaleString();
  document.getElementById('bKpiCharged').textContent=fmtMoney(m.charged);
  document.getElementById('bKpiPaid').textContent=fmtMoney(m.paid);
  document.getElementById('bKpiCR').textContent=fmtPct(m.cr);
  document.getElementById('bKpiNR').textContent=fmtPct(m.nr);
  document.getElementById('bKpiAvg').textContent=fmtAvg(m.avg);
  document.getElementById('bPeriodLabel').innerHTML=bNavLabel();
  document.getElementById('bViewMonth').className='view-btn'+(bNavView==='month'?' active':'');
  document.getElementById('bViewWeek').className='view-btn'+(bNavView==='week'?' active':'');
  document.getElementById('bViewDay').className='view-btn'+(bNavView==='day'?' active':'');

  const cols=['deposit_date','payer_name','level_of_care','adjustment_type','service_facility','service_name','line_patient_name','procedure_code','line_charge_amount','line_paid_amount','line_adjusted','line_allocated_amount'];
  const hdrs=['Deposit Date','Payer','Level of Care','Adj Type','Facility','Service','Patient','Code','Charged $','Paid $','Adjusted $','Allocated $'];
  const moneyIdx=new Set([8,9,10,11]);
  let h='<div class="table-wrap"><table><thead><tr>';
  cols.forEach((c,i)=>{const arr=bSortCol===c?(bSortAsc?' &#9650;':' &#9660;'):'';h+='<th onclick="bSetSort(\''+c+'\')" >'+hdrs[i]+arr+'</th>';});
  h+='</tr></thead><tbody>';
  const pg=rows.slice(0,200);
  if(!pg.length)h+='<tr><td colspan="'+cols.length+'" class="no-data">No records for selected period.</td></tr>';
  pg.forEach(r=>{h+='<tr>';cols.forEach((c,i)=>{const v=r[c];const disp=moneyIdx.has(i)?fmtMoney(+v):esc(v);h+='<td title="'+esc(v)+'">'+disp+'</td>';});h+='</tr>';});
  h+='</tbody></table></div>';
  if(rows.length>200)h+='<div class="page-info" style="margin-top:8px">Showing 200 of '+rows.length.toLocaleString()+' records — Export CSV for full data.</div>';
  document.getElementById('bDetailTable').innerHTML=h;
}

function bSetSort(col){if(bSortCol===col)bSortAsc=!bSortAsc;else{bSortCol=col;bSortAsc=false;}renderBillingDetail();renderBillingBreakdowns();}

function renderBillingPeriod(){ renderBillingBreakdowns(); renderBillingDetail(); }

function bExport(){
  const{from,to}=bNavRange();let rows=bFilter(BROWS,from,to);
  const cols=Object.keys(rows[0]||{});
  let csv=cols.map(c=>'"'+c+'"').join(',')+'\n';
  rows.forEach(r=>{csv+=cols.map(c=>'"'+String(r[c]||'').replace(/"/g,'""')+'"').join(',')+'\n';});
  const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([csv],{type:'text/csv'}));
  a.download='billing_'+bNavLabel().replace(/[^a-z0-9]/gi,'_')+'.csv';a.click();
}

/* ═══════════════════════════════════════════════════════════════
   GENERAL REPORTS LOGIC
═══════════════════════════════════════════════════════════════ */
function gFilterRows(sheet){
  const info=RAW[sheet],dIdx=DATE_IDX[sheet],ref=gDate[sheet],view=gView[sheet],search=gSearch[sheet].toLowerCase();
  let rows=info.rows.filter(row=>{
    if(dIdx>=0&&ref){
      const d=pd(row[dIdx]);if(!d)return false;
      if(view==='month'&&!smm(d,ref))return false;
      if(view==='week'&&!swk(d,ref))return false;
      if(view==='day'&&!sdy(d,ref))return false;
    }
    if(search)return row.some(c=>String(c).toLowerCase().includes(search));
    return true;
  });
  const ss=gSort[sheet];
  if(ss.col>=0){rows=rows.slice().sort((a,b)=>{
    const av=String(a[ss.col]||''),bv=String(b[ss.col]||'');
    const ad=pd(av),bd=pd(bv);if(ad&&bd)return ss.asc?(ad-bd):(bd-ad);
    return ss.asc?av.localeCompare(bv):bv.localeCompare(av);
  });}
  return rows;
}
function gPeriodLabel(sheet){
  const ref=gDate[sheet],view=gView[sheet];
  if(!ref)return'All Dates';
  if(view==='month')return ref.toLocaleString('default',{month:'long'})+' '+ref.getFullYear();
  if(view==='week'){const ws=gwk(ref),we=new Date(ws);we.setDate(we.getDate()+6);return fd(ws)+' \u2013 '+fd(we);}
  return fd(ref);
}
function gNavigate(sheet,dir){
  const view=gView[sheet];let ref=new Date(gDate[sheet]||new Date());
  if(view==='month')ref=new Date(ref.getFullYear(),ref.getMonth()+dir,1);
  else if(view==='week')ref=new Date(ref.getTime()+dir*7*86400000);
  else ref=new Date(ref.getTime()+dir*86400000);
  gDate[sheet]=ref;gOffset[sheet]=0;renderGeneral(sheet);
}
function gSetView(sheet,view){
  gView[sheet]=view;gOffset[sheet]=0;
  if(!gDate[sheet]){const dIdx=DATE_IDX[sheet];if(dIdx>=0)for(const row of RAW[sheet].rows){const d=pd(row[dIdx]);if(d){gDate[sheet]=d;break;}}if(!gDate[sheet])gDate[sheet]=new Date();}
  renderGeneral(sheet);
}
function gSortCol(sheet,ci){const ss=gSort[sheet];if(ss.col===ci)ss.asc=!ss.asc;else{ss.col=ci;ss.asc=true;}gOffset[sheet]=0;renderGeneral(sheet);}
function gGotoPage(sheet,offset){gOffset[sheet]=Math.max(0,offset);renderGeneral(sheet);}
function gSearch2(sheet,val){gSearch[sheet]=val;gOffset[sheet]=0;renderGeneral(sheet);}
function gJump(sheet,val){if(!val)return;const p=val.split('-');gDate[sheet]=new Date(+p[0],+p[1]-1,+p[2]);gOffset[sheet]=0;renderGeneral(sheet);}
function gExport(sheet){
  const info=RAW[sheet],filtered=gFilterRows(sheet);
  let csv=info.columns.map(c=>'"'+c.replace(/"/g,'""')+'"').join(',')+'\n';
  filtered.forEach(row=>{csv+=row.map(c=>'"'+String(c||'').replace(/"/g,'""')+'"').join(',')+'\n';});
  const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([csv],{type:'text/csv'}));
  a.download=sheet.replace(/[^a-z0-9]/gi,'_')+'.csv';a.click();
}

function renderGeneral(sheet){
  const sid=sheet.replace(/[^a-zA-Z0-9]/g,'_');
  const panel=document.getElementById('gpanel-'+sid);
  if(!panel)return;
  const info=RAW[sheet],cols=info.columns,dIdx=DATE_IDX[sheet],view=gView[sheet];
  const filtered=gFilterRows(sheet),offset=gOffset[sheet];
  const page=filtered.slice(offset,offset+PAGE_SIZE);
  const totalPages=Math.ceil(filtered.length/PAGE_SIZE);
  const curPage=Math.floor(offset/PAGE_SIZE)+1;

  // Stats
  let stats='<div class="stats-bar">';
  stats+='<div class="stat-card"><div class="val">'+filtered.length.toLocaleString()+'</div><div class="lbl">Total Records</div></div>';
  if(dIdx>=0){const ds=new Set();filtered.forEach(r=>{const d=pd(r[dIdx]);if(d)ds.add(fd(d));});stats+='<div class="stat-card green"><div class="val">'+ds.size+'</div><div class="lbl">Distinct Dates</div></div>';}
  const nIdx=cols.findIndex(c=>c.toLowerCase()==='patient name'||c.toLowerCase()==='patient_name'||c.toLowerCase()==='line_patient_name');
  if(nIdx>=0){const u=new Set(filtered.map(r=>r[nIdx]).filter(v=>v&&v!==''));stats+='<div class="stat-card orange"><div class="val">'+u.size+'</div><div class="lbl">Unique Patients</div></div>';}
  const fIdx=cols.findIndex(c=>c.toLowerCase().includes('service_facility')||c.toLowerCase()==='service facility');
  if(fIdx>=0){const u=new Set(filtered.map(r=>r[fIdx]).filter(v=>v&&v!==''));stats+='<div class="stat-card purple"><div class="val">'+u.size+'</div><div class="lbl">Facilities</div></div>';}
  stats+='</div>';

  const hasDate=dIdx>=0,dcn=hasDate?cols[dIdx]:'';
  let nav='';
  if(hasDate){
    nav='<div class="nav-btns">'
      +'<button class="period-nav-btn" onclick="gNavigate(\''+sid+'\',\''+sheet+'\',-1)">&#8249;</button>'
      +'<span class="period-label">'+gPeriodLabel(sheet)+'</span>'
      +'<button class="period-nav-btn" onclick="gNavigate(\''+sid+'\',\''+sheet+'\',1)">&#8250;</button>'
      +'</div>'
      +'<input type="date" class="date-input" title="Jump to date" onchange="gJump(\''+sheet+'\',this.value)">'
      +'<span class="date-field-label">by: '+esc(dcn)+'</span>';
  } else nav='<span class="period-label">All Dates</span>';

  let vbtns='';
  if(hasDate){
    vbtns='<div class="view-btns">'
      +'<button class="view-btn'+(view==='month'?' active':'')+'" onclick="gSetView(\''+sheet+'\',\'month\')">Month</button>'
      +'<button class="view-btn'+(view==='week' ?' active':'')+'" onclick="gSetView(\''+sheet+'\',\'week\')">Week</button>'
      +'<button class="view-btn'+(view==='day'  ?' active':'')+'" onclick="gSetView(\''+sheet+'\',\'day\')">Day</button>'
      +'</div>';
  }

  // Table
  let tbl='';
  if(!page.length){
    tbl='<div class="no-data">No records found for the selected period.</div>';
  } else {
    const ss=gSort[sheet];
    tbl='<div class="table-wrap"><table><thead><tr>';
    cols.forEach((c,i)=>{const arr=ss.col===i?(ss.asc?' &#9650;':' &#9660;'):'';tbl+='<th onclick="gSortCol(\''+sheet+'\','+i+')" title="Sort by '+esc(c)+'">'+esc(c)+arr+'</th>';});
    tbl+='</tr></thead><tbody>';
    page.forEach(row=>{tbl+='<tr>';row.forEach(cell=>{const v=esc(cell);tbl+='<td title="'+v+'">'+v+'</td>';});tbl+='</tr>';});
    tbl+='</tbody></table></div>';
  }

  // Pagination
  let pg='';
  if(totalPages>1){
    pg='<div class="pagination">';
    pg+='<span class="page-info">Showing '+(offset+1)+'\u2013'+Math.min(offset+PAGE_SIZE,filtered.length)+' of '+filtered.length.toLocaleString()+'</span>';
    if(curPage>1)pg+='<button class="page-btn" onclick="gGotoPage(\''+sheet+'\',0)">\u00ab</button>';
    if(curPage>1)pg+='<button class="page-btn" onclick="gGotoPage(\''+sheet+'\','+(offset-PAGE_SIZE)+')">\u2039</button>';
    for(let i=Math.max(0,curPage-3);i<Math.min(totalPages,curPage+2);i++)
      pg+='<button class="page-btn'+(i===curPage-1?' active':'')+'" onclick="gGotoPage(\''+sheet+'\','+(i*PAGE_SIZE)+')">'+(i+1)+'</button>';
    if(curPage<totalPages)pg+='<button class="page-btn" onclick="gGotoPage(\''+sheet+'\','+(offset+PAGE_SIZE)+')">\u203a</button>';
    if(curPage<totalPages)pg+='<button class="page-btn" onclick="gGotoPage(\''+sheet+'\','+((totalPages-1)*PAGE_SIZE)+')">\u00bb</button>';
    pg+='</div>';
  }

  panel.innerHTML='<div class="controls">'+vbtns+nav
    +'<input type="text" class="search-box" placeholder="Search all columns..." value="'+esc(gSearch[sheet])+'" oninput="gSearch2(\''+sheet+'\',this.value)">'
    +'<button class="export-btn" onclick="gExport(\''+sheet+'\')">&#8595; Export CSV</button>'
    +'</div>'
    +stats+tbl+pg;
}

// Alias for onclick (sheet id vs sheet name)
function gNavigate(sid, sheet, dir){ /* patched below */ }

/* ===== INIT ===== */
// Fix gNavigate to accept both sid and sheet
window.gNavigate = function(sheet, dir){
  const view=gView[sheet];let ref=new Date(gDate[sheet]||new Date());
  if(view==='month')ref=new Date(ref.getFullYear(),ref.getMonth()+dir,1);
  else if(view==='week')ref=new Date(ref.getTime()+dir*7*86400000);
  else ref=new Date(ref.getTime()+dir*86400000);
  gDate[sheet]=ref;gOffset[sheet]=0;renderGeneral(sheet);
};

// Build sidebar
(function buildSidebar(){
  const nav=document.getElementById('sidebarNav');
  // Billing
  const lbl1=document.createElement('div');lbl1.className='sb-section';lbl1.textContent='Billing';nav.appendChild(lbl1);
  const btnB=document.createElement('button');btnB.className='nav-item active';btnB.id='btn-billing';btnB.textContent='AR / Billing Dashboard';
  btnB.onclick=()=>showPage('billing');nav.appendChild(btnB);
  // Census
  const lblC=document.createElement('div');lblC.className='sb-section';lblC.textContent='Census';nav.appendChild(lblC);
  const btnC=document.createElement('button');btnC.className='nav-item';btnC.id='btn-census';btnC.textContent='Census Dashboard';
  btnC.onclick=()=>showPage('census');nav.appendChild(btnC);
  // Marketing
  const lblM=document.createElement('div');lblM.className='sb-section';lblM.textContent='Marketing';nav.appendChild(lblM);
  const btnM=document.createElement('button');btnM.className='nav-item';btnM.id='btn-marketing';btnM.textContent='Marketing Dashboard';
  btnM.onclick=()=>showPage('marketing');nav.appendChild(btnM);
  // Opportunities
  const lblO=document.createElement('div');lblO.className='sb-section';lblO.textContent='Opportunities';nav.appendChild(lblO);
  const btnO=document.createElement('button');btnO.className='nav-item';btnO.id='btn-opportunities';btnO.textContent='Opportunities Detail';
  btnO.onclick=()=>showPage('opportunities');nav.appendChild(btnO);
  // Utilization Review
  const lblUR=document.createElement('div');lblUR.className='sb-section';lblUR.textContent='Utilization Review';nav.appendChild(lblUR);
  const btnUR=document.createElement('button');btnUR.className='nav-item';btnUR.id='btn-ur';btnUR.textContent='UR Dashboard';
  btnUR.onclick=()=>showPage('ur');nav.appendChild(btnUR);
  // Clinical
  const lblCL=document.createElement('div');lblCL.className='sb-section';lblCL.textContent='Clinical';nav.appendChild(lblCL);
  const btnCL=document.createElement('button');btnCL.className='nav-item';btnCL.id='btn-clinical';btnCL.textContent='Clinical / Group Notes';
  btnCL.onclick=()=>showPage('clinical');nav.appendChild(btnCL);
  // Operations
  const lblOP=document.createElement('div');lblOP.className='sb-section';lblOP.textContent='Operations';nav.appendChild(lblOP);
  const btnOP=document.createElement('button');btnOP.className='nav-item';btnOP.id='btn-operations';btnOP.textContent='Operations Dashboard';
  btnOP.onclick=()=>showPage('operations');nav.appendChild(btnOP);
  // Field Explorer
  const lblFE=document.createElement('div');lblFE.className='sb-section';lblFE.textContent='Tools';nav.appendChild(lblFE);
  const btnFE=document.createElement('button');btnFE.className='nav-item';btnFE.id='btn-fieldexplorer';btnFE.textContent='Field Explorer';
  btnFE.onclick=()=>showPage('fieldexplorer');nav.appendChild(btnFE);
  // Reports
  const lbl2=document.createElement('div');lbl2.className='sb-section';lbl2.textContent='Reports';nav.appendChild(lbl2);
  SHEETS.forEach(s=>{
    const b=document.createElement('button');b.className='nav-item';b.id='btn-'+s.replace(/[^a-zA-Z0-9]/g,'_');
    b.textContent=s;b.title=s;b.onclick=()=>showPage(s);nav.appendChild(b);
  });
})();

// Build section containers
(function buildSections(){
  const wrap=document.getElementById('sectionsWrap');
  // General report panels
  SHEETS.forEach(s=>{
    const sid=s.replace(/[^a-zA-Z0-9]/g,'_');
    const sec=document.createElement('div');sec.className='page-section';sec.id='sec-'+sid;sec.style.display='none';
    const panel=document.createElement('div');panel.id='gpanel-'+sid;
    sec.appendChild(panel);wrap.appendChild(sec);
  });
})();

// Init general reports date
SHEETS.forEach(s=>{
  const dIdx=DATE_IDX[s];
  if(dIdx>=0)for(const row of RAW[s].rows){const d=pd(row[dIdx]);if(d){gDate[s]=d;break;}}
  if(!gDate[s])gDate[s]=new Date();
});

/* ═══════════════════════════════════════════════════════════════
   CENSUS LOGIC
═══════════════════════════════════════════════════════════════ */
const CROWS = JSON.parse(document.getElementById('censusData').textContent);

// Active census: patients who were active on date D
function cActive(rows, D) {
  return rows.filter(r => {
    const adm = pd(r.adm); if (!adm || adm > D) return false;
    const dis = pd(r.dis); return !dis || dis > D;
  });
}
// Admits in [from, to]
function cAdmits(rows, from, to) { return rows.filter(r => { const d=pd(r.adm); return d&&d>=from&&d<=to; }); }
// Discharges in [from, to]
function cDischarges(rows, from, to) { return rows.filter(r => { const d=pd(r.dis); return d&&d>=from&&d<=to; }); }

// LOC grouping order
const LOC_ORDER = ['Res 3.5','Res 3.1','PHP','Detox','IOP','Other'];
function allLOCs(rows) {
  const s=new Set(rows.map(r=>r.loc||'Other'));
  return LOC_ORDER.filter(l=>s.has(l)).concat([...s].filter(l=>!LOC_ORDER.includes(l)));
}

const CSPOT_PERIODS = [
  { label:'Today',        snap:()=>new Date(TODAY), from:()=>new Date(TODAY),        to:()=>new Date(TODAY) },
  { label:'Yesterday',    snap:()=>daysAgo(1),       from:()=>daysAgo(1),             to:()=>daysAgo(1) },
  { label:'Last 7 Days',  snap:()=>daysAgo(6),       from:()=>daysAgo(6),             to:()=>new Date(TODAY) },
  { label:'Last 31 Days', snap:()=>daysAgo(30),      from:()=>daysAgo(30),            to:()=>new Date(TODAY) },
  { label:'Last 12 Mo',   snap:()=>daysAgo(364),     from:()=>daysAgo(364),           to:()=>new Date(TODAY) },
  { label:'YTD',          snap:()=>new Date(TODAY.getFullYear(),0,1), from:()=>new Date(TODAY.getFullYear(),0,1), to:()=>new Date(TODAY) },
];

function renderCensusSpot() {
  const locs = allLOCs(CROWS);
  let h = '<table class="spot-table"><thead><tr><th>Metric / LOC</th>';
  CSPOT_PERIODS.forEach(p=>h+='<th>'+p.label+'</th>'); h+='</tr></thead><tbody>';

  // Section: ACTIVE CENSUS
  h+='<tr><td colspan="'+(CSPOT_PERIODS.length+1)+'" class="spot-section">ACTIVE CENSUS (as of period end)</td></tr>';
  locs.forEach(loc=>{
    h+='<tr><td class="metric-name">&nbsp;&nbsp;'+esc(loc)+'</td>';
    CSPOT_PERIODS.forEach(p=>{ const n=cActive(CROWS,p.snap()).filter(r=>r.loc===loc).length; h+='<td class="num">'+n+'</td>'; });
    h+='</tr>';
  });
  h+='<tr class="total-row"><td class="metric-name">&nbsp;&nbsp;Total</td>';
  CSPOT_PERIODS.forEach(p=>{ h+='<td class="num">'+cActive(CROWS,p.snap()).length+'</td>'; }); h+='</tr>';

  // Section: ADMITS
  h+='<tr><td colspan="'+(CSPOT_PERIODS.length+1)+'" class="spot-section">ADMITS (in period)</td></tr>';
  locs.forEach(loc=>{
    h+='<tr><td class="metric-name">&nbsp;&nbsp;'+esc(loc)+'</td>';
    CSPOT_PERIODS.forEach(p=>{ h+='<td class="num">'+cAdmits(CROWS,p.from(),p.to()).filter(r=>r.loc===loc).length+'</td>'; });
    h+='</tr>';
  });
  h+='<tr class="total-row"><td class="metric-name">&nbsp;&nbsp;Total</td>';
  CSPOT_PERIODS.forEach(p=>{ h+='<td class="num">'+cAdmits(CROWS,p.from(),p.to()).length+'</td>'; }); h+='</tr>';

  // Section: DISCHARGES
  h+='<tr><td colspan="'+(CSPOT_PERIODS.length+1)+'" class="spot-section">DISCHARGES (in period)</td></tr>';
  locs.forEach(loc=>{
    h+='<tr><td class="metric-name">&nbsp;&nbsp;'+esc(loc)+'</td>';
    CSPOT_PERIODS.forEach(p=>{ h+='<td class="num">'+cDischarges(CROWS,p.from(),p.to()).filter(r=>r.loc===loc).length+'</td>'; });
    h+='</tr>';
  });
  h+='<tr class="total-row"><td class="metric-name">&nbsp;&nbsp;Total</td>';
  CSPOT_PERIODS.forEach(p=>{ h+='<td class="num">'+cDischarges(CROWS,p.from(),p.to()).length+'</td>'; }); h+='</tr>';

  h+='</tbody></table>';
  document.getElementById('cSpotTable').innerHTML=h;

  // Discharge type breakdown
  let h2='<table class="spot-table"><thead><tr><th>Discharge Type</th>';
  CSPOT_PERIODS.forEach(p=>h2+='<th>'+p.label+'</th>'); h2+='</tr></thead><tbody>';
  const dtypes=['Successful','AMA/ASA','Administrative','Transferred'];
  dtypes.forEach(dt=>{
    h2+='<tr><td class="metric-name">'+esc(dt)+'</td>';
    CSPOT_PERIODS.forEach(p=>{
      const dis=cDischarges(CROWS,p.from(),p.to());
      const cnt=dis.filter(r=>r.dtype.startsWith(dt)).length;
      const pct=dis.length>0?(cnt/dis.length*100).toFixed(1)+'%':'0%';
      h2+='<td class="num">'+pct+'</td>';
    });
    h2+='</tr>';
  });
  h2+='<tr class="total-row"><td class="metric-name">Total (count)</td>';
  CSPOT_PERIODS.forEach(p=>h2+='<td class="num">'+cDischarges(CROWS,p.from(),p.to()).length+'</td>');
  h2+='</tr></tbody></table>';
  document.getElementById('cDischargeBreakdown').innerHTML=h2;
}

// ── Monthly trend ──────────────────────────────────────────────────────────
function renderCensusTrend() {
  const months=[];
  for(let i=5;i>=0;i--){const d=new Date(TODAY.getFullYear(),TODAY.getMonth()-i,1);months.push({label:d.toLocaleString('default',{month:'short',year:'2-digit'}),start:d,end:eom(d)});}
  const locs=allLOCs(CROWS);
  const sections=[
    { title:'ACTIVE CENSUS (end of month)', fn:(m,loc)=>cActive(CROWS,m.end).filter(r=>r.loc===loc).length, total:(m)=>cActive(CROWS,m.end).length },
    { title:'ADMITS', fn:(m,loc)=>cAdmits(CROWS,m.start,m.end).filter(r=>r.loc===loc).length, total:(m)=>cAdmits(CROWS,m.start,m.end).length },
    { title:'DISCHARGES', fn:(m,loc)=>cDischarges(CROWS,m.start,m.end).filter(r=>r.loc===loc).length, total:(m)=>cDischarges(CROWS,m.start,m.end).length },
    { title:'NET GROWTH (Admits \u2013 Discharges)', fn:(m,loc)=>cAdmits(CROWS,m.start,m.end).filter(r=>r.loc===loc).length-cDischarges(CROWS,m.start,m.end).filter(r=>r.loc===loc).length, total:(m)=>cAdmits(CROWS,m.start,m.end).length-cDischarges(CROWS,m.start,m.end).length },
  ];
  let h='<table class="trend-table"><thead><tr><th>Metric / LOC</th>';
  months.forEach(m=>h+='<th>'+m.label+'</th>'); h+='</tr></thead><tbody>';
  sections.forEach(sec=>{
    h+='<tr><td colspan="'+(months.length+1)+'" class="spot-section">'+sec.title+'</td></tr>';
    locs.forEach(loc=>{
      h+='<tr><td class="metric-name">&nbsp;&nbsp;'+esc(loc)+'</td>';
      months.forEach(m=>{const v=sec.fn(m,loc);h+='<td class="num">'+(v>0?v:v<0?'<span style="color:#c00">'+v+'</span>':0)+'</td>';});
      h+='</tr>';
    });
    h+='<tr class="total-row"><td class="metric-name">&nbsp;&nbsp;Total</td>';
    months.forEach(m=>{const v=sec.total(m);h+='<td class="num">'+(v>0?v:v<0?'<span style="color:#c00">'+v+'</span>':0)+'</td>';});
    h+='</tr>';
  });
  h+='</tbody></table>';
  document.getElementById('cTrendTable').innerHTML=h;
}

// ── Period nav state for breakdowns ───────────────────────────────────────
let cNavView='month', cNavDate=null;
for(const r of CROWS){const d=pd(r.adm);if(d){cNavDate=d;break;}}
if(!cNavDate)cNavDate=new Date();

function cNavRange(){
  if(cNavView==='month')return{from:som(cNavDate),to:eom(cNavDate)};
  if(cNavView==='week'){const ws=gwk(cNavDate),we=new Date(ws);we.setDate(we.getDate()+6);return{from:ws,to:we};}
  return{from:new Date(cNavDate),to:new Date(cNavDate)};
}
function cNavLabel(){
  if(cNavView==='month')return cNavDate.toLocaleString('default',{month:'long'})+' '+cNavDate.getFullYear();
  if(cNavView==='week'){const{from,to}=cNavRange();return fd(from)+' \u2013 '+fd(to);}
  return fd(cNavDate);
}
function cNavigate(dir){
  if(cNavView==='month')cNavDate=new Date(cNavDate.getFullYear(),cNavDate.getMonth()+dir,1);
  else if(cNavView==='week')cNavDate=new Date(cNavDate.getTime()+dir*7*86400000);
  else cNavDate=new Date(cNavDate.getTime()+dir*86400000);
  renderCensusBreakdowns();
}
function cSetView(v){cNavView=v;renderCensusBreakdowns();}
function cJump(val){if(!val)return;const p=val.split('-');cNavDate=new Date(+p[0],+p[1]-1,+p[2]);renderCensusBreakdowns();}

function pctRow(label,cnt,total){
  const pct=total>0?(cnt/total*100).toFixed(1)+'%':'0%';
  return '<tr><td>'+esc(label)+'</td><td class="num">'+cnt+'</td><td class="num">'+pct+'</td></tr>';
}

function renderCensusBreakdowns(){
  document.getElementById('cPeriodLabel').innerHTML=cNavLabel();
  document.getElementById('cViewMonth').className='view-btn'+(cNavView==='month'?' active':'');
  document.getElementById('cViewWeek').className='view-btn'+(cNavView==='week'?' active':'');
  document.getElementById('cViewDay').className='view-btn'+(cNavView==='day'?' active':'');

  const{from,to}=cNavRange();
  const admits=cAdmits(CROWS,from,to);
  const discharges=cDischarges(CROWS,from,to);

  // KPI cards
  const snapDate=to;
  const activeNow=cActive(CROWS,snapDate);
  document.getElementById('cKpiActive').textContent=activeNow.length;
  document.getElementById('cKpiAdmits').textContent=admits.length;
  document.getElementById('cKpiDischarges').textContent=discharges.length;
  document.getElementById('cKpiNet').textContent=(admits.length-discharges.length>=0?'+':'')+(admits.length-discharges.length);
  const los=admits.filter(r=>r.los!==null).map(r=>r.los);
  document.getElementById('cKpiLOS').textContent=los.length>0?(los.reduce((s,v)=>s+v,0)/los.length).toFixed(1)+' days':'N/A';

  // Gender
  const genF=admits.filter(r=>r.gen==='F').length, genM=admits.filter(r=>r.gen==='M').length;
  const gTotal=admits.filter(r=>r.gen).length;
  let hg='<table class="break-table"><thead><tr><th>Gender</th><th># Admits</th><th>%</th></tr></thead><tbody>';
  hg+=pctRow('Female',genF,gTotal)+pctRow('Male',genM,gTotal);
  hg+='<tr class="total-row"><td>Total</td><td class="num">'+gTotal+'</td><td class="num">100%</td></tr></tbody></table>';
  document.getElementById('cGenderTable').innerHTML=hg;

  // Age buckets
  const ageBuckets=[['18-24',18,24],['25-34',25,34],['35-44',35,44],['45-54',45,54],['55-64',55,64],['65+',65,999]];
  const aTotal=admits.filter(r=>r.age!==null).length;
  let ha='<table class="break-table"><thead><tr><th>Age Bucket</th><th># Admits</th><th>%</th></tr></thead><tbody>';
  ageBuckets.forEach(([lbl,lo,hi])=>{
    const cnt=admits.filter(r=>r.age!==null&&r.age>=lo&&r.age<=hi).length;
    ha+=pctRow(lbl,cnt,aTotal);
  });
  ha+='<tr class="total-row"><td>Total</td><td class="num">'+aTotal+'</td><td class="num">100%</td></tr></tbody></table>';
  document.getElementById('cAgeTable').innerHTML=ha;

  // Drug of choice
  const drugs=['Alcohol','Stimulant','Opioid','Other'];
  const dAdmits=admits.filter(r=>r.drug);
  let hd='<table class="break-table"><thead><tr><th>Primary Drug</th><th># Admits</th><th>%</th></tr></thead><tbody>';
  drugs.forEach(drg=>{
    const cnt=dAdmits.filter(r=>r.drug===drg).length;
    hd+=pctRow(drg,cnt,dAdmits.length);
  });
  const otherDrugs=dAdmits.filter(r=>!drugs.includes(r.drug)).length;
  if(otherDrugs>0)hd+=pctRow('Other (unlisted)',otherDrugs,dAdmits.length);
  hd+='<tr class="total-row"><td>Total</td><td class="num">'+dAdmits.length+'</td><td class="num">100%</td></tr></tbody></table>';
  document.getElementById('cDrugTable').innerHTML=hd;

  // Discharge type
  const dtypes=['Successful','AMA/ASA','Administrative','Transferred'];
  let hdt='<table class="break-table"><thead><tr><th>Discharge Type</th><th># Discharges</th><th>%</th></tr></thead><tbody>';
  dtypes.forEach(dt=>{
    const cnt=discharges.filter(r=>r.dtype.startsWith(dt)).length;
    hdt+=pctRow(dt,cnt,discharges.length);
  });
  hdt+='<tr class="total-row"><td>Total</td><td class="num">'+discharges.length+'</td><td class="num">100%</td></tr></tbody></table>';
  document.getElementById('cDtypeTable').innerHTML=hdt;

  // Referral sources
  const refMap={};
  admits.forEach(r=>{if(r.ref)refMap[r.ref]=(refMap[r.ref]||0)+1;});
  const refSorted=Object.entries(refMap).sort((a,b)=>b[1]-a[1]);
  const top10=refSorted.slice(0,10), others=refSorted.slice(10).reduce((s,[,v])=>s+v,0);
  let hr='<table class="break-table"><thead><tr><th>Referral Source</th><th># Admits</th><th>%</th></tr></thead><tbody>';
  top10.forEach(([k,v])=>hr+=pctRow(k,v,admits.length));
  if(others>0)hr+=pctRow('Others',others,admits.length);
  hr+='<tr class="total-row"><td>Total</td><td class="num">'+admits.length+'</td><td class="num">100%</td></tr></tbody></table>';
  document.getElementById('cReferralTable').innerHTML=hr;

  // LOS table
  const locs2=allLOCs(CROWS);
  const cohort=CROWS.filter(r=>r.los!==null&&r.dis);
  let hl='<table class="break-table"><thead><tr><th>Level of Care</th><th>Avg LOS (days)</th><th># Patients</th></tr></thead><tbody>';
  locs2.forEach(loc=>{
    const pts=cohort.filter(r=>r.loc===loc);
    const avg=pts.length>0?(pts.reduce((s,r)=>s+r.los,0)/pts.length).toFixed(1):'-';
    hl+='<tr><td>'+esc(loc)+'</td><td class="num">'+avg+'</td><td class="num">'+pts.length+'</td></tr>';
  });
  const allAvg=cohort.length>0?(cohort.reduce((s,r)=>s+r.los,0)/cohort.length).toFixed(1):'-';
  hl+='<tr class="total-row"><td>All LOC (overall)</td><td class="num">'+allAvg+'</td><td class="num">'+cohort.length+'</td></tr>';
  hl+='</tbody></table>';
  document.getElementById('cLOSTable').innerHTML=hl;
}

/* ═══════════════════════════════════════════════════════════════
   MARKETING LOGIC
═══════════════════════════════════════════════════════════════ */
const OROWS = JSON.parse(document.getElementById('oppData').textContent);

function mFilter(rows,from,to){return rows.filter(r=>{const d=pd(r.co);return d&&d>=from&&d<=to;});}
function mMetrics(rows){
  const total=rows.length;
  const admitted=rows.filter(r=>r.outcome==='Admitted'||r.outcome==='AdmittedLegacy').length;
  const active=rows.filter(r=>r.outcome==='Active').length;
  const scheduled=rows.filter(r=>r.outcome==='Scheduled').length;
  const lost=rows.filter(r=>r.outcome==='Lost').length;
  const abandoned=rows.filter(r=>r.outcome==='Abandoned').length;
  return{total,admitted,active,scheduled,lost,abandoned,rate:total>0?admitted/total:0};
}
const MKT_PERIODS=[
  {label:'Today',from:()=>new Date(TODAY),to:()=>new Date(TODAY)},
  {label:'Yesterday',from:()=>daysAgo(1),to:()=>daysAgo(1)},
  {label:'Last 7 Days',from:()=>daysAgo(6),to:()=>new Date(TODAY)},
  {label:'Last 31 Days',from:()=>daysAgo(30),to:()=>new Date(TODAY)},
  {label:'Last 90 Days',from:()=>daysAgo(89),to:()=>new Date(TODAY)},
  {label:'Last 12 Mo',from:()=>daysAgo(364),to:()=>new Date(TODAY)},
  {label:'YTD',from:()=>new Date(TODAY.getFullYear(),0,1),to:()=>new Date(TODAY)},
];
const MKT_METRICS=[
  {key:'total',label:'Total Created',fmt:v=>v.toLocaleString()},
  {key:'active',label:'Active',fmt:v=>v.toLocaleString()},
  {key:'scheduled',label:'Scheduled',fmt:v=>v.toLocaleString()},
  {key:'admitted',label:'Admitted',fmt:v=>v.toLocaleString()},
  {key:'lost',label:'Lost',fmt:v=>v.toLocaleString()},
  {key:'abandoned',label:'Abandoned',fmt:v=>v.toLocaleString()},
  {key:'rate',label:'Admit Rate',fmt:v=>fmtPct(v)},
];

function renderMarketingSpot(){
  let h='<table class="spot-table"><thead><tr><th>Metric</th>';
  MKT_PERIODS.forEach(p=>h+='<th>'+p.label+'</th>'); h+='</tr></thead><tbody>';
  MKT_METRICS.forEach(m=>{
    h+='<tr><td class="metric-name">'+m.label+'</td>';
    MKT_PERIODS.forEach(p=>{const c=mMetrics(mFilter(OROWS,p.from(),p.to()));h+='<td class="num">'+m.fmt(c[m.key])+'</td>';});
    h+='</tr>';
  });
  h+='</tbody></table>';
  document.getElementById('mktSpot').innerHTML=h;
}

let mktNavView='month',mktNavDate=null;
for(const r of OROWS){const d=pd(r.co);if(d){mktNavDate=d;break;}}
if(!mktNavDate)mktNavDate=new Date();

function mktNavRange(){
  if(mktNavView==='month')return{from:som(mktNavDate),to:eom(mktNavDate)};
  if(mktNavView==='week'){const ws=gwk(mktNavDate),we=new Date(ws);we.setDate(we.getDate()+6);return{from:ws,to:we};}
  return{from:new Date(mktNavDate),to:new Date(mktNavDate)};
}
function mktNavLabel(){
  if(mktNavView==='month')return mktNavDate.toLocaleString('default',{month:'long'})+' '+mktNavDate.getFullYear();
  if(mktNavView==='week'){const{from,to}=mktNavRange();return fd(from)+' \u2013 '+fd(to);}
  return fd(mktNavDate);
}
function mktNavigate(dir){
  if(mktNavView==='month')mktNavDate=new Date(mktNavDate.getFullYear(),mktNavDate.getMonth()+dir,1);
  else if(mktNavView==='week')mktNavDate=new Date(mktNavDate.getTime()+dir*7*86400000);
  else mktNavDate=new Date(mktNavDate.getTime()+dir*86400000);
  renderMarketingDetail();
}
function mktSetView(v){mktNavView=v;renderMarketingDetail();}
function mktJump(val){if(!val)return;const p=val.split('-');mktNavDate=new Date(+p[0],+p[1]-1,+p[2]);renderMarketingDetail();}

function renderMarketingTrend(){
  const months=[];for(let i=5;i>=0;i--){const d=new Date(TODAY.getFullYear(),TODAY.getMonth()-i,1);months.push({label:d.toLocaleString('default',{month:'short',year:'2-digit'}),from:d,to:eom(d)});}
  let h='<table class="trend-table"><thead><tr><th>Metric</th>';
  months.forEach(m=>h+='<th>'+m.label+'</th>'); h+='</tr></thead><tbody>';
  MKT_METRICS.forEach(m=>{
    h+='<tr><td class="metric-name">'+m.label+'</td>';
    months.forEach(mo=>{const c=mMetrics(mFilter(OROWS,mo.from,mo.to));h+='<td class="num">'+m.fmt(c[m.key])+'</td>';});
    h+='</tr>';
  });
  h+='</tbody></table>';
  document.getElementById('mktTrend').innerHTML=h;
}

function renderMarketingDetail(){
  document.getElementById('mktPeriodLabel').innerHTML=mktNavLabel();
  document.getElementById('mktViewMonth').className='view-btn'+(mktNavView==='month'?' active':'');
  document.getElementById('mktViewWeek').className='view-btn'+(mktNavView==='week'?' active':'');
  document.getElementById('mktViewDay').className='view-btn'+(mktNavView==='day'?' active':'');
  const{from,to}=mktNavRange();
  const rows=mFilter(OROWS,from,to);
  const m=mMetrics(rows);
  document.getElementById('mktKpiTotal').textContent=m.total;
  document.getElementById('mktKpiAdmitted').textContent=m.admitted;
  document.getElementById('mktKpiRate').textContent=fmtPct(m.rate);
  document.getElementById('mktKpiActive').textContent=m.active;
  document.getElementById('mktKpiLost').textContent=m.lost;
  document.getElementById('mktKpiAbandoned').textContent=m.abandoned;

  // Funnel
  const funnelItems=[
    {label:'Created',val:m.total,color:'#1a3a5c'},
    {label:'Active',val:m.active,color:'#1a6ec0'},
    {label:'Scheduled',val:m.scheduled,color:'#2196f3'},
    {label:'Admitted',val:m.admitted,color:'#217346'},
    {label:'Lost',val:m.lost,color:'#c86a00'},
    {label:'Abandoned',val:m.abandoned,color:'#888'},
  ];
  const maxV=m.total||1;
  let hf='';
  funnelItems.forEach(f=>{
    const pct=Math.round(f.val/maxV*100);
    const dark=pct<15;
    hf+='<div class="funnel-bar"><span class="funnel-label">'+f.label+'</span>'
      +'<div class="funnel-track"><div class="funnel-fill" style="width:'+pct+'%;background:'+f.color+'"></div>'
      +'<span class="funnel-val'+(dark?' dark':'')+'">'+f.val+' ('+fmtPct(m.total>0?f.val/m.total:0)+')</span></div></div>';
  });
  document.getElementById('mktFunnel').innerHTML=hf;

  // Lost reasons
  const lostRows=OROWS.filter(r=>r.outcome==='Lost'&&r.lost_r);
  const lostMap={};lostRows.forEach(r=>lostMap[r.lost_r]=(lostMap[r.lost_r]||0)+1);
  let hl='<table class="break-table"><thead><tr><th>Reason</th><th>#</th><th>%</th></tr></thead><tbody>';
  Object.entries(lostMap).sort((a,b)=>b[1]-a[1]).forEach(([k,v])=>hl+='<tr><td>'+esc(k)+'</td><td class="num">'+v+'</td><td class="num">'+(lostRows.length>0?(v/lostRows.length*100).toFixed(1)+'%':'0%')+'</td></tr>');
  hl+='<tr class="total-row"><td>Total</td><td class="num">'+lostRows.length+'</td><td class="num">100%</td></tr></tbody></table>';
  document.getElementById('mktLostTable').innerHTML=hl;

  // Abandoned reasons
  const abandRows=OROWS.filter(r=>r.outcome==='Abandoned'&&r.aband_r);
  const abandMap={};abandRows.forEach(r=>abandMap[r.aband_r]=(abandMap[r.aband_r]||0)+1);
  let ha='<table class="break-table"><thead><tr><th>Reason</th><th>#</th><th>%</th></tr></thead><tbody>';
  Object.entries(abandMap).sort((a,b)=>b[1]-a[1]).forEach(([k,v])=>ha+='<tr><td>'+esc(k)+'</td><td class="num">'+v+'</td><td class="num">'+(abandRows.length>0?(v/abandRows.length*100).toFixed(1)+'%':'0%')+'</td></tr>');
  ha+='<tr class="total-row"><td>Total</td><td class="num">'+abandRows.length+'</td><td class="num">100%</td></tr></tbody></table>';
  document.getElementById('mktAbandTable').innerHTML=ha;

  // Top referral sources for admitted
  const admRows=rows.filter(r=>r.outcome==='Admitted'||r.outcome==='AdmittedLegacy');
  const refMap={};admRows.forEach(r=>{if(r.ref)refMap[r.ref]=(refMap[r.ref]||0)+1;});
  const topRef=Object.entries(refMap).sort((a,b)=>b[1]-a[1]).slice(0,10);
  let hr='<table class="break-table"><thead><tr><th>Referral Source</th><th>Admitted</th><th>%</th></tr></thead><tbody>';
  topRef.forEach(([k,v])=>hr+='<tr><td>'+esc(k)+'</td><td class="num">'+v+'</td><td class="num">'+(admRows.length>0?(v/admRows.length*100).toFixed(1)+'%':'0%')+'</td></tr>');
  hr+='<tr class="total-row"><td>Total</td><td class="num">'+admRows.length+'</td><td class="num">100%</td></tr></tbody></table>';
  document.getElementById('mktRefTable').innerHTML=hr;
}

/* ═══════════════════════════════════════════════════════════════
   OPPORTUNITIES DETAIL LOGIC
═══════════════════════════════════════════════════════════════ */
let oppNavView='month',oppNavDate=null,oppSearch='',oppSortCol='co',oppSortAsc=false;
for(const r of OROWS){const d=pd(r.co);if(d){oppNavDate=d;break;}}
if(!oppNavDate)oppNavDate=new Date();

function oppNavRange(){
  if(oppNavView==='month')return{from:som(oppNavDate),to:eom(oppNavDate)};
  if(oppNavView==='week'){const ws=gwk(oppNavDate),we=new Date(ws);we.setDate(we.getDate()+6);return{from:ws,to:we};}
  return{from:new Date(oppNavDate),to:new Date(oppNavDate)};
}
function oppNavLabel(){
  if(oppNavView==='month')return oppNavDate.toLocaleString('default',{month:'long'})+' '+oppNavDate.getFullYear();
  if(oppNavView==='week'){const{from,to}=oppNavRange();return fd(from)+' \u2013 '+fd(to);}
  return fd(oppNavDate);
}
function oppNavigate(dir){
  if(oppNavView==='month')oppNavDate=new Date(oppNavDate.getFullYear(),oppNavDate.getMonth()+dir,1);
  else if(oppNavView==='week')oppNavDate=new Date(oppNavDate.getTime()+dir*7*86400000);
  else oppNavDate=new Date(oppNavDate.getTime()+dir*86400000);
  renderOpportunities();
}
function oppSetView(v){oppNavView=v;renderOpportunities();}
function oppJump(val){if(!val)return;const p=val.split('-');oppNavDate=new Date(+p[0],+p[1]-1,+p[2]);renderOpportunities();}
function oppSetSort(col){if(oppSortCol===col)oppSortAsc=!oppSortAsc;else{oppSortCol=col;oppSortAsc=false;}renderOpportunities();}

function renderOpportunities(){
  document.getElementById('oppPeriodLabel').innerHTML=oppNavLabel();
  document.getElementById('oppViewMonth').className='view-btn'+(oppNavView==='month'?' active':'');
  document.getElementById('oppViewWeek').className='view-btn'+(oppNavView==='week'?' active':'');
  document.getElementById('oppViewDay').className='view-btn'+(oppNavView==='day'?' active':'');
  const{from,to}=oppNavRange();
  let rows=mFilter(OROWS,from,to);
  if(oppSearch){const s=oppSearch.toLowerCase();rows=rows.filter(r=>Object.values(r).some(v=>String(v).toLowerCase().includes(s)));}
  rows=rows.slice().sort((a,b)=>{
    const av=String(a[oppSortCol]||''),bv=String(b[oppSortCol]||'');
    const ad2=pd(av),bd2=pd(bv);if(ad2&&bd2)return oppSortAsc?(ad2-bd2):(bd2-ad2);
    return oppSortAsc?av.localeCompare(bv):bv.localeCompare(av);
  });
  // KPIs
  const m=mMetrics(rows);
  document.getElementById('oppKpiTotal').textContent=m.total;
  document.getElementById('oppKpiAdmitted').textContent=m.admitted;
  document.getElementById('oppKpiRate').textContent=fmtPct(m.rate);
  document.getElementById('oppKpiActive').textContent=m.active;
  // By outcome
  const outcomes=['Active','Scheduled','Admitted','AdmittedLegacy','Lost','Abandoned'];
  const outcomeMap={};rows.forEach(r=>outcomeMap[r.outcome]=(outcomeMap[r.outcome]||0)+1);
  let ho='<table class="break-table"><thead><tr><th>Outcome</th><th>#</th><th>%</th></tr></thead><tbody>';
  Object.entries(outcomeMap).sort((a,b)=>b[1]-a[1]).forEach(([k,v])=>ho+='<tr><td>'+esc(k)+'</td><td class="num">'+v+'</td><td class="num">'+(rows.length>0?(v/rows.length*100).toFixed(1)+'%':'0%')+'</td></tr>');
  ho+='<tr class="total-row"><td>Total</td><td class="num">'+rows.length+'</td><td class="num">100%</td></tr></tbody></table>';
  document.getElementById('oppOutcomeTable').innerHTML=ho;
  // By insurance
  const insMap={};rows.forEach(r=>{if(r.ins)insMap[r.ins]=(insMap[r.ins]||0)+1;});
  let hi='<table class="break-table"><thead><tr><th>Insurance</th><th>#</th><th>%</th></tr></thead><tbody>';
  Object.entries(insMap).sort((a,b)=>b[1]-a[1]).slice(0,10).forEach(([k,v])=>hi+='<tr><td>'+esc(k)+'</td><td class="num">'+v+'</td><td class="num">'+(rows.length>0?(v/rows.length*100).toFixed(1)+'%':'0%')+'</td></tr>');
  hi+='<tr class="total-row"><td>Total</td><td class="num">'+rows.length+'</td><td class="num">100%</td></tr></tbody></table>';
  document.getElementById('oppInsTable').innerHTML=hi;
  // Table
  const cols=['co','name','outcome','stage','loc','ins','ref'];
  const hdrs=['Created On','Patient','Outcome','Stage','LOC','Insurance','Referral'];
  let ht='<div class="table-wrap"><table><thead><tr>';
  cols.forEach((c,i)=>{const arr=oppSortCol===c?(oppSortAsc?' &#9650;':' &#9660;'):'';ht+='<th onclick="oppSetSort(\''+c+'\')">'+hdrs[i]+arr+'</th>';});
  ht+='</tr></thead><tbody>';
  const pg=rows.slice(0,150);
  if(!pg.length)ht+='<tr><td colspan="'+cols.length+'" class="no-data">No records.</td></tr>';
  pg.forEach(r=>{ht+='<tr>';cols.forEach(c=>{ht+='<td title="'+esc(r[c])+'">'+esc(r[c])+'</td>';});ht+='</tr>';});
  ht+='</tbody></table></div>';
  if(rows.length>150)ht+='<div class="page-info" style="margin-top:6px">Showing 150 of '+rows.length+' records.</div>';
  document.getElementById('oppTableWrap').innerHTML=ht;
}

/* ═══════════════════════════════════════════════════════════════
   UTILIZATION REVIEW LOGIC
═══════════════════════════════════════════════════════════════ */
const AROWS = JSON.parse(document.getElementById('authData').textContent);

function aFilter(rows,from,to){return rows.filter(r=>{const d=pd(r.adm);return d&&d>=from&&d<=to;});}
function aMetrics(rows){
  const auths=new Set(rows.filter(r=>r.code).map(r=>r.code)).size;
  const au=rows.reduce((s,r)=>s+r.au,0);
  const bu=rows.reduce((s,r)=>s+r.bu,0);
  return{auths,au:Math.round(au),bu:Math.round(bu),rem:Math.round(au-bu),util:au>0?bu/au:0,avg:auths>0?au/auths:0};
}
const UR_PERIODS=MKT_PERIODS;
const UR_METRICS=[
  {key:'auths',label:'# Authorizations',fmt:v=>v.toLocaleString()},
  {key:'au',label:'Total Authorized Units',fmt:v=>v.toLocaleString()},
  {key:'bu',label:'Total Billed Units',fmt:v=>v.toLocaleString()},
  {key:'rem',label:'Remaining Units',fmt:v=>v.toLocaleString()},
  {key:'util',label:'Auth Utilization %',fmt:fmtPct},
  {key:'avg',label:'Avg Units per Auth',fmt:v=>v.toFixed(1)},
];

function renderURSpot(){
  let h='<table class="spot-table"><thead><tr><th>Metric</th>';
  UR_PERIODS.forEach(p=>h+='<th>'+p.label+'</th>'); h+='</tr></thead><tbody>';
  UR_METRICS.forEach(m=>{
    h+='<tr><td class="metric-name">'+m.label+'</td>';
    UR_PERIODS.forEach(p=>{const c=aMetrics(aFilter(AROWS,p.from(),p.to()));h+='<td class="num">'+m.fmt(c[m.key])+'</td>';});
    h+='</tr>';
  });
  h+='</tbody></table>';
  document.getElementById('urSpot').innerHTML=h;
  // Upcoming reviews
  const now=new Date(TODAY);
  const thresholds=[{label:'Overdue',days:-1},{label:'Due Today',days:0},{label:'Due in 3 Days',days:3},{label:'Due in 7 Days',days:7},{label:'Due in 14 Days',days:14}];
  let hu='<table class="break-table"><thead><tr><th>Review Status</th><th># Auths</th></tr></thead><tbody>';
  thresholds.forEach(t=>{
    const cnt=AROWS.filter(r=>{
      const d=pd(r.nrd);if(!d)return false;
      const diff=Math.floor((d-now)/86400000);
      if(t.days<0)return diff<0;
      if(t.days===0)return diff===0;
      return diff>0&&diff<=t.days;
    }).length;
    hu+='<tr><td>'+t.label+'</td><td class="num">'+cnt+'</td></tr>';
  });
  hu+='</tbody></table>';
  document.getElementById('urUpcoming').innerHTML=hu;
}

function renderURTrend(){
  const months=[];for(let i=5;i>=0;i--){const d=new Date(TODAY.getFullYear(),TODAY.getMonth()-i,1);months.push({label:d.toLocaleString('default',{month:'short',year:'2-digit'}),from:d,to:eom(d)});}
  let h='<table class="trend-table"><thead><tr><th>Metric</th>';
  months.forEach(m=>h+='<th>'+m.label+'</th>'); h+='</tr></thead><tbody>';
  UR_METRICS.forEach(m=>{
    h+='<tr><td class="metric-name">'+m.label+'</td>';
    months.forEach(mo=>{const c=aMetrics(aFilter(AROWS,mo.from,mo.to));h+='<td class="num">'+m.fmt(c[m.key])+'</td>';});
    h+='</tr>';
  });
  h+='</tbody></table>';
  document.getElementById('urTrend').innerHTML=h;
  // By insurance (YTD)
  const ytd=aFilter(AROWS,new Date(TODAY.getFullYear(),0,1),TODAY);
  const insMap={};ytd.forEach(r=>{if(r.ins){const au=r.au,bu=r.bu;if(!insMap[r.ins])insMap[r.ins]={auths:new Set(),au:0,bu:0};insMap[r.ins].auths.add(r.code);insMap[r.ins].au+=au;insMap[r.ins].bu+=bu;}});
  let hi='<table class="break-table"><thead><tr><th>Insurance (YTD)</th><th>Auths</th><th>Auth Units</th><th>Billed</th><th>Util %</th></tr></thead><tbody>';
  Object.entries(insMap).sort((a,b)=>b[1].au-a[1].au).forEach(([k,v])=>{
    hi+='<tr><td>'+esc(k)+'</td><td class="num">'+v.auths.size+'</td><td class="num">'+Math.round(v.au)+'</td><td class="num">'+Math.round(v.bu)+'</td><td class="num">'+(v.au>0?(v.bu/v.au*100).toFixed(1)+'%':'0%')+'</td></tr>';
  });
  hi+='</tbody></table>';
  document.getElementById('urInsTable').innerHTML=hi;
  // By reviewer
  const revMap={};ytd.forEach(r=>{if(r.reviewer){if(!revMap[r.reviewer])revMap[r.reviewer]={auths:new Set(),au:0,bu:0};revMap[r.reviewer].auths.add(r.code);revMap[r.reviewer].au+=r.au;revMap[r.reviewer].bu+=r.bu;}});
  let hrev='<table class="break-table"><thead><tr><th>UR Reviewer (YTD)</th><th>Auths</th><th>Auth Units</th><th>Billed</th><th>Util %</th></tr></thead><tbody>';
  Object.entries(revMap).sort((a,b)=>b[1].auths.size-a[1].auths.size).forEach(([k,v])=>{
    hrev+='<tr><td>'+esc(k)+'</td><td class="num">'+v.auths.size+'</td><td class="num">'+Math.round(v.au)+'</td><td class="num">'+Math.round(v.bu)+'</td><td class="num">'+(v.au>0?(v.bu/v.au*100).toFixed(1)+'%':'0%')+'</td></tr>';
  });
  hrev+='</tbody></table>';
  document.getElementById('urRevTable').innerHTML=hrev;
}

/* ═══════════════════════════════════════════════════════════════
   CLINICAL LOGIC
═══════════════════════════════════════════════════════════════ */
const GNROWS = JSON.parse(document.getElementById('gnData').textContent);

function gnFilter(rows,from,to){return rows.filter(r=>{const d=pd(r.date);return d&&d>=from&&d<=to;});}

let clinNavView='month',clinNavDate=null;
for(const r of GNROWS){const d=pd(r.date);if(d){clinNavDate=d;break;}}
if(!clinNavDate)clinNavDate=new Date();

function clinNavRange(){
  if(clinNavView==='month')return{from:som(clinNavDate),to:eom(clinNavDate)};
  if(clinNavView==='week'){const ws=gwk(clinNavDate),we=new Date(ws);we.setDate(we.getDate()+6);return{from:ws,to:we};}
  return{from:new Date(clinNavDate),to:new Date(clinNavDate)};
}
function clinNavLabel(){
  if(clinNavView==='month')return clinNavDate.toLocaleString('default',{month:'long'})+' '+clinNavDate.getFullYear();
  if(clinNavView==='week'){const{from,to}=clinNavRange();return fd(from)+' \u2013 '+fd(to);}
  return fd(clinNavDate);
}
function clinNavigate(dir){
  if(clinNavView==='month')clinNavDate=new Date(clinNavDate.getFullYear(),clinNavDate.getMonth()+dir,1);
  else if(clinNavView==='week')clinNavDate=new Date(clinNavDate.getTime()+dir*7*86400000);
  else clinNavDate=new Date(clinNavDate.getTime()+dir*86400000);
  renderClinical();
}
function clinSetView(v){clinNavView=v;renderClinical();}
function clinJump(val){if(!val)return;const p=val.split('-');clinNavDate=new Date(+p[0],+p[1]-1,+p[2]);renderClinical();}

function renderClinicalSpot(){
  const months=[];for(let i=5;i>=0;i--){const d=new Date(TODAY.getFullYear(),TODAY.getMonth()-i,1);months.push({label:d.toLocaleString('default',{month:'short',year:'2-digit'}),from:d,to:eom(d)});}
  let h='<table class="trend-table"><thead><tr><th>Metric</th>';
  months.forEach(m=>h+='<th>'+m.label+'</th>'); h+='</tr></thead><tbody>';
  const clinMetrics=[
    {label:'# Sessions',fn:rs=>rs.length},
    {label:'Total Hours',fn:rs=>(rs.reduce((s,r)=>s+r.mins,0)/60).toFixed(1)},
    {label:'Active Sessions',fn:rs=>rs.filter(r=>r.status==='active').length},
  ];
  clinMetrics.forEach(m=>{
    h+='<tr><td class="metric-name">'+m.label+'</td>';
    months.forEach(mo=>{h+='<td class="num">'+m.fn(gnFilter(GNROWS,mo.from,mo.to))+'</td>';});
    h+='</tr>';
  });
  h+='</tbody></table>';
  document.getElementById('clinTrend').innerHTML=h;
}

function renderClinical(){
  document.getElementById('clinPeriodLabel').innerHTML=clinNavLabel();
  document.getElementById('clinViewMonth').className='view-btn'+(clinNavView==='month'?' active':'');
  document.getElementById('clinViewWeek').className='view-btn'+(clinNavView==='week'?' active':'');
  document.getElementById('clinViewDay').className='view-btn'+(clinNavView==='day'?' active':'');
  const{from,to}=clinNavRange();
  const rows=gnFilter(GNROWS,from,to);
  const totalMins=rows.reduce((s,r)=>s+r.mins,0);
  document.getElementById('clinKpiSessions').textContent=rows.length;
  document.getElementById('clinKpiHours').textContent=(totalMins/60).toFixed(1);
  document.getElementById('clinKpiActive').textContent=rows.filter(r=>r.status==='active').length;
  // By group title
  const titleMap={};rows.forEach(r=>{if(r.title)titleMap[r.title]=(titleMap[r.title]||0)+1;});
  let ht='<table class="break-table"><thead><tr><th>Group Title</th><th># Sessions</th><th>Total Hrs</th></tr></thead><tbody>';
  Object.entries(titleMap).sort((a,b)=>b[1]-a[1]).forEach(([k,v])=>{
    const hrs=(rows.filter(r=>r.title===k).reduce((s,r)=>s+r.mins,0)/60).toFixed(1);
    ht+='<tr><td>'+esc(k)+'</td><td class="num">'+v+'</td><td class="num">'+hrs+'</td></tr>';
  });
  ht+='<tr class="total-row"><td>Total</td><td class="num">'+rows.length+'</td><td class="num">'+( totalMins/60).toFixed(1)+'</td></tr></tbody></table>';
  document.getElementById('clinGroupTable').innerHTML=ht;
  // Session list
  let hlist='<div class="table-wrap"><table><thead><tr><th>Date</th><th>Group Title</th><th>Status</th><th>Duration (min)</th></tr></thead><tbody>';
  const sorted=rows.slice().sort((a,b)=>{const da=pd(a.date),db=pd(b.date);return db-da;});
  if(!sorted.length)hlist+='<tr><td colspan="4" class="no-data">No sessions.</td></tr>';
  sorted.forEach(r=>hlist+='<tr><td>'+esc(r.date)+'</td><td>'+esc(r.title)+'</td><td>'+esc(r.status)+'</td><td class="num">'+r.mins+'</td></tr>');
  hlist+='</tbody></table></div>';
  document.getElementById('clinSessionList').innerHTML=hlist;
}

/* ═══════════════════════════════════════════════════════════════
   OPERATIONS LOGIC
═══════════════════════════════════════════════════════════════ */
const OPROWS2 = JSON.parse(document.getElementById('opsData').textContent);

function renderOpsHeatmap(){
  const from=daysAgo(89);
  const rows=OPROWS2.filter(r=>{const d=pd(r.date);return d&&d>=from&&d<=TODAY&&r.hour>=0&&r.dow>=0;});
  const grid=Array(24).fill(null).map(()=>Array(7).fill(0));
  rows.forEach(r=>grid[r.hour][r.dow]++);
  const maxV=Math.max(1,...grid.flat());
  const days=['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  let h='<table class="heatmap-table"><thead><tr><th class="row-hdr">Hour</th>';
  days.forEach(d=>h+='<th>'+d+'</th>'); h+='<th>Total</th></tr></thead><tbody>';
  // Business hours first (6am-9pm), then rest
  const hours=Array.from({length:24},(_,i)=>(i+6)%24);
  hours.forEach(hr=>{
    const rowTot=grid[hr].reduce((s,v)=>s+v,0);
    const label=(hr===0?'12am':hr<12?hr+'am':hr===12?'12pm':(hr-12)+'pm');
    h+='<tr><td class="row-hdr" style="background:#eef3f9;padding:4px 8px;font-weight:600;text-align:right">'+label+'</td>';
    for(let d=0;d<7;d++){
      const v=grid[hr][d];
      const level=v===0?0:Math.min(6,Math.ceil(v/maxV*6));
      h+='<td class="hm-'+level+'" title="'+days[d]+' '+label+': '+v+' admits">'+(v||'')+'</td>';
    }
    h+='<td style="text-align:center;font-weight:700;background:#f5f8fc">'+(rowTot||'')+'</td></tr>';
  });
  // Totals row
  h+='<tr><td class="row-hdr" style="background:#1a3a5c;color:#fff;padding:5px 8px;font-weight:700">Total</td>';
  for(let d=0;d<7;d++){const tot=grid.reduce((s,row)=>s+row[d],0);h+='<td style="text-align:center;font-weight:700;background:#1a3a5c;color:#fff">'+tot+'</td>';}
  h+='<td style="text-align:center;font-weight:700;background:#0d2a44;color:#fff">'+rows.length+'</td></tr>';
  h+='</tbody></table>';
  document.getElementById('opsHeatmap').innerHTML=h;
}

let opsNavView='month',opsNavDate=null;
for(const r of OPROWS2){const d=pd(r.date);if(d){opsNavDate=d;break;}}
if(!opsNavDate)opsNavDate=new Date();

function opsNavRange(){
  if(opsNavView==='month')return{from:som(opsNavDate),to:eom(opsNavDate)};
  if(opsNavView==='week'){const ws=gwk(opsNavDate),we=new Date(ws);we.setDate(we.getDate()+6);return{from:ws,to:we};}
  return{from:new Date(opsNavDate),to:new Date(opsNavDate)};
}
function opsNavLabel(){
  if(opsNavView==='month')return opsNavDate.toLocaleString('default',{month:'long'})+' '+opsNavDate.getFullYear();
  if(opsNavView==='week'){const{from,to}=opsNavRange();return fd(from)+' \u2013 '+fd(to);}
  return fd(opsNavDate);
}
function opsNavigate(dir){
  if(opsNavView==='month')opsNavDate=new Date(opsNavDate.getFullYear(),opsNavDate.getMonth()+dir,1);
  else if(opsNavView==='week')opsNavDate=new Date(opsNavDate.getTime()+dir*7*86400000);
  else opsNavDate=new Date(opsNavDate.getTime()+dir*86400000);
  renderOpsDetail();
}
function opsSetView(v){opsNavView=v;renderOpsDetail();}
function opsJump(val){if(!val)return;const p=val.split('-');opsNavDate=new Date(+p[0],+p[1]-1,+p[2]);renderOpsDetail();}

function renderOpsDetail(){
  document.getElementById('opsPeriodLabel').innerHTML=opsNavLabel();
  document.getElementById('opsViewMonth').className='view-btn'+(opsNavView==='month'?' active':'');
  document.getElementById('opsViewWeek').className='view-btn'+(opsNavView==='week'?' active':'');
  document.getElementById('opsViewDay').className='view-btn'+(opsNavView==='day'?' active':'');
  const{from,to}=opsNavRange();
  const rows=OPROWS2.filter(r=>{const d=pd(r.date);return d&&d>=from&&d<=to;});
  document.getElementById('opsKpiAdmits').textContent=rows.length;
  const locs={}; rows.forEach(r=>{if(r.loc)locs[r.loc]=(locs[r.loc]||0)+1;});
  document.getElementById('opsKpiLOC').textContent=Object.entries(locs).sort((a,b)=>b[1]-a[1])[0]?.[0]||'-';
  document.getElementById('opsKpiTopRep').textContent=(()=>{const m={};rows.forEach(r=>{if(r.rep)m[r.rep]=(m[r.rep]||0)+1;});return Object.entries(m).sort((a,b)=>b[1]-a[1])[0]?.[0]||'-';})();

  function mkBreakTable(keyFn,hdr){
    const map={};rows.forEach(r=>{const k=keyFn(r);if(k)map[k]=(map[k]||0)+1;});
    let h='<table class="break-table"><thead><tr><th>'+hdr+'</th><th>Admits</th><th>%</th></tr></thead><tbody>';
    Object.entries(map).sort((a,b)=>b[1]-a[1]).forEach(([k,v])=>h+='<tr><td>'+esc(k)+'</td><td class="num">'+v+'</td><td class="num">'+(rows.length>0?(v/rows.length*100).toFixed(1)+'%':'0%')+'</td></tr>');
    h+='<tr class="total-row"><td>Total</td><td class="num">'+rows.length+'</td><td class="num">100%</td></tr></tbody></table>';
    return h;
  }
  document.getElementById('opsRepTable').innerHTML=mkBreakTable(r=>r.rep,'Admissions Rep');
  document.getElementById('opsTherapistTable').innerHTML=mkBreakTable(r=>r.therapist,'Assigned Therapist');
  document.getElementById('opsInsTable').innerHTML=mkBreakTable(r=>r.ins,'Insurance');
  document.getElementById('opsLocTable').innerHTML=mkBreakTable(r=>r.loc,'Level of Care');
}

function renderOpsMonthlyIns(){
  const months=[];for(let i=5;i>=0;i--){const d=new Date(TODAY.getFullYear(),TODAY.getMonth()-i,1);months.push({label:d.toLocaleString('default',{month:'short',year:'2-digit'}),from:d,to:eom(d)});}
  const insSet=new Set(OPROWS2.map(r=>r.ins).filter(Boolean));
  const topIns=Object.entries((() => {const m={};OPROWS2.forEach(r=>{if(r.ins)m[r.ins]=(m[r.ins]||0)+1;});return m;})()).sort((a,b)=>b[1]-a[1]).slice(0,8).map(([k])=>k);
  let h='<table class="trend-table"><thead><tr><th>Insurance</th>';
  months.forEach(m=>h+='<th>'+m.label+'</th>'); h+='<th>Total</th></tr></thead><tbody>';
  topIns.forEach(ins=>{
    h+='<tr><td class="metric-name">'+esc(ins)+'</td>';
    let tot=0;
    months.forEach(mo=>{const rs=OPROWS2.filter(r=>{const d=pd(r.date);return d&&d>=mo.from&&d<=mo.to&&r.ins===ins;});h+='<td class="num">'+rs.length+'</td>';tot+=rs.length;});
    h+='<td class="num" style="font-weight:700">'+tot+'</td></tr>';
  });
  // Others row
  h+='<tr><td class="metric-name"><em>Others</em></td>';
  let tot=0;
  months.forEach(mo=>{const rs=OPROWS2.filter(r=>{const d=pd(r.date);return d&&d>=mo.from&&d<=mo.to&&!topIns.includes(r.ins);});h+='<td class="num">'+rs.length+'</td>';tot+=rs.length;});
  h+='<td class="num" style="font-weight:700">'+tot+'</td></tr>';
  // Total row
  h+='<tr class="total-row"><td>Total</td>';
  let grandTot=0;
  months.forEach(mo=>{const rs=OPROWS2.filter(r=>{const d=pd(r.date);return d&&d>=mo.from&&d<=mo.to;});h+='<td class="num">'+rs.length+'</td>';grandTot+=rs.length;});
  h+='<td class="num">'+grandTot+'</td></tr></tbody></table>';
  document.getElementById('opsMonthlyIns').innerHTML=h;
}

/* ═══════════════════════════════════════════════════════════════
   FIELD EXPLORER LOGIC
═══════════════════════════════════════════════════════════════ */
function renderFieldExplorer(){
  const sorted=CROWS.slice().sort((a,b)=>{const da=pd(a.adm),db=pd(b.adm);return db-da;});
  const admits=sorted.filter(r=>!r.dis).slice(0,5);
  const discharges=sorted.filter(r=>r.dis).slice(0,5);
  const fields=[
    {label:'Patient Name',fn:r=>r.name},
    {label:'Admission Date',fn:r=>r.adm},
    {label:'Discharge Date',fn:r=>r.dis||'\u2014'},
    {label:'Admission LOC',fn:r=>r.loc},
    {label:'Current LOC',fn:r=>r.cloc},
    {label:'Gender',fn:r=>r.gen},
    {label:'Age',fn:r=>r.age!=null?r.age:''},
    {label:'Primary Drug',fn:r=>r.drug},
    {label:'Referral Source',fn:r=>r.ref},
    {label:'Discharge Type',fn:r=>r.dtype||'\u2014'},
    {label:'Length of Stay',fn:r=>r.los!=null?r.los+' days':'\u2014'},
  ];
  const mkHeader=(recs,type)=>{
    let h='<th class="fe-section-hdr">'+type+'</th>';
    recs.forEach((r,i)=>h+='<th>'+esc(type==='Active Admits'?r.adm:r.dis)+' \u2014 '+esc(r.name.split(' ').slice(-1)[0])+'</th>');
    while(recs.length<5)h+='<th style="opacity:.3">(empty)</th>',recs.push(null);
    return h;
  };
  let h='<div style="overflow-x:auto"><table style="border-collapse:collapse;font-size:12px;width:100%"><thead>';
  h+='<tr><th style="background:#eef3f9;color:#1a3a5c;padding:8px 12px;text-align:left;position:sticky;left:0;z-index:3;min-width:160px">Field</th>';
  const a5=admits.slice(),d5=discharges.slice();
  while(a5.length<5)a5.push(null);while(d5.length<5)d5.push(null);
  a5.forEach((r,i)=>h+='<th style="background:#1a6ec0;color:#fff;padding:8px 10px;min-width:130px;text-align:center">'+(r?'Admit '+( i+1)+' \u2014 '+esc(r.adm):'&mdash;')+'</th>');
  d5.forEach((r,i)=>h+='<th style="background:#217346;color:#fff;padding:8px 10px;min-width:130px;text-align:center">'+(r?'Disch '+( i+1)+' \u2014 '+esc(r.dis):'&mdash;')+'</th>');
  h+='</tr></thead><tbody>';
  fields.forEach((f,ri)=>{
    const bg=ri%2===0?'#fafbfd':'#fff';
    h+='<tr><td style="background:#eef3f9;font-weight:700;padding:7px 12px;position:sticky;left:0;z-index:2;white-space:nowrap">'+f.label+'</td>';
    a5.forEach(r=>h+='<td style="background:'+bg+';padding:6px 10px;text-align:center;border-bottom:1px solid #eee">'+(r?esc(String(f.fn(r))):'')+'</td>');
    d5.forEach(r=>h+='<td style="background:'+bg+';padding:6px 10px;text-align:center;border-bottom:1px solid #eee">'+(r?esc(String(f.fn(r))):'')+'</td>');
    h+='</tr>';
  });
  h+='</tbody></table></div>';
  document.getElementById('feTable').innerHTML=h;
}

// ── Initial renders ────────────────────────────────────────────────────────
// Initial renders
renderBillingSpot();
renderBillingBreakdowns();
renderBillingTrend();
renderBillingDetail();
renderCensusSpot();
renderCensusTrend();
renderCensusBreakdowns();
renderMarketingSpot();
renderMarketingTrend();
renderMarketingDetail();
renderOpportunities();
renderURSpot();
renderURTrend();
renderClinicalSpot();
renderClinical();
renderOpsHeatmap();
renderOpsDetail();
renderOpsMonthlyIns();
renderFieldExplorer();
// Show billing by default
showPage('billing');
"""

# ── HTML ─────────────────────────────────────────────────────────────────────
CENSUS_SECTION = """
<div class="page-section" id="sec-census" style="display:none">
  <div class="main">

    <h2 class="section-title">SPOT / MOST RECENT &mdash; Active Census, Admits &amp; Discharges by Period</h2>
    <div class="spot-wrap" id="cSpotTable"></div>

    <h2 class="section-title" style="margin-top:18px">DISCHARGE TYPE BREAKDOWN (% of in-period discharges)</h2>
    <div class="spot-wrap" id="cDischargeBreakdown"></div>

    <h2 class="section-title" style="margin-top:18px">MONTHLY TREND &mdash; Last 6 Months</h2>
    <div class="trend-wrap"><div id="cTrendTable"></div></div>

    <h2 class="section-title">PERIOD DETAIL &mdash; Breakdowns by Selected Period</h2>
    <div class="controls">
      <div class="view-btns">
        <button id="cViewMonth" class="view-btn active" onclick="cSetView('month')">Month</button>
        <button id="cViewWeek"  class="view-btn"        onclick="cSetView('week')">Week</button>
        <button id="cViewDay"   class="view-btn"        onclick="cSetView('day')">Day</button>
      </div>
      <div class="nav-btns">
        <button class="period-nav-btn" onclick="cNavigate(-1)">&#8249;</button>
        <span class="period-label" id="cPeriodLabel"></span>
        <button class="period-nav-btn" onclick="cNavigate(1)">&#8250;</button>
      </div>
      <input type="date" class="date-input" title="Jump to date" onchange="cJump(this.value)">
    </div>

    <div class="stats-bar">
      <div class="stat-card"><div class="val" id="cKpiActive">-</div><div class="lbl">Active Census</div></div>
      <div class="stat-card green"><div class="val" id="cKpiAdmits">-</div><div class="lbl">Admits</div></div>
      <div class="stat-card orange"><div class="val" id="cKpiDischarges">-</div><div class="lbl">Discharges</div></div>
      <div class="stat-card purple"><div class="val" id="cKpiNet">-</div><div class="lbl">Net Growth</div></div>
      <div class="stat-card"><div class="val" id="cKpiLOS">-</div><div class="lbl">Avg LOS</div></div>
    </div>

    <div class="break-grid">
      <div class="break-card"><h3>ADMITS BY GENDER</h3><div id="cGenderTable"></div></div>
      <div class="break-card"><h3>ADMITS BY AGE BUCKET</h3><div id="cAgeTable"></div></div>
    </div>
    <div class="break-grid">
      <div class="break-card"><h3>PRIMARY DRUG OF CHOICE &mdash; % of Admits</h3><div id="cDrugTable"></div></div>
      <div class="break-card"><h3>DISCHARGE TYPE BREAKDOWN</h3><div id="cDtypeTable"></div></div>
    </div>
    <div class="break-grid">
      <div class="break-card"><h3>REFERRAL SOURCES &mdash; Top 10 + Others</h3><div id="cReferralTable"></div></div>
      <div class="break-card"><h3>AVG LENGTH OF STAY (all-time cohort)</h3><div id="cLOSTable"></div></div>
    </div>

  </div>
</div>
"""

BILLING_SECTION = """
<div class="page-section" id="sec-billing" style="display:none">
  <div class="main">

    <h2 class="section-title">SPOT / MOST RECENT &mdash; Revenue &amp; Collections by Deposit Date</h2>
    <div class="spot-wrap" id="bSpotTable"></div>

    <h2 class="section-title" style="margin-top:18px">MONTHLY TREND &mdash; Last 6 Months</h2>
    <div class="trend-wrap"><div id="bTrendTable"></div></div>

    <h2 class="section-title">DETAIL VIEW &mdash; Selected Period</h2>
    <div class="controls">
      <div class="view-btns">
        <button id="bViewMonth" class="view-btn active" onclick="bSetView('month')">Month</button>
        <button id="bViewWeek"  class="view-btn"        onclick="bSetView('week')">Week</button>
        <button id="bViewDay"   class="view-btn"        onclick="bSetView('day')">Day</button>
      </div>
      <div class="nav-btns">
        <button class="period-nav-btn" onclick="bNavigate(-1)">&#8249;</button>
        <span class="period-label" id="bPeriodLabel"></span>
        <button class="period-nav-btn" onclick="bNavigate(1)">&#8250;</button>
      </div>
      <input type="date" class="date-input" title="Jump to date" onchange="bJump(this.value)">
      <input type="text" class="search-box" placeholder="Search records..." oninput="bNavSearch=this.value;renderBillingDetail()">
      <button class="export-btn" onclick="bExport()">&#8595; Export CSV</button>
    </div>

    <div class="stats-bar">
      <div class="stat-card"><div class="val" id="bKpiLines">-</div><div class="lbl"># Payment Lines</div></div>
      <div class="stat-card"><div class="val" id="bKpiCharged">-</div><div class="lbl">Charged $</div></div>
      <div class="stat-card green"><div class="val" id="bKpiPaid">-</div><div class="lbl">Paid $</div></div>
      <div class="stat-card orange"><div class="val" id="bKpiCR">-</div><div class="lbl">Collection Rate</div></div>
      <div class="stat-card orange"><div class="val" id="bKpiNR">-</div><div class="lbl">Net Realization</div></div>
      <div class="stat-card"><div class="val" id="bKpiAvg">-</div><div class="lbl">Avg $ / Line</div></div>
    </div>

    <div class="break-grid">
      <div class="break-card"><h3>PAID $ BY PAYER</h3><div id="bPayerTable"></div></div>
      <div class="break-card"><h3>PAID $ BY LEVEL OF CARE</h3><div id="bLocTable"></div></div>
    </div>
    <div class="break-grid" style="grid-template-columns:1fr">
      <div class="break-card"><h3>ADJUSTMENTS &amp; DENIALS $</h3><div id="bAdjTable"></div></div>
    </div>

    <h2 class="section-title">TRANSACTION DETAIL</h2>
    <div id="bDetailTable"></div>

  </div>
</div>
"""

MARKETING_SECTION = """
<div class="page-section" id="sec-marketing" style="display:none">
  <div class="main">
    <h2 class="section-title">SPOT / MOST RECENT &mdash; Opportunity Funnel by Period</h2>
    <div class="spot-wrap" id="mktSpot"></div>
    <h2 class="section-title" style="margin-top:18px">MONTHLY TREND &mdash; Last 6 Months</h2>
    <div class="trend-wrap"><div id="mktTrend"></div></div>
    <h2 class="section-title">PERIOD DETAIL &mdash; Funnel &amp; Breakdowns</h2>
    <div class="controls">
      <div class="view-btns">
        <button id="mktViewMonth" class="view-btn active" onclick="mktSetView('month')">Month</button>
        <button id="mktViewWeek"  class="view-btn"        onclick="mktSetView('week')">Week</button>
        <button id="mktViewDay"   class="view-btn"        onclick="mktSetView('day')">Day</button>
      </div>
      <div class="nav-btns">
        <button class="period-nav-btn" onclick="mktNavigate(-1)">&#8249;</button>
        <span class="period-label" id="mktPeriodLabel"></span>
        <button class="period-nav-btn" onclick="mktNavigate(1)">&#8250;</button>
      </div>
      <input type="date" class="date-input" title="Jump to date" onchange="mktJump(this.value)">
    </div>
    <div class="stats-bar">
      <div class="stat-card"><div class="val" id="mktKpiTotal">-</div><div class="lbl">Total Created</div></div>
      <div class="stat-card green"><div class="val" id="mktKpiAdmitted">-</div><div class="lbl">Admitted</div></div>
      <div class="stat-card orange"><div class="val" id="mktKpiRate">-</div><div class="lbl">Admit Rate</div></div>
      <div class="stat-card"><div class="val" id="mktKpiActive">-</div><div class="lbl">Active</div></div>
      <div class="stat-card purple"><div class="val" id="mktKpiLost">-</div><div class="lbl">Lost</div></div>
      <div class="stat-card purple"><div class="val" id="mktKpiAbandoned">-</div><div class="lbl">Abandoned</div></div>
    </div>
    <div class="break-grid" style="grid-template-columns:1fr 1fr">
      <div class="break-card"><h3>CONVERSION FUNNEL</h3><div id="mktFunnel"></div></div>
      <div class="break-card"><h3>TOP REFERRAL SOURCES (Admitted)</h3><div id="mktRefTable"></div></div>
    </div>
    <div class="break-grid">
      <div class="break-card"><h3>LOST REASONS</h3><div id="mktLostTable"></div></div>
      <div class="break-card"><h3>ABANDONED REASONS</h3><div id="mktAbandTable"></div></div>
    </div>
  </div>
</div>
"""

OPPORTUNITIES_SECTION = """
<div class="page-section" id="sec-opportunities" style="display:none">
  <div class="main">
    <h2 class="section-title">OPPORTUNITIES DETAIL &mdash; Period View</h2>
    <div class="controls">
      <div class="view-btns">
        <button id="oppViewMonth" class="view-btn active" onclick="oppSetView('month')">Month</button>
        <button id="oppViewWeek"  class="view-btn"        onclick="oppSetView('week')">Week</button>
        <button id="oppViewDay"   class="view-btn"        onclick="oppSetView('day')">Day</button>
      </div>
      <div class="nav-btns">
        <button class="period-nav-btn" onclick="oppNavigate(-1)">&#8249;</button>
        <span class="period-label" id="oppPeriodLabel"></span>
        <button class="period-nav-btn" onclick="oppNavigate(1)">&#8250;</button>
      </div>
      <input type="date" class="date-input" title="Jump to date" onchange="oppJump(this.value)">
      <input type="text" class="search-box" placeholder="Search opportunities..." oninput="oppSearch=this.value;renderOpportunities()">
    </div>
    <div class="stats-bar">
      <div class="stat-card"><div class="val" id="oppKpiTotal">-</div><div class="lbl">Total</div></div>
      <div class="stat-card green"><div class="val" id="oppKpiAdmitted">-</div><div class="lbl">Admitted</div></div>
      <div class="stat-card orange"><div class="val" id="oppKpiRate">-</div><div class="lbl">Admit Rate</div></div>
      <div class="stat-card"><div class="val" id="oppKpiActive">-</div><div class="lbl">Active</div></div>
    </div>
    <div class="break-grid">
      <div class="break-card"><h3>BY OUTCOME</h3><div id="oppOutcomeTable"></div></div>
      <div class="break-card"><h3>BY INSURANCE (Top 10)</h3><div id="oppInsTable"></div></div>
    </div>
    <h2 class="section-title">OPPORTUNITY LIST</h2>
    <div id="oppTableWrap"></div>
  </div>
</div>
"""

UR_SECTION = """
<div class="page-section" id="sec-ur" style="display:none">
  <div class="main">
    <h2 class="section-title">SPOT / MOST RECENT &mdash; Auth Metrics by Period</h2>
    <div class="spot-wrap" id="urSpot"></div>
    <div class="break-grid" style="margin-top:14px">
      <div class="break-card"><h3>UPCOMING REVIEW DATES</h3><div id="urUpcoming"></div></div>
      <div class="break-card"><h3>BY INSURANCE (YTD)</h3><div id="urInsTable"></div></div>
    </div>
    <h2 class="section-title" style="margin-top:18px">MONTHLY TREND &mdash; Last 6 Months</h2>
    <div class="trend-wrap"><div id="urTrend"></div></div>
    <div class="break-grid" style="grid-template-columns:1fr">
      <div class="break-card"><h3>BY UR REVIEWER (YTD)</h3><div id="urRevTable"></div></div>
    </div>
  </div>
</div>
"""

CLINICAL_SECTION = """
<div class="page-section" id="sec-clinical" style="display:none">
  <div class="main">
    <h2 class="section-title">GROUP NOTES TREND &mdash; Last 6 Months</h2>
    <div class="trend-wrap"><div id="clinTrend"></div></div>
    <h2 class="section-title">PERIOD DETAIL</h2>
    <div class="controls">
      <div class="view-btns">
        <button id="clinViewMonth" class="view-btn active" onclick="clinSetView('month')">Month</button>
        <button id="clinViewWeek"  class="view-btn"        onclick="clinSetView('week')">Week</button>
        <button id="clinViewDay"   class="view-btn"        onclick="clinSetView('day')">Day</button>
      </div>
      <div class="nav-btns">
        <button class="period-nav-btn" onclick="clinNavigate(-1)">&#8249;</button>
        <span class="period-label" id="clinPeriodLabel"></span>
        <button class="period-nav-btn" onclick="clinNavigate(1)">&#8250;</button>
      </div>
      <input type="date" class="date-input" title="Jump to date" onchange="clinJump(this.value)">
    </div>
    <div class="stats-bar">
      <div class="stat-card"><div class="val" id="clinKpiSessions">-</div><div class="lbl"># Sessions</div></div>
      <div class="stat-card green"><div class="val" id="clinKpiHours">-</div><div class="lbl">Total Hours</div></div>
      <div class="stat-card orange"><div class="val" id="clinKpiActive">-</div><div class="lbl">Active Sessions</div></div>
    </div>
    <div class="break-grid" style="grid-template-columns:1fr">
      <div class="break-card"><h3>BY GROUP TITLE</h3><div id="clinGroupTable"></div></div>
    </div>
    <h2 class="section-title">SESSION LIST</h2>
    <div id="clinSessionList"></div>
  </div>
</div>
"""

OPERATIONS_SECTION = """
<div class="page-section" id="sec-operations" style="display:none">
  <div class="main">
    <h2 class="section-title">ADMISSION HEATMAP &mdash; Hour &times; Day of Week (Last 90 Days)</h2>
    <div class="heatmap-wrap"><div id="opsHeatmap"></div></div>
    <h2 class="section-title">MONTHLY INSURANCE MIX &mdash; Top 8 Insurers</h2>
    <div class="trend-wrap"><div id="opsMonthlyIns"></div></div>
    <h2 class="section-title">PERIOD DETAIL</h2>
    <div class="controls">
      <div class="view-btns">
        <button id="opsViewMonth" class="view-btn active" onclick="opsSetView('month')">Month</button>
        <button id="opsViewWeek"  class="view-btn"        onclick="opsSetView('week')">Week</button>
        <button id="opsViewDay"   class="view-btn"        onclick="opsSetView('day')">Day</button>
      </div>
      <div class="nav-btns">
        <button class="period-nav-btn" onclick="opsNavigate(-1)">&#8249;</button>
        <span class="period-label" id="opsPeriodLabel"></span>
        <button class="period-nav-btn" onclick="opsNavigate(1)">&#8250;</button>
      </div>
      <input type="date" class="date-input" title="Jump to date" onchange="opsJump(this.value)">
    </div>
    <div class="stats-bar">
      <div class="stat-card"><div class="val" id="opsKpiAdmits">-</div><div class="lbl">Admits</div></div>
      <div class="stat-card green"><div class="val" id="opsKpiLOC">-</div><div class="lbl">Top LOC</div></div>
      <div class="stat-card orange"><div class="val" id="opsKpiTopRep">-</div><div class="lbl">Top Admissions Rep</div></div>
    </div>
    <div class="break-grid">
      <div class="break-card"><h3>BY ADMISSIONS REP</h3><div id="opsRepTable"></div></div>
      <div class="break-card"><h3>BY ASSIGNED THERAPIST</h3><div id="opsTherapistTable"></div></div>
    </div>
    <div class="break-grid">
      <div class="break-card"><h3>BY INSURANCE</h3><div id="opsInsTable"></div></div>
      <div class="break-card"><h3>BY LEVEL OF CARE</h3><div id="opsLocTable"></div></div>
    </div>
  </div>
</div>
"""

FIELD_EXPLORER_SECTION = """
<div class="page-section" id="sec-fieldexplorer" style="display:none">
  <div class="main">
    <h2 class="section-title">FIELD EXPLORER &mdash; 5 Most Recent Admits &amp; Discharges</h2>
    <div class="trend-wrap"><div id="feTable"></div></div>
  </div>
</div>
"""

html = (
    '<!DOCTYPE html>\n<html lang="en">\n<head>\n'
    '<meta charset="UTF-8">\n'
    '<meta name="viewport" content="width=device-width,initial-scale=1">\n'
    '<title>Sunwave Dashboard</title>\n'
    '<style>' + CSS + '</style>\n'
    '</head>\n<body>\n'
    '<div id="app">\n'

    # Sidebar
    '  <div id="sidebar">\n'
    '    <div class="sb-header"><h1>Sunwave Dashboard</h1><p>Provident Healthcare Management</p></div>\n'
    '    <button class="refresh-btn" onclick="doRefresh()">&#8635;&nbsp; Refresh</button>\n'
    '    <div class="sb-nav" id="sidebarNav"></div>\n'
    '  </div>\n'

    # Content
    '  <div id="content">\n'
    '    <div class="page-header">\n'
    '      <div><h2 id="pageTitle">AR / Billing Dashboard</h2>\n'
    '           <small id="pageSub">Payment Report Deposit Date &mdash; MASTER_Sunwave_New_PowerQuerry.xlsx</small></div>\n'
    '    </div>\n'
    '    <div id="sectionsWrap">\n'
    + BILLING_SECTION
    + CENSUS_SECTION
    + MARKETING_SECTION
    + OPPORTUNITIES_SECTION
    + UR_SECTION
    + CLINICAL_SECTION
    + OPERATIONS_SECTION
    + FIELD_EXPLORER_SECTION +
    '    </div>\n'
    '  </div>\n'
    '</div>\n'

    # Data
    '<script type="application/json" id="generalData">' + general_js + '</script>\n'
    '<script type="application/json" id="dateIdx">'     + config_js  + '</script>\n'
    '<script type="application/json" id="billingData">' + billing_js + '</script>\n'
    '<script type="application/json" id="censusData">'  + census_js  + '</script>\n'
    '<script type="application/json" id="oppData">'     + opp_js     + '</script>\n'
    '<script type="application/json" id="authData">'    + auth_js    + '</script>\n'
    '<script type="application/json" id="opsData">'     + ops_js     + '</script>\n'
    '<script type="application/json" id="gnData">'      + gnotes_js  + '</script>\n'
    '<script>' + JS + '</script>\n'
    '</body>\n</html>'
)

out = 'Sunwave_Dashboard.html'
with open(out, 'w', encoding='utf-8') as f:
    f.write(html)
print(f"Done: {os.path.getsize(out)/1024/1024:.1f} MB")
