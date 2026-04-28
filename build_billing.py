import pandas as pd, json, math, os

df = pd.read_excel('MASTER_Sunwave_New_PowerQuerry.xlsx', sheet_name='Payment Report Deposit Date')

# Parse dates
df['deposit_date'] = pd.to_datetime(df['deposit_date'], errors='coerce')

# Clean numerics
num_cols = ['line_charge_amount','line_paid_amount','line_adjusted','line_allocated_amount','line_allowed']
for c in num_cols:
    df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

# Build row records for JS
rows = []
for _, r in df.iterrows():
    dep = r['deposit_date']
    rows.append({
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

data_json = json.dumps(rows, separators=(',', ':'), ensure_ascii=True)
data_json = data_json.replace('</', '<\\/')

JS = r"""
const ROWS = JSON.parse(document.getElementById('billingData').textContent);

const TODAY = new Date(); TODAY.setHours(0,0,0,0);
function daysAgo(n) { const d = new Date(TODAY); d.setDate(d.getDate()-n); return d; }
function startOfYear() { return new Date(TODAY.getFullYear(),0,1); }
function startOfMonth(d) { return new Date(d.getFullYear(), d.getMonth(), 1); }
function getWeekStart(d) { const c=new Date(d); c.setDate(c.getDate()-c.getDay()); c.setHours(0,0,0,0); return c; }

function parseDate(str) {
  if (!str) return null;
  const m = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (m) { const d = new Date(+m[3],+m[1]-1,+m[2]); d.setHours(0,0,0,0); return d; }
  return null;
}
function fmtDate(d) {
  if (!d) return '';
  return String(d.getMonth()+1).padStart(2,'0')+'/'+String(d.getDate()).padStart(2,'0')+'/'+d.getFullYear();
}
function fmtMoney(v) {
  if (v === null || v === undefined || isNaN(v)) return '$0';
  return '$' + Math.round(v).toLocaleString();
}
function fmtPct(v) {
  if (!v || isNaN(v) || !isFinite(v)) return '0.0%';
  return (v*100).toFixed(1) + '%';
}
function fmtAvg(v) {
  if (!v || isNaN(v) || !isFinite(v)) return '$0';
  return '$' + v.toFixed(2);
}

function filterRows(rows, from, to) {
  return rows.filter(r => {
    const d = parseDate(r.deposit_date);
    if (!d) return false;
    return d >= from && d <= to;
  });
}

function calcMetrics(rows) {
  const paid_rows = rows.filter(r => r.adjustment_type === 'Allowed');
  const paid       = paid_rows.reduce((s,r) => s + r.line_paid_amount, 0);
  const charged    = rows.reduce((s,r) => s + r.line_charge_amount, 0);
  const allowed    = paid_rows.reduce((s,r) => s + r.line_allocated_amount, 0);
  const contractual= rows.filter(r => r.adjustment_type === 'Contractual')
                         .reduce((s,r) => s + r.line_adjusted, 0);
  const writeoff   = rows.filter(r => r.adjustment_type === 'Write Off' || r.adjustment_type === 'Administrative Write Off')
                         .reduce((s,r) => s + r.line_adjusted, 0);
  const noncovered = rows.filter(r => r.adjustment_type === 'Non Covered Service')
                         .reduce((s,r) => s + r.line_charge_amount, 0);
  const lines      = paid_rows.length;
  return { paid, charged, allowed, contractual, writeoff, noncovered, lines,
    collection_rate: allowed > 0 ? paid/allowed : 0,
    net_realization: charged > 0 ? paid/charged : 0,
    avg_per_line: lines > 0 ? paid/lines : 0 };
}

const PERIODS = [
  { label:'Today',       from: TODAY,       to: TODAY },
  { label:'Yesterday',   from: daysAgo(1),  to: daysAgo(1) },
  { label:'Last 7 Days', from: daysAgo(6),  to: TODAY },
  { label:'Last 31 Days',from: daysAgo(30), to: TODAY },
  { label:'Last 90 Days',from: daysAgo(89), to: TODAY },
  { label:'Last 12 Mo',  from: daysAgo(364),to: TODAY },
  { label:'YTD',         from: startOfYear(),to: TODAY },
];

const METRICS = [
  { key:'lines',          label:'# Payment Lines',                   fmt: v => v.toLocaleString() },
  { key:'charged',        label:'Charged $',                         fmt: fmtMoney },
  { key:'allowed',        label:'Allowed $ (contractual)',            fmt: fmtMoney },
  { key:'paid',           label:'Paid $',                            fmt: fmtMoney },
  { key:'contractual',    label:'Contractual Adjustments $',          fmt: fmtMoney },
  { key:'writeoff',       label:'Write-Offs $',                      fmt: fmtMoney },
  { key:'noncovered',     label:'Non-Covered / Denied $',            fmt: fmtMoney },
  { key:'collection_rate',label:'Collection Rate (Paid \u00f7 Allowed)', fmt: fmtPct },
  { key:'net_realization',label:'Net Realization (Paid \u00f7 Charged)', fmt: fmtPct },
  { key:'avg_per_line',   label:'Avg $ / Line',                      fmt: fmtAvg },
];

// --- Period nav state ---
let navView = 'month';
let navDate = null;
for (const r of ROWS) { const d = parseDate(r.deposit_date); if (d) { navDate = d; break; } }
if (!navDate) navDate = new Date();
let navSearch = '';
let navSortCol = 'deposit_date', navSortAsc = false;

function getNavRange() {
  if (navView === 'month') return { from: startOfMonth(navDate), to: new Date(navDate.getFullYear(), navDate.getMonth()+1, 0) };
  if (navView === 'week') { const ws = getWeekStart(navDate); const we = new Date(ws); we.setDate(we.getDate()+6); return { from:ws, to:we }; }
  const d = new Date(navDate); return { from:d, to:d };
}

function navLabel() {
  if (navView === 'month') return navDate.toLocaleString('default',{month:'long'}) + ' ' + navDate.getFullYear();
  if (navView === 'week') { const {from,to} = getNavRange(); return fmtDate(from) + ' \u2013 ' + fmtDate(to); }
  return fmtDate(navDate);
}

function navigatePeriod(dir) {
  if (navView === 'month') navDate = new Date(navDate.getFullYear(), navDate.getMonth()+dir, 1);
  else if (navView === 'week') navDate = new Date(navDate.getTime() + dir*7*86400000);
  else navDate = new Date(navDate.getTime() + dir*86400000);
  renderAll();
}

function setNavView(v) { navView = v; renderAll(); }
function jumpNav(val) { if (!val) return; const p=val.split('-'); navDate=new Date(+p[0],+p[1]-1,+p[2]); renderAll(); }

function groupBy(rows, key, valFn) {
  const map = {};
  rows.forEach(r => {
    const k = r[key] || '(blank)';
    if (!map[k]) map[k] = 0;
    map[k] += valFn(r);
  });
  return Object.entries(map).sort((a,b)=>b[1]-a[1]);
}

function esc(s) {
  return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function renderSpotTable() {
  let html = '<table class="spot-table"><thead><tr><th>Metric</th>';
  PERIODS.forEach(p => { html += '<th>' + p.label + '</th>'; });
  html += '</tr></thead><tbody>';
  METRICS.forEach(m => {
    html += '<tr><td class="metric-name">' + m.label + '</td>';
    PERIODS.forEach(p => {
      const rows = filterRows(ROWS, p.from, p.to);
      const calc = calcMetrics(rows);
      html += '<td class="num">' + m.fmt(calc[m.key]) + '</td>';
    });
    html += '</tr>';
  });
  html += '</tbody></table>';
  document.getElementById('spotTable').innerHTML = html;
}

function renderBreakdowns() {
  const { from, to } = getNavRange();
  const rows = filterRows(ROWS, from, to);
  const paidRows = rows.filter(r => r.adjustment_type === 'Allowed');

  // Paid by Payer
  const byPayer = groupBy(paidRows, 'payer_name', r => r.line_paid_amount);
  let h1 = '<table class="break-table"><thead><tr><th>Payer</th><th>Paid $</th><th>Lines</th><th>% of Total</th></tr></thead><tbody>';
  const totalPaid = byPayer.reduce((s,[,v])=>s+v,0);
  const topPayers = byPayer.slice(0,9);
  const otherPaid = byPayer.slice(9).reduce((s,[,v])=>s+v,0);
  const otherLines = paidRows.filter(r=>!topPayers.find(([k])=>k===r.payer_name)).length;
  topPayers.forEach(([k,v]) => {
    const lines = paidRows.filter(r=>r.payer_name===k).length;
    const pct = totalPaid>0 ? (v/totalPaid*100).toFixed(1)+'%' : '0%';
    h1 += '<tr><td>'+esc(k)+'</td><td class="num">'+fmtMoney(v)+'</td><td class="num">'+lines+'</td><td class="num">'+pct+'</td></tr>';
  });
  if (otherPaid > 0) h1 += '<tr><td><em>Others</em></td><td class="num">'+fmtMoney(otherPaid)+'</td><td class="num">'+otherLines+'</td><td class="num">'+(totalPaid>0?(otherPaid/totalPaid*100).toFixed(1)+'%':'0%')+'</td></tr>';
  h1 += '<tr class="total-row"><td>Total</td><td class="num">'+fmtMoney(totalPaid)+'</td><td class="num">'+paidRows.length+'</td><td class="num">100%</td></tr>';
  h1 += '</tbody></table>';
  document.getElementById('payerTable').innerHTML = h1;

  // Paid by Level of Care
  const byLoc = groupBy(paidRows, 'level_of_care', r => r.line_paid_amount);
  let h2 = '<table class="break-table"><thead><tr><th>Level of Care</th><th>Paid $</th><th>Lines</th><th>% of Total</th></tr></thead><tbody>';
  byLoc.forEach(([k,v]) => {
    const lines = paidRows.filter(r=>r.level_of_care===k).length;
    const pct = totalPaid>0 ? (v/totalPaid*100).toFixed(1)+'%' : '0%';
    h2 += '<tr><td>'+esc(k)+'</td><td class="num">'+fmtMoney(v)+'</td><td class="num">'+lines+'</td><td class="num">'+pct+'</td></tr>';
  });
  h2 += '<tr class="total-row"><td>Total</td><td class="num">'+fmtMoney(totalPaid)+'</td><td class="num">'+paidRows.length+'</td><td class="num">100%</td></tr>';
  h2 += '</tbody></table>';
  document.getElementById('locTable').innerHTML = h2;

  // Adjustments & Denials
  const adjTypes = ['Contractual','Non Covered Service','Write Off','Administrative Write Off'];
  let h3 = '<table class="break-table"><thead><tr><th>Adjustment Type</th><th>Amount $</th><th>Lines</th><th>% of Charged</th></tr></thead><tbody>';
  const totalCharged = rows.reduce((s,r)=>s+r.line_charge_amount,0);
  let adjTotal = 0;
  adjTypes.forEach(at => {
    const adjRows = rows.filter(r=>r.adjustment_type===at);
    const amt = adjRows.reduce((s,r)=>s+(at==='Non Covered Service'?r.line_charge_amount:r.line_adjusted),0);
    adjTotal += amt;
    const pct = totalCharged>0 ? (amt/totalCharged*100).toFixed(1)+'%' : '0%';
    h3 += '<tr><td>'+esc(at)+'</td><td class="num">'+fmtMoney(amt)+'</td><td class="num">'+adjRows.length+'</td><td class="num">'+pct+'</td></tr>';
  });
  h3 += '<tr class="total-row"><td>Total Written Off</td><td class="num">'+fmtMoney(adjTotal)+'</td><td class="num">'+rows.filter(r=>r.adjustment_type!=='Allowed').length+'</td><td class="num">'+(totalCharged>0?(adjTotal/totalCharged*100).toFixed(1)+'%':'0%')+'</td></tr>';
  h3 += '</tbody></table>';
  document.getElementById('adjTable').innerHTML = h3;
}

function renderMonthlyTrend() {
  // Last 6 calendar months
  const months = [];
  for (let i = 5; i >= 0; i--) {
    const d = new Date(TODAY.getFullYear(), TODAY.getMonth()-i, 1);
    const from = d;
    const to = new Date(d.getFullYear(), d.getMonth()+1, 0);
    months.push({ label: d.toLocaleString('default',{month:'short',year:'2-digit'}), from, to });
  }
  const paidRows_all = ROWS.filter(r => r.adjustment_type === 'Allowed');
  let html = '<table class="trend-table"><thead><tr><th>Metric</th>';
  months.forEach(m => { html += '<th>'+m.label+'</th>'; });
  html += '</tr></thead><tbody>';
  const trendMetrics = [
    { key:'paid', label:'Paid $', fn: rs => fmtMoney(rs.filter(r=>r.adjustment_type==='Allowed').reduce((s,r)=>s+r.line_paid_amount,0)) },
    { key:'charged', label:'Charged $', fn: rs => fmtMoney(rs.reduce((s,r)=>s+r.line_charge_amount,0)) },
    { key:'lines', label:'# Payment Lines', fn: rs => rs.filter(r=>r.adjustment_type==='Allowed').length.toLocaleString() },
    { key:'contractual', label:'Contractual Adj $', fn: rs => fmtMoney(rs.filter(r=>r.adjustment_type==='Contractual').reduce((s,r)=>s+r.line_adjusted,0)) },
    { key:'noncovered', label:'Non-Covered / Denied $', fn: rs => fmtMoney(rs.filter(r=>r.adjustment_type==='Non Covered Service').reduce((s,r)=>s+r.line_charge_amount,0)) },
    { key:'avg', label:'Avg $ / Line', fn: rs => { const pr=rs.filter(r=>r.adjustment_type==='Allowed'); return fmtAvg(pr.length>0?pr.reduce((s,r)=>s+r.line_paid_amount,0)/pr.length:0); } },
  ];
  trendMetrics.forEach(m => {
    html += '<tr><td class="metric-name">'+m.label+'</td>';
    months.forEach(mo => {
      const rs = filterRows(ROWS, mo.from, mo.to);
      html += '<td class="num">'+m.fn(rs)+'</td>';
    });
    html += '</tr>';
  });
  html += '</tbody></table>';
  document.getElementById('trendTable').innerHTML = html;
}

function renderDetailTable() {
  const { from, to } = getNavRange();
  let rows = filterRows(ROWS, from, to);
  if (navSearch) {
    const s = navSearch.toLowerCase();
    rows = rows.filter(r => Object.values(r).some(v => String(v).toLowerCase().includes(s)));
  }
  rows = rows.slice().sort((a,b) => {
    const av = String(a[navSortCol]||''), bv = String(b[navSortCol]||'');
    const ad = parseDate(av), bd = parseDate(bv);
    if (ad && bd) return navSortAsc ? (ad-bd) : (bd-ad);
    const an = parseFloat(av), bn = parseFloat(bv);
    if (!isNaN(an) && !isNaN(bn)) return navSortAsc ? (an-bn) : (bn-an);
    return navSortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
  });

  const cols = ['deposit_date','payer_name','level_of_care','adjustment_type','service_facility',
                'service_name','line_patient_name','procedure_code',
                'line_charge_amount','line_paid_amount','line_adjusted','line_allocated_amount'];
  const headers = ['Deposit Date','Payer','Level of Care','Adj Type','Facility',
                   'Service','Patient','Code','Charged $','Paid $','Adjusted $','Allocated $'];
  const moneyIdx = [8,9,10,11];

  let html = '<div class="table-wrap"><table><thead><tr>';
  cols.forEach((c,i) => {
    const arrow = navSortCol===c ? (navSortAsc ? ' &#9650;' : ' &#9660;') : '';
    html += '<th onclick="setSortCol(\''+c+'\')" style="cursor:pointer">'+headers[i]+arrow+'</th>';
  });
  html += '</tr></thead><tbody>';
  const page = rows.slice(0,200);
  page.forEach(r => {
    html += '<tr>';
    cols.forEach((c,i) => {
      const v = r[c];
      const disp = moneyIdx.includes(i) ? fmtMoney(+v) : esc(v);
      html += '<td title="'+esc(v)+'">'+disp+'</td>';
    });
    html += '</tr>';
  });
  if (rows.length === 0) html += '<tr><td colspan="'+cols.length+'" class="no-data">No records for selected period.</td></tr>';
  html += '</tbody></table></div>';
  if (rows.length > 200) html += '<div class="page-info" style="margin-top:8px">Showing 200 of '+rows.length.toLocaleString()+' records. Export CSV for full data.</div>';
  document.getElementById('detailTable').innerHTML = html;
  document.getElementById('periodLabel').innerHTML = navLabel();

  // Update period KPIs
  const m = calcMetrics(rows);
  document.getElementById('kpi-lines').textContent = m.lines.toLocaleString();
  document.getElementById('kpi-charged').textContent = fmtMoney(m.charged);
  document.getElementById('kpi-paid').textContent = fmtMoney(m.paid);
  document.getElementById('kpi-cr').textContent = fmtPct(m.collection_rate);
  document.getElementById('kpi-nr').textContent = fmtPct(m.net_realization);
  document.getElementById('kpi-avg').textContent = fmtAvg(m.avg_per_line);
}

function setSortCol(col) {
  if (navSortCol === col) navSortAsc = !navSortAsc;
  else { navSortCol = col; navSortAsc = false; }
  renderDetailTable();
  renderBreakdowns();
}

function exportCSV() {
  const { from, to } = getNavRange();
  let rows = filterRows(ROWS, from, to);
  const cols = Object.keys(rows[0]||{});
  let csv = cols.map(c=>'"'+c+'"').join(',') + '\n';
  rows.forEach(r => { csv += cols.map(c=>'"'+String(r[c]||'').replace(/"/g,'""')+'"').join(',') + '\n'; });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([csv],{type:'text/csv'}));
  a.download = 'billing_' + navLabel().replace(/[^a-z0-9]/gi,'_') + '.csv';
  a.click();
}

function renderAll() {
  document.getElementById('viewMonth').className = 'view-btn' + (navView==='month'?' active':'');
  document.getElementById('viewWeek').className  = 'view-btn' + (navView==='week' ?' active':'');
  document.getElementById('viewDay').className   = 'view-btn' + (navView==='day'  ?' active':'');
  renderBreakdowns();
  renderDetailTable();
}

renderSpotTable();
renderAll();
renderMonthlyTrend();
"""

CSS = """
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:Arial,sans-serif;background:#f0f2f5;color:#333;font-size:13px}
header{background:#1a3a5c;color:#fff;padding:14px 24px}
header h1{font-size:20px;font-weight:700}
header p{font-size:12px;opacity:.7;margin-top:2px}
.main{padding:16px 20px;max-width:1600px;margin:0 auto}
h2{font-size:15px;font-weight:700;color:#1a3a5c;margin:20px 0 10px;padding-bottom:5px;border-bottom:2px solid #1a3a5c}
h3{font-size:13px;font-weight:700;color:#1a3a5c;margin:0 0 8px}

/* Spot summary table */
.spot-wrap{background:#fff;border-radius:8px;padding:0;box-shadow:0 1px 4px rgba(0,0,0,.08);overflow:auto}
.spot-table{width:100%;border-collapse:collapse;font-size:12px}
.spot-table thead th{background:#1a3a5c;color:#fff;padding:9px 14px;text-align:center;white-space:nowrap;font-weight:600}
.spot-table thead th:first-child{text-align:left}
.spot-table tbody tr{border-bottom:1px solid #eee}
.spot-table tbody tr:hover{background:#f0f5fb}
.spot-table tbody tr:nth-child(even){background:#fafbfd}
.spot-table tbody tr:nth-child(even):hover{background:#f0f5fb}
.spot-table td{padding:8px 14px;white-space:nowrap}
.spot-table td.metric-name{font-weight:600;color:#333}
.spot-table td.num{text-align:right;font-variant-numeric:tabular-nums;color:#1a3a5c}

/* KPI cards */
.kpi-bar{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:14px}
.kpi-card{background:#fff;border-radius:8px;padding:14px 18px;flex:1;min-width:130px;box-shadow:0 1px 4px rgba(0,0,0,.08);border-top:3px solid #1a3a5c}
.kpi-card.green{border-top-color:#217346}
.kpi-card.orange{border-top-color:#c86a00}
.kpi-card .val{font-size:22px;font-weight:700;color:#1a3a5c;line-height:1.2}
.kpi-card .lbl{font-size:11px;color:#777;margin-top:3px;text-transform:uppercase;letter-spacing:.4px}

/* Controls */
.controls{background:#fff;border-radius:8px;padding:12px 16px;margin-bottom:14px;display:flex;flex-wrap:wrap;align-items:center;gap:10px;box-shadow:0 1px 4px rgba(0,0,0,.08)}
.view-btns{display:flex;gap:4px}
.view-btn{padding:5px 14px;border:1.5px solid #1a3a5c;background:#fff;color:#1a3a5c;border-radius:4px;cursor:pointer;font-size:12px;font-weight:600;transition:all .2s}
.view-btn.active{background:#1a3a5c;color:#fff}
.nav-btns{display:flex;align-items:center;gap:8px}
.nav-btn{padding:4px 12px;border:1px solid #ccc;background:#fff;border-radius:4px;cursor:pointer;font-size:16px;font-weight:700}
.nav-btn:hover{background:#eee}
.period-label{font-weight:700;min-width:170px;text-align:center;color:#1a3a5c;background:#eef3f9;padding:5px 10px;border-radius:4px}
.search-box{padding:5px 10px;border:1px solid #ccc;border-radius:4px;font-size:12px;flex:1;min-width:160px}
.export-btn{padding:6px 14px;background:#217346;color:#fff;border:none;border-radius:4px;cursor:pointer;font-size:12px;font-weight:600}
.export-btn:hover{background:#1a5c38}
.date-input{padding:5px 8px;border:1px solid #ccc;border-radius:4px;font-size:12px}

/* Breakdown grid */
.break-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
.break-card{background:#fff;border-radius:8px;padding:14px 16px;box-shadow:0 1px 4px rgba(0,0,0,.08)}
.break-table{width:100%;border-collapse:collapse;font-size:12px}
.break-table thead th{background:#eef3f9;color:#1a3a5c;padding:7px 10px;text-align:left;font-weight:700;border-bottom:2px solid #c5d8ec}
.break-table tbody tr{border-bottom:1px solid #eee}
.break-table tbody tr:hover{background:#f0f5fb}
.break-table td{padding:6px 10px}
.break-table td.num{text-align:right;font-variant-numeric:tabular-nums}
.break-table tr.total-row td{font-weight:700;border-top:2px solid #ccc;background:#f5f8fc}

/* Monthly trend */
.trend-wrap{background:#fff;border-radius:8px;padding:14px 16px;box-shadow:0 1px 4px rgba(0,0,0,.08);overflow:auto;margin-bottom:16px}
.trend-table{width:100%;border-collapse:collapse;font-size:12px}
.trend-table thead th{background:#1a3a5c;color:#fff;padding:8px 14px;text-align:center;white-space:nowrap}
.trend-table thead th:first-child{text-align:left}
.trend-table tbody tr{border-bottom:1px solid #eee}
.trend-table tbody tr:hover{background:#f0f5fb}
.trend-table tbody tr:nth-child(even){background:#fafbfd}
.trend-table td{padding:7px 14px;white-space:nowrap}
.trend-table td.metric-name{font-weight:600}
.trend-table td.num{text-align:right}

/* Detail table */
.table-wrap{background:#fff;border-radius:8px;box-shadow:0 1px 4px rgba(0,0,0,.08);overflow:auto;max-height:480px}
table{width:100%;border-collapse:collapse;font-size:12px}
thead th{background:#1a3a5c;color:#fff;padding:8px 10px;text-align:left;position:sticky;top:0;z-index:2;white-space:nowrap;font-weight:600}
thead th:hover{background:#244d73}
tbody tr{border-bottom:1px solid #eee}
tbody tr:hover{background:#edf3fa}
tbody td{padding:6px 10px;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
tbody tr:nth-child(even){background:#fafbfd}
.no-data{text-align:center;padding:40px;color:#999}
.page-info{font-size:12px;color:#666;margin-top:8px}
"""

html = (
    '<!DOCTYPE html>\n<html lang="en">\n<head>\n'
    '<meta charset="UTF-8">\n'
    '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
    '<title>AR / Billing Dashboard &mdash; Strive Fort Wayne</title>\n'
    '<style>' + CSS + '</style>\n'
    '</head>\n<body>\n'
    '<header>\n'
    '  <h1>AR / Billing Dashboard &mdash; Strive Fort Wayne, IN</h1>\n'
    '  <p>Source: Payment Report Deposit Date &bull; MASTER_Sunwave_New_PowerQuerry.xlsx</p>\n'
    '</header>\n'
    '<div class="main">\n'

    # Spot table
    '<h2>SPOT / MOST RECENT &mdash; Revenue &amp; Collections by Deposit Date</h2>\n'
    '<div class="spot-wrap" id="spotTable"></div>\n'

    # Monthly trend
    '<h2>MONTHLY TREND &mdash; Last 6 Months</h2>\n'
    '<div class="trend-wrap" id="trendTable"></div>\n'

    # Period controls
    '<h2>DETAIL VIEW &mdash; by Selected Period</h2>\n'
    '<div class="controls">\n'
    '  <div class="view-btns">\n'
    '    <button id="viewMonth" class="view-btn active" onclick="setNavView(\'month\')">Month</button>\n'
    '    <button id="viewWeek"  class="view-btn"        onclick="setNavView(\'week\')">Week</button>\n'
    '    <button id="viewDay"   class="view-btn"        onclick="setNavView(\'day\')">Day</button>\n'
    '  </div>\n'
    '  <div class="nav-btns">\n'
    '    <button class="nav-btn" onclick="navigatePeriod(-1)">&#8249;</button>\n'
    '    <span class="period-label" id="periodLabel"></span>\n'
    '    <button class="nav-btn" onclick="navigatePeriod(1)">&#8250;</button>\n'
    '  </div>\n'
    '  <input type="date" class="date-input" title="Jump to date" onchange="jumpNav(this.value)">\n'
    '  <input type="text" class="search-box" placeholder="Search records..." oninput="navSearch=this.value;renderAll()">\n'
    '  <button class="export-btn" onclick="exportCSV()">&#8595; Export CSV</button>\n'
    '</div>\n'

    # Period KPIs
    '<div class="kpi-bar">\n'
    '  <div class="kpi-card"><div class="val" id="kpi-lines">-</div><div class="lbl"># Payment Lines</div></div>\n'
    '  <div class="kpi-card"><div class="val" id="kpi-charged">-</div><div class="lbl">Charged $</div></div>\n'
    '  <div class="kpi-card green"><div class="val" id="kpi-paid">-</div><div class="lbl">Paid $</div></div>\n'
    '  <div class="kpi-card orange"><div class="val" id="kpi-cr">-</div><div class="lbl">Collection Rate</div></div>\n'
    '  <div class="kpi-card orange"><div class="val" id="kpi-nr">-</div><div class="lbl">Net Realization</div></div>\n'
    '  <div class="kpi-card"><div class="val" id="kpi-avg">-</div><div class="lbl">Avg $ / Line</div></div>\n'
    '</div>\n'

    # Breakdown tables
    '<div class="break-grid">\n'
    '  <div class="break-card"><h3>PAID $ BY PAYER</h3><div id="payerTable"></div></div>\n'
    '  <div class="break-card"><h3>PAID $ BY LEVEL OF CARE</h3><div id="locTable"></div></div>\n'
    '</div>\n'
    '<div class="break-grid">\n'
    '  <div class="break-card"><h3>ADJUSTMENTS &amp; DENIALS $</h3><div id="adjTable"></div></div>\n'
    '</div>\n'

    # Detail table
    '<h2>TRANSACTION DETAIL</h2>\n'
    '<div id="detailTable"></div>\n'

    '</div>\n'
    '<script type="application/json" id="billingData">' + data_json + '</script>\n'
    '<script>' + JS + '</script>\n'
    '</body>\n</html>'
)

out = 'Billing_Report.html'
with open(out, 'w', encoding='utf-8') as f:
    f.write(html)
print(f"Done: {os.path.getsize(out)/1024/1024:.1f} MB")
