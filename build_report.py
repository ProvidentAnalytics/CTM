import json, os

with open('report_data.json', 'r') as f:
    raw_data = json.load(f)

DATE_FIELDS = {
    'Census': 'Admission Date',
    'Census Active': 'Admission Date',
    'Census_Admitted': 'Admission Date',
    'Census_Discharge': 'Discharge Date',
    'GroupNotes': 'session_date',
    'Incident Report': 'incident_reports.date_of_incident',
    'Opportunities Active': 'created_on',
    'Opportunities by Created Date': 'created_on',
    'Opportunities': 'created_on',
    'Patients': 'created_on',
    'Payment Report Payment Date': 'payment_date',
    'Payment Report Deposit Date': 'deposit_date',
    'Referral Active': 'created_on',
    'Report Auth': 'admission_date',
    'Report Deleted Form': 'deleted_on',
    'Report Diagnois Changes': 'date_from',
    'Report Form Modified': 'modified_on',
    'Report Program Change': 'start_on',
    'Report UR Changes': 'admission_date',
    'Users': 'created_on',
}

tab_config = {}
for sheet, info in raw_data.items():
    date_col = DATE_FIELDS.get(sheet, '')
    date_idx = -1
    if date_col and date_col in info['columns']:
        date_idx = info['columns'].index(date_col)
    tab_config[sheet] = date_idx

data_json = json.dumps(raw_data, separators=(',', ':'), ensure_ascii=True)
config_json = json.dumps(tab_config, separators=(',', ':'))

# Escape </script> in JSON data just in case
data_json = data_json.replace('</', '<\\/')

JS_CODE = r"""
const RAW = JSON.parse(document.getElementById('reportData').textContent);
const DATE_IDX = JSON.parse(document.getElementById('dateIdx').textContent);

const SHEETS = Object.keys(RAW);
let curSheet = SHEETS[0];
let curView = {}, curOffset = {}, curSearch = {}, curDate = {}, sortState = {};
const PAGE_SIZE = 100;
SHEETS.forEach(s => {
  curView[s] = 'month'; curOffset[s] = 0; curSearch[s] = ''; curDate[s] = null;
  sortState[s] = { col: -1, asc: true };
});

function parseDate(str) {
  if (!str || str === 'NaT' || str === '' || str === 'nan') return null;
  const m = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (m) return new Date(+m[3], +m[1]-1, +m[2]);
  const d = new Date(str);
  return isNaN(d.getTime()) ? null : d;
}

function fmtDate(d) {
  if (!d) return '';
  return String(d.getMonth()+1).padStart(2,'0') + '/' + String(d.getDate()).padStart(2,'0') + '/' + d.getFullYear();
}

function getWeekStart(d) { const c = new Date(d); c.setDate(c.getDate() - c.getDay()); return c; }
function sameMonth(a, b) { return a.getFullYear()===b.getFullYear() && a.getMonth()===b.getMonth(); }
function sameWeek(a, b) { return getWeekStart(a).getTime() === getWeekStart(b).getTime(); }
function sameDay(a, b) { return a.getFullYear()===b.getFullYear() && a.getMonth()===b.getMonth() && a.getDate()===b.getDate(); }

function getFilteredRows(sheet) {
  const info = RAW[sheet], dIdx = DATE_IDX[sheet], ref = curDate[sheet];
  const view = curView[sheet], search = curSearch[sheet].toLowerCase();
  let rows = info.rows.filter(row => {
    if (dIdx >= 0 && ref) {
      const d = parseDate(row[dIdx]);
      if (!d) return false;
      if (view === 'month' && !sameMonth(d, ref)) return false;
      if (view === 'week' && !sameWeek(d, ref)) return false;
      if (view === 'day' && !sameDay(d, ref)) return false;
    }
    if (search) return row.some(c => String(c).toLowerCase().includes(search));
    return true;
  });
  const ss = sortState[sheet];
  if (ss.col >= 0) {
    rows = rows.slice().sort((a, b) => {
      const av = String(a[ss.col]||''), bv = String(b[ss.col]||'');
      const ad = parseDate(av), bd = parseDate(bv);
      if (ad && bd) return ss.asc ? (ad - bd) : (bd - ad);
      return ss.asc ? av.localeCompare(bv) : bv.localeCompare(av);
    });
  }
  return rows;
}

function getPeriodLabel(sheet) {
  const ref = curDate[sheet], view = curView[sheet];
  if (!ref) return 'All Dates';
  if (view === 'month') return ref.toLocaleString('default', {month:'long'}) + ' ' + ref.getFullYear();
  if (view === 'week') {
    const ws = getWeekStart(ref), we = new Date(ws); we.setDate(we.getDate()+6);
    return fmtDate(ws) + ' \u2013 ' + fmtDate(we);
  }
  return fmtDate(ref);
}

function navigatePeriod(sheet, dir) {
  const view = curView[sheet]; let ref = new Date(curDate[sheet] || new Date());
  if (view === 'month') ref = new Date(ref.getFullYear(), ref.getMonth()+dir, 1);
  else if (view === 'week') ref.setDate(ref.getDate() + dir*7);
  else ref.setDate(ref.getDate() + dir);
  curDate[sheet] = ref; curOffset[sheet] = 0; renderPanel(sheet);
}

function setView(sheet, view) {
  curView[sheet] = view; curOffset[sheet] = 0;
  if (!curDate[sheet]) {
    const dIdx = DATE_IDX[sheet];
    if (dIdx >= 0) {
      for (const row of RAW[sheet].rows) { const d = parseDate(row[dIdx]); if (d) { curDate[sheet]=d; break; } }
    }
    if (!curDate[sheet]) curDate[sheet] = new Date();
  }
  renderPanel(sheet);
}

function sortCol(sheet, ci) {
  const ss = sortState[sheet];
  if (ss.col === ci) ss.asc = !ss.asc; else { ss.col = ci; ss.asc = true; }
  curOffset[sheet] = 0; renderPanel(sheet);
}

function gotoPage(sheet, offset) { curOffset[sheet] = Math.max(0, offset); renderPanel(sheet); }
function doSearch(sheet, val) { curSearch[sheet] = val; curOffset[sheet] = 0; renderPanel(sheet); }
function jumpToDate(sheet, val) {
  if (!val) return;
  const p = val.split('-'); curDate[sheet] = new Date(+p[0], +p[1]-1, +p[2]); curOffset[sheet] = 0; renderPanel(sheet);
}

function exportCSV(sheet) {
  const info = RAW[sheet], filtered = getFilteredRows(sheet);
  let csv = info.columns.map(c => '"' + c.replace(/"/g,'""') + '"').join(',') + '\n';
  filtered.forEach(row => { csv += row.map(c => '"' + String(c||'').replace(/"/g,'""') + '"').join(',') + '\n'; });
  const blob = new Blob([csv], {type:'text/csv'});
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob);
  a.download = sheet.replace(/[^a-z0-9]/gi,'_') + '.csv'; a.click();
}

function esc(s) {
  return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

function renderPanel(sheet) {
  const panel = document.getElementById('panel-' + sheet.replace(/[^a-zA-Z0-9]/g,'_'));
  const info = RAW[sheet], cols = info.columns, dIdx = DATE_IDX[sheet], view = curView[sheet];
  const filtered = getFilteredRows(sheet), offset = curOffset[sheet];
  const page = filtered.slice(offset, offset + PAGE_SIZE);
  const totalPages = Math.ceil(filtered.length / PAGE_SIZE);
  const curPage = Math.floor(offset / PAGE_SIZE) + 1;

  // Stats
  let statsHtml = '<div class="stats-bar">';
  statsHtml += '<div class="stat-card"><div class="val">' + filtered.length.toLocaleString() + '</div><div class="lbl">Total Records</div></div>';
  if (dIdx >= 0) {
    const ds = new Set(); filtered.forEach(r => { const d = parseDate(r[dIdx]); if (d) ds.add(fmtDate(d)); });
    statsHtml += '<div class="stat-card green"><div class="val">' + ds.size + '</div><div class="lbl">Distinct Dates</div></div>';
  }
  const nIdx = cols.findIndex(c => c.toLowerCase() === 'patient name' || c.toLowerCase() === 'patient_name' || c.toLowerCase() === 'line_patient_name');
  if (nIdx >= 0) {
    const u = new Set(filtered.map(r => r[nIdx]).filter(v => v && v !== ''));
    statsHtml += '<div class="stat-card orange"><div class="val">' + u.size + '</div><div class="lbl">Unique Patients</div></div>';
  }
  const fIdx = cols.findIndex(c => c.toLowerCase().includes('service_facility') || c.toLowerCase() === 'service facility');
  if (fIdx >= 0) {
    const u = new Set(filtered.map(r => r[fIdx]).filter(v => v && v !== ''));
    statsHtml += '<div class="stat-card purple"><div class="val">' + u.size + '</div><div class="lbl">Facilities</div></div>';
  }
  statsHtml += '</div>';

  // Controls
  const hasDate = dIdx >= 0;
  const dateColName = hasDate ? cols[dIdx] : '';
  let navHtml = '';
  if (hasDate) {
    navHtml = '<div class="nav-btns">'
      + '<button class="nav-btn" onclick="navigatePeriod(\'' + esc(sheet) + '\',-1)">&#8249;</button>'
      + '<span class="period-label">' + getPeriodLabel(sheet) + '</span>'
      + '<button class="nav-btn" onclick="navigatePeriod(\'' + esc(sheet) + '\',1)">&#8250;</button>'
      + '</div>'
      + '<input type="date" class="date-input" title="Jump to date" onchange="jumpToDate(\'' + esc(sheet) + '\',this.value)">'
      + '<span class="date-field-label">by: ' + esc(dateColName) + '</span>';
  } else {
    navHtml = '<span class="period-label">All Dates</span>';
  }

  let viewBtns = '';
  if (hasDate) {
    viewBtns = '<div class="view-btns">'
      + '<button class="view-btn ' + (view==='month'?'active':'') + '" onclick="setView(\'' + esc(sheet) + '\',\'month\')">Month</button>'
      + '<button class="view-btn ' + (view==='week'?'active':'') + '" onclick="setView(\'' + esc(sheet) + '\',\'week\')">Week</button>'
      + '<button class="view-btn ' + (view==='day'?'active':'') + '" onclick="setView(\'' + esc(sheet) + '\',\'day\')">Day</button>'
      + '</div>';
  }

  // Table
  let tableHtml = '';
  if (page.length === 0) {
    tableHtml = '<div class="no-data">No records found for the selected period.<br><small>Try navigating to a different period or clear the search.</small></div>';
  } else {
    const ss = sortState[sheet];
    tableHtml = '<div class="table-wrap"><table><thead><tr>';
    cols.forEach((c, i) => {
      const arrow = ss.col === i ? (ss.asc ? ' &#9650;' : ' &#9660;') : '';
      tableHtml += '<th onclick="sortCol(\'' + esc(sheet) + '\',' + i + ')" title="Sort by ' + esc(c) + '">' + esc(c) + arrow + '</th>';
    });
    tableHtml += '</tr></thead><tbody>';
    page.forEach(row => {
      tableHtml += '<tr>';
      row.forEach(cell => {
        const v = esc(cell);
        tableHtml += '<td title="' + v + '">' + v + '</td>';
      });
      tableHtml += '</tr>';
    });
    tableHtml += '</tbody></table></div>';
  }

  // Pagination
  let pageHtml = '';
  if (totalPages > 1) {
    pageHtml = '<div class="pagination">';
    pageHtml += '<span class="page-info">Showing ' + (offset+1) + '\u2013' + Math.min(offset+PAGE_SIZE, filtered.length) + ' of ' + filtered.length.toLocaleString() + '</span>';
    if (curPage > 1) pageHtml += '<button class="page-btn" onclick="gotoPage(\'' + esc(sheet) + '\',0)">\u00ab First</button>';
    if (curPage > 1) pageHtml += '<button class="page-btn" onclick="gotoPage(\'' + esc(sheet) + '\',' + (offset-PAGE_SIZE) + ')">\u2039 Prev</button>';
    const s2 = Math.max(0, curPage-3), e2 = Math.min(totalPages, curPage+2);
    for (let i = s2; i < e2; i++) {
      pageHtml += '<button class="page-btn ' + (i===curPage-1?'active':'') + '" onclick="gotoPage(\'' + esc(sheet) + '\',' + (i*PAGE_SIZE) + ')">' + (i+1) + '</button>';
    }
    if (curPage < totalPages) pageHtml += '<button class="page-btn" onclick="gotoPage(\'' + esc(sheet) + '\',' + (offset+PAGE_SIZE) + ')">Next \u203a</button>';
    if (curPage < totalPages) pageHtml += '<button class="page-btn" onclick="gotoPage(\'' + esc(sheet) + '\',' + ((totalPages-1)*PAGE_SIZE) + ')">Last \u00bb</button>';
    pageHtml += '</div>';
  }

  const searchVal = esc(curSearch[sheet]);
  panel.innerHTML = '<div class="controls">'
    + viewBtns + navHtml
    + '<input type="text" class="search-box" placeholder="Search all columns..." value="' + searchVal + '" oninput="doSearch(\'' + esc(sheet) + '\',this.value)">'
    + '<button class="export-btn" onclick="exportCSV(\'' + esc(sheet) + '\')">&#8595; Export CSV</button>'
    + '</div>'
    + statsHtml + tableHtml + pageHtml;
}

function buildTabBar() {
  const bar = document.getElementById('tabBar'); bar.innerHTML = '';
  SHEETS.forEach(s => {
    const btn = document.createElement('button');
    btn.className = 'tab-btn' + (s === curSheet ? ' active' : '');
    btn.textContent = s;
    btn.onclick = () => {
      curSheet = s;
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
      document.getElementById('panel-' + s.replace(/[^a-zA-Z0-9]/g,'_')).classList.add('active');
    };
    bar.appendChild(btn);
  });
}

function buildPanelShells() {
  const main = document.getElementById('mainContent'); main.innerHTML = '';
  SHEETS.forEach(s => {
    const div = document.createElement('div');
    div.className = 'panel' + (s === curSheet ? ' active' : '');
    div.id = 'panel-' + s.replace(/[^a-zA-Z0-9]/g, '_');
    main.appendChild(div);
  });
}

buildTabBar();
buildPanelShells();
SHEETS.forEach(s => {
  const dIdx = DATE_IDX[s];
  if (dIdx >= 0) {
    for (const row of RAW[s].rows) { const d = parseDate(row[dIdx]); if (d) { curDate[s] = d; break; } }
  }
  if (!curDate[s]) curDate[s] = new Date();
  renderPanel(s);
});
"""

CSS = """
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:Arial,sans-serif;background:#f0f2f5;color:#333;font-size:13px}
header{background:#1a3a5c;color:#fff;padding:14px 24px}
header h1{font-size:20px;font-weight:700;letter-spacing:.5px}
header p{font-size:12px;opacity:.7;margin-top:2px}
.tab-bar{background:#1e4d78;display:flex;flex-wrap:wrap;gap:2px;padding:6px 12px}
.tab-btn{background:transparent;border:none;color:#aac4e0;padding:7px 13px;cursor:pointer;font-size:12px;border-radius:4px;white-space:nowrap;transition:all .2s}
.tab-btn:hover{background:rgba(255,255,255,.12);color:#fff}
.tab-btn.active{background:#fff;color:#1a3a5c;font-weight:700}
.main{padding:16px 20px}
.panel{display:none}
.panel.active{display:block}
.controls{background:#fff;border-radius:8px;padding:14px 18px;margin-bottom:14px;display:flex;flex-wrap:wrap;align-items:center;gap:12px;box-shadow:0 1px 4px rgba(0,0,0,.08)}
.view-btns{display:flex;gap:4px}
.view-btn{padding:6px 16px;border:1.5px solid #1a3a5c;background:#fff;color:#1a3a5c;border-radius:4px;cursor:pointer;font-size:12px;font-weight:600;transition:all .2s}
.view-btn.active{background:#1a3a5c;color:#fff}
.nav-btns{display:flex;align-items:center;gap:8px}
.nav-btn{padding:5px 14px;border:1px solid #ccc;background:#fff;border-radius:4px;cursor:pointer;font-size:16px;font-weight:700}
.nav-btn:hover{background:#e8edf2}
.period-label{font-weight:700;font-size:13px;min-width:180px;text-align:center;color:#1a3a5c;background:#eef3f9;padding:5px 12px;border-radius:4px}
.date-input{padding:5px 10px;border:1px solid #ccc;border-radius:4px;font-size:12px}
.search-box{padding:6px 10px;border:1px solid #ccc;border-radius:4px;font-size:12px;flex:1;min-width:180px}
.date-field-label{font-size:11px;color:#999;font-style:italic}
.stats-bar{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:14px}
.stat-card{background:#fff;border-radius:8px;padding:14px 20px;min-width:140px;flex:1;box-shadow:0 1px 4px rgba(0,0,0,.08);border-left:4px solid #1a3a5c}
.stat-card.green{border-left-color:#217346}
.stat-card.orange{border-left-color:#c86a00}
.stat-card.purple{border-left-color:#6a3a9c}
.stat-card .val{font-size:28px;font-weight:700;color:#1a3a5c;line-height:1.2}
.stat-card .lbl{font-size:11px;color:#777;margin-top:3px;text-transform:uppercase;letter-spacing:.5px}
.table-wrap{background:#fff;border-radius:8px;box-shadow:0 1px 4px rgba(0,0,0,.08);overflow:auto;max-height:500px}
table{width:100%;border-collapse:collapse;font-size:12px}
thead th{background:#1a3a5c;color:#fff;padding:9px 10px;text-align:left;position:sticky;top:0;z-index:2;white-space:nowrap;font-weight:600;cursor:pointer;user-select:none}
thead th:hover{background:#244d73}
tbody tr{border-bottom:1px solid #eee}
tbody tr:hover{background:#edf3fa}
tbody td{padding:7px 10px;max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
tbody tr:nth-child(even){background:#fafbfd}
tbody tr:nth-child(even):hover{background:#edf3fa}
.no-data{text-align:center;padding:48px;color:#999;font-size:14px}
.pagination{display:flex;align-items:center;gap:6px;margin-top:12px;flex-wrap:wrap}
.page-info{font-size:12px;color:#666;margin-right:4px}
.page-btn{padding:4px 10px;border:1px solid #ccc;background:#fff;border-radius:4px;cursor:pointer;font-size:11px}
.page-btn:hover{background:#eee}
.page-btn.active{background:#1a3a5c;color:#fff;border-color:#1a3a5c}
.export-btn{padding:7px 16px;background:#217346;color:#fff;border:none;border-radius:4px;cursor:pointer;font-size:12px;font-weight:600;white-space:nowrap}
.export-btn:hover{background:#1a5c38}
"""

html = (
    '<!DOCTYPE html>\n<html lang="en">\n<head>\n'
    '<meta charset="UTF-8">\n'
    '<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
    '<title>Sunwave Reports Dashboard</title>\n'
    '<style>' + CSS + '</style>\n'
    '</head>\n<body>\n'
    '<header><h1>Sunwave Reports Dashboard</h1>'
    '<p>Provident Healthcare Management &mdash; MASTER_Sunwave_New_PowerQuerry.xlsx</p></header>\n'
    '<div class="tab-bar" id="tabBar"></div>\n'
    '<div class="main" id="mainContent"></div>\n'
    '<script type="application/json" id="reportData">' + data_json + '</script>\n'
    '<script type="application/json" id="dateIdx">' + config_json + '</script>\n'
    '<script>' + JS_CODE + '</script>\n'
    '</body>\n</html>'
)

with open('Sunwave_Reports.html', 'w', encoding='utf-8') as f:
    f.write(html)

print(f"Done: {os.path.getsize('Sunwave_Reports.html')/1024/1024:.1f} MB")
