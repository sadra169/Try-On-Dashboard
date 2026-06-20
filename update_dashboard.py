#!/usr/bin/env python3
"""
Try-On Dashboard  —  Incremental data fetcher + HTML generator
================================================================
  Run:  python update_dashboard.py

Fetches Style Try-on & Makeup Try-on metrics from ClickHouse.
Data is cached in data_cache.json (only missing dates are re-queried).
A self-contained index.html is regenerated every run and is ready
for GitHub Pages hosting.

Credentials are loaded from a .env file (never committed to git).
Copy .env.example → .env and fill in your values.
"""

import json, os, sys, subprocess
from datetime import date, timedelta, datetime
from pathlib import Path

# Load .env file if present (before reading env vars)
def _load_dotenv():
    env_file = Path(__file__).parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

_load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
CACHE_FILE = BASE_DIR / "data_cache.json"
HTML_FILE  = BASE_DIR / "index.html"
START_DATE = date(2026, 6, 17)

DB_HOST = os.environ.get("DB_HOST", "172.16.10.180")
DB_PORT = int(os.environ.get("DB_PORT", "28123"))
DB_USER = os.environ.get("DB_USER", "")
DB_PASS = os.environ.get("DB_PASS", "")

# ── Cache helpers ─────────────────────────────────────────────────────────────

def load_cache() -> dict:
    if CACHE_FILE.exists():
        return json.loads(CACHE_FILE.read_text())
    return {
        "fetched_dates": [], "last_updated": None,
        "style_tp_view": {}, "style_tp_click": {}, "style_upload": {},
        "style_show_result": {}, "style_check_result": {}, "style_upload_type": {},
        "makeup_tp_view": {}, "makeup_tp_click": {},
    }

def save_cache(c: dict):
    CACHE_FILE.write_text(json.dumps(c, indent=2, default=str))

def get_missing_dates(cache: dict) -> list:
    yesterday = date.today() - timedelta(days=1)
    if yesterday < START_DATE:
        return []
    dates, cur = [], START_DATE
    while cur <= yesterday:
        dates.append(str(cur))
        cur += timedelta(days=1)
    done = set(cache.get("fetched_dates", []))
    return [d for d in dates if d not in done]

# ── Database ──────────────────────────────────────────────────────────────────

def ensure_pkg(name: str):
    try:
        __import__(name.replace("-", "_"))
    except ImportError:
        print(f"  Installing {name}...")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", name,
             "--break-system-packages", "-q"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

def run_queries(mn: str, mx: str) -> dict:
    ensure_pkg("clickhouse-connect")
    import clickhouse_connect as cc  # noqa

    print(f"  Connecting to {DB_HOST}:{DB_PORT} ...")
    client = cc.get_client(
        host=DB_HOST, port=DB_PORT,
        username=DB_USER, password=DB_PASS,
        connect_timeout=30, send_receive_timeout=180,
    )

    def q(sql: str, name: str) -> list:
        print(f"    → {name} ...", end=" ", flush=True)
        try:
            r = client.query(sql)
            rows = [list(row) for row in r.result_rows]
            print(f"{len(rows)} rows")
            return rows
        except Exception as exc:
            print(f"ERROR — {exc}")
            return []

    return {
        # ── Style ────────────────────────────────────────────────────────────
        "style_tp_view": q(f"""
            SELECT partition_date,
                   count()                                          AS c,
                   uniqExact(session_id, page_product_id)           AS s,
                   uniqExact(user_id)                               AS u
            FROM bigdata.widget_view
            WHERE partition_date BETWEEN '{mn}' AND '{mx}'
              AND widget_name = 'style'
              AND viewed_item_name = 'style-touchpoint'
            GROUP BY partition_date ORDER BY partition_date
        """, "style_tp_view"),

        "style_tp_click": q(f"""
            SELECT partition_date,
                   count()                                          AS c,
                   uniqExact(session_id, page_product_id)           AS s,
                   uniqExact(user_id)                               AS u
            FROM bigdata.widget_click
            WHERE partition_date BETWEEN '{mn}' AND '{mx}'
              AND widget_name = 'style'
              AND clicked_item_name = 'style-touchpoint'
            GROUP BY partition_date ORDER BY partition_date
        """, "style_tp_click"),

        "style_upload": q(f"""
            SELECT partition_date,
                   count()                                          AS c,
                   uniqExact(session_id, page_product_id)           AS s,
                   uniqExact(user_id)                               AS u
            FROM bigdata.widget_click
            WHERE partition_date BETWEEN '{mn}' AND '{mx}'
              AND widget_name IN ('select-photo-tryon', 'capture-photo')
            GROUP BY partition_date ORDER BY partition_date
        """, "style_upload"),

        "style_show_result": q(f"""
            SELECT partition_date,
                   count()                                          AS c,
                   uniqExact(session_id, page_product_id)           AS s,
                   uniqExact(user_id)                               AS u
            FROM bigdata.widget_view
            WHERE partition_date BETWEEN '{mn}' AND '{mx}'
              AND widget_name = 'show-result'
              AND viewed_item_name = 'generated'
            GROUP BY partition_date ORDER BY partition_date
        """, "style_show_result"),

        "style_check_result": q(f"""
            SELECT partition_date,
                   count()                                          AS c,
                   uniqExact(session_id, page_product_id)           AS s,
                   uniqExact(user_id)                               AS u
            FROM bigdata.widget_view
            WHERE partition_date BETWEEN '{mn}' AND '{mx}'
              AND widget_name = 'show-result'
              AND viewed_item_name != 'generated'
            GROUP BY partition_date ORDER BY partition_date
        """, "style_check_result"),

        "style_upload_type": q(f"""
            SELECT partition_date, widget_name,
                   count()                                          AS c,
                   uniqExact(session_id, page_product_id)           AS s,
                   uniqExact(user_id)                               AS u
            FROM bigdata.widget_click
            WHERE partition_date BETWEEN '{mn}' AND '{mx}'
              AND widget_name IN ('select-photo-tryon', 'capture-photo')
            GROUP BY partition_date, widget_name
            ORDER BY partition_date, widget_name
        """, "style_upload_type"),

        # ── Makeup ───────────────────────────────────────────────────────────
        "makeup_tp_view": q(f"""
            SELECT partition_date, platform,
                   count()                                          AS c,
                   uniqExact(session_id, page_product_id)           AS s,
                   uniqExact(user_id)                               AS u
            FROM bigdata.widget_view
            WHERE partition_date BETWEEN '{mn}' AND '{mx}'
              AND platform NOT IN ('desktop', 'app_web_view')
              AND (
                    widget_name = 'ar-makeup-try-on'
                OR (widget_name = 'makeup' AND viewed_item_name = 'makeup-touchpoint')
              )
            GROUP BY partition_date, platform
            ORDER BY partition_date, platform
        """, "makeup_tp_view"),

        "makeup_tp_click": q(f"""
            SELECT partition_date, platform,
                   count()                                          AS c,
                   uniqExact(session_id, page_product_id)           AS s,
                   uniqExact(user_id)                               AS u
            FROM bigdata.widget_click
            WHERE partition_date BETWEEN '{mn}' AND '{mx}'
              AND platform NOT IN ('desktop', 'app_web_view')
              AND (
                    widget_name = 'ar-makeup-try-on'
                OR (widget_name = 'makeup' AND clicked_item_name = 'makeup-touchpoint')
              )
            GROUP BY partition_date, platform
            ORDER BY partition_date, platform
        """, "makeup_tp_click"),
    }

# ── Merge into cache ──────────────────────────────────────────────────────────

def merge(cache: dict, raw: dict, dates: list) -> dict:
    # Simple per-date keys
    for k in ["style_tp_view", "style_tp_click", "style_upload",
               "style_show_result", "style_check_result"]:
        for row in raw.get(k, []):
            ds = str(row[0])
            cache[k][ds] = {"c": int(row[1]), "s": int(row[2]), "u": int(row[3])}

    # Upload type  (date → widget_name → metrics)
    for row in raw.get("style_upload_type", []):
        ds, wname = str(row[0]), str(row[1])
        cache["style_upload_type"].setdefault(ds, {})[wname] = {
            "c": int(row[2]), "s": int(row[3]), "u": int(row[4])
        }

    # Makeup  (date → platform → metrics)
    for k in ["makeup_tp_view", "makeup_tp_click"]:
        for row in raw.get(k, []):
            ds, plat = str(row[0]), str(row[1])
            cache[k].setdefault(ds, {})[plat] = {
                "c": int(row[2]), "s": int(row[3]), "u": int(row[4])
            }

    cache["fetched_dates"] = sorted(set(cache.get("fetched_dates", []) + dates))
    cache["last_updated"]  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return cache

# ── HTML generation ───────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Try-On Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root {
  --bg:#0f172a; --card:#1e293b; --card2:#263348; --border:#334155;
  --accent:#6366f1; --cyan:#22d3ee; --text:#f1f5f9; --muted:#94a3b8;
  --green:#22c55e; --yellow:#eab308; --red:#ef4444; --orange:#f97316;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:14px;line-height:1.5}
.container{max-width:1500px;margin:0 auto;padding:28px 24px}
/* header */
.hdr{display:flex;justify-content:space-between;align-items:flex-start;
     border-bottom:1px solid var(--border);padding-bottom:20px;margin-bottom:36px;flex-wrap:wrap;gap:12px}
.hdr h1{font-size:22px;font-weight:700;display:flex;align-items:center;gap:10px}
.hdr .meta{font-size:12px;color:var(--muted);margin-top:4px}
.hdr .cmd{font-size:11px;background:var(--card);border:1px solid var(--border);
           padding:6px 12px;border-radius:6px;color:var(--muted);font-family:monospace}
/* section */
.sec{margin-bottom:52px}
.sec-hdr{display:flex;align-items:center;gap:12px;margin-bottom:22px}
.sec-hdr h2{font-size:17px;font-weight:600}
.badge{font-size:11px;padding:3px 10px;border-radius:12px;background:var(--accent);color:#fff;font-weight:500}
.badge.cyan{background:#0891b2}
/* card */
.card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:18px}
.card-title{font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;
            letter-spacing:.07em;margin-bottom:18px}
/* funnel */
.funnel{display:grid;grid-template-columns:repeat(5,1fr);gap:1px;background:var(--border);
        border-radius:10px;overflow:hidden}
.f-step{background:var(--card);padding:18px 14px;text-align:center;position:relative}
.f-step:not(:first-child)::before{content:'›';position:absolute;left:-1px;top:50%;
  transform:translateY(-50%);color:var(--border);font-size:22px;background:var(--bg);
  padding:4px 2px;line-height:1}
.f-label{font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;
          letter-spacing:.06em;margin-bottom:10px}
.f-count{font-size:26px;font-weight:800;color:var(--text)}
.f-users{font-size:12px;color:var(--cyan);margin-top:3px}
.f-sdp{font-size:10px;color:var(--muted);margin-top:2px}
.f-cvr{display:inline-block;margin-top:10px;font-size:11px;padding:3px 8px;
        border-radius:6px;font-weight:600}
.f-cvr.hi{background:rgba(34,197,94,.15);color:var(--green)}
.f-cvr.mid{background:rgba(234,179,8,.15);color:var(--yellow)}
.f-cvr.lo{background:rgba(239,68,68,.15);color:var(--red)}
/* toggle */
.toggle-grp{display:flex;gap:6px;margin-bottom:14px}
.tbtn{background:transparent;border:1px solid var(--border);color:var(--muted);
       padding:5px 14px;border-radius:6px;cursor:pointer;font-size:12px;transition:all .15s}
.tbtn.on{background:var(--accent);border-color:var(--accent);color:#fff}
/* table */
.tbl-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:13px}
thead th{background:rgba(99,102,241,.12);color:var(--muted);font-size:10px;font-weight:700;
          text-transform:uppercase;letter-spacing:.05em;padding:10px 12px;
          text-align:center;border-bottom:1px solid var(--border);white-space:nowrap}
thead th:first-child{text-align:left}
tbody tr{border-bottom:1px solid rgba(51,65,85,.4)}
tbody tr:hover{background:rgba(99,102,241,.05)}
tbody td{padding:9px 12px;text-align:center}
tbody td:first-child{text-align:left;font-weight:500}
.n1{font-weight:600}
.n2{color:var(--cyan);font-size:12px}
.cvr-tag{display:inline-block;padding:2px 7px;border-radius:5px;font-size:11px;font-weight:600}
.cvr-tag.hi{background:rgba(34,197,94,.12);color:var(--green)}
.cvr-tag.mid{background:rgba(234,179,8,.12);color:var(--yellow)}
.cvr-tag.lo{background:rgba(239,68,68,.12);color:var(--red)}
/* chart */
.chart-wrap{position:relative;height:280px}
/* platform pills */
.pill{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600}
.pl-mw{background:rgba(99,102,241,.2);color:#818cf8}
.pl-pwa{background:rgba(34,211,238,.2);color:#22d3ee}
.pl-app{background:rgba(34,197,94,.2);color:#22c55e}
.pl-x{background:rgba(148,163,184,.2);color:#94a3b8}
/* upload type */
.ut-wrap{display:flex;gap:24px;align-items:flex-start;flex-wrap:wrap}
/* summary cards */
.sum-row{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:14px;margin-bottom:18px}
.sum-card{background:var(--card2);border:1px solid var(--border);border-radius:10px;padding:16px 18px}
.sum-label{font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;
            letter-spacing:.06em;margin-bottom:8px}
.sum-val{font-size:24px;font-weight:800}
.sum-sub{font-size:11px;color:var(--muted);margin-top:5px}
hr.div{border:none;border-top:1px solid var(--border);margin:36px 0}
.empty{color:var(--muted);padding:36px;text-align:center;font-size:13px}
</style>
</head>
<body>
<div class="container">

  <div class="hdr">
    <div>
      <h1>🎭 Try-On Dashboard</h1>
      <div class="meta">Feature live since: <strong>2026-06-17</strong> &nbsp;|&nbsp; Last updated: <strong>__LAST__</strong></div>
    </div>
    <code class="cmd">python update_dashboard.py</code>
  </div>

  <!-- ══ SECTION 1 : STYLE TRY-ON ══════════════════════════════════════════ -->
  <div class="sec">
    <div class="sec-hdr">
      <h2>1 · Style Try-on</h2>
      <span class="badge">Mobile-web &amp; PWA only</span>
    </div>

    <div class="card">
      <div class="card-title">Conversion Funnel — aggregate across all available dates</div>
      <div class="funnel" id="funnel"></div>
      <div style="margin-top:12px;font-size:11px;color:var(--muted)">
        Conversions shown as <strong>count CVR</strong> / <strong>user CVR</strong> from previous step.
        Counts = total events · Users = distinct user_id · Sess-PDPs = distinct (session_id, page_product_id).
      </div>
    </div>

    <div class="card">
      <div class="card-title">Daily breakdown with step-to-step conversion</div>
      <div class="toggle-grp" id="st-tog"></div>
      <div class="tbl-wrap"><table id="st-tbl"></table></div>
    </div>

    <div class="card">
      <div class="card-title">Upload method — Capture Photo vs Gallery Upload</div>
      <div class="ut-wrap">
        <canvas id="ut-chart" width="220" height="220" style="max-width:220px;flex-shrink:0"></canvas>
        <div class="tbl-wrap" style="flex:1;min-width:300px">
          <table id="ut-tbl"></table>
        </div>
      </div>
    </div>
  </div>

  <hr class="div">

  <!-- ══ SECTION 2 : MAKEUP TRY-ON ═════════════════════════════════════════ -->
  <div class="sec">
    <div class="sec-hdr">
      <h2>2 · Makeup Try-on (AR)</h2>
      <span class="badge cyan">Mobile-web, PWA &amp; Application</span>
    </div>

    <div class="sum-row" id="mk-sum"></div>

    <div class="card">
      <div class="card-title">CTR by platform (daily)</div>
      <div class="chart-wrap"><canvas id="mk-chart"></canvas></div>
    </div>

    <div class="card">
      <div class="card-title">Daily breakdown by platform</div>
      <div class="toggle-grp" id="mk-tog"></div>
      <div class="tbl-wrap"><table id="mk-tbl"></table></div>
    </div>
  </div>

</div><!-- /container -->

<script>
/* ─── Embedded data ──────────────────────────────────────────────────────── */
const D = __DATA__;

/* ─── Helpers ────────────────────────────────────────────────────────────── */
const fmt = n => (n == null || n === 0) ? '0' : Number(n).toLocaleString();
const pct = (n, d) => d > 0 ? (n / d * 100).toFixed(1) + '%' : '—';

function cvrCls(p) {
  const v = parseFloat(p);
  if (isNaN(v)) return '';
  return v >= 40 ? 'hi' : v >= 15 ? 'mid' : 'lo';
}

function cvrTag(p) {
  const cls = cvrCls(p);
  return cls ? `<span class="cvr-tag ${cls}">${p}</span>` : `<span class="cvr-tag">${p}</span>`;
}

function fCvr(n, d) {
  const p = pct(n, d);
  const cls = cvrCls(p);
  return `<span class="f-cvr ${cls}">${p}</span>`;
}

function platPill(p) {
  const low = (p||'').toLowerCase();
  const cls = low.includes('web') ? 'pl-mw' :
              low === 'pwa'       ? 'pl-pwa' :
              (low === 'android' || low === 'ios' || low === 'app') ? 'pl-app' : 'pl-x';
  return `<span class="pill ${cls}">${p||'—'}</span>`;
}

/* ─── Step metadata ──────────────────────────────────────────────────────── */
const STEPS = [
  {key:'style_tp_view',   label:'TP View'},
  {key:'style_tp_click',  label:'TP Click'},
  {key:'style_upload',    label:'Upload Photo'},
  {key:'style_show_result', label:'Show Result'},
  {key:'style_check_result', label:'Check Result'},
];

function agg(key) {
  let c=0, s=0, u=0;
  Object.values(D[key]||{}).forEach(v => { c+=v.c; s+=v.s; u+=v.u; });
  return {c, s, u};
}

function dayVal(key, ds) { return D[key]?.[ds] || {c:0,s:0,u:0}; }

const allDates = (D.fetched_dates || []).slice().sort();

/* ─── Section 1 · Funnel ─────────────────────────────────────────────────── */
function renderFunnel() {
  const totals = STEPS.map(st => ({...st, ...agg(st.key)}));
  document.getElementById('funnel').innerHTML = totals.map((st, i) => {
    const prev = i > 0 ? totals[i-1] : null;
    const cvrHtml = prev
      ? `<div style="margin-top:10px;font-size:10px;color:var(--muted)">vs prev step</div>
         ${fCvr(st.c, prev.c)} count<br>${fCvr(st.u, prev.u)} user`
      : `<div style="margin-top:14px;font-size:11px;color:var(--accent)">▶ Entry point</div>`;
    return `
      <div class="f-step">
        <div class="f-label">${st.label}</div>
        <div class="f-count">${fmt(st.c)}</div>
        <div class="f-users">👤 ${fmt(st.u)} users</div>
        <div class="f-sdp">${fmt(st.s)} sess-pdps</div>
        ${cvrHtml}
      </div>`;
  }).join('');
}

/* ─── Section 1 · Daily table ────────────────────────────────────────────── */
let stMode = 'count';
function renderStToggle() {
  document.getElementById('st-tog').innerHTML = `
    <button class="tbtn ${stMode==='count'?'on':''}" onclick="stMode='count';renderStToggle();renderStTable()">By Count</button>
    <button class="tbtn ${stMode==='users'?'on':''}" onclick="stMode='users';renderStToggle();renderStTable()">By Users</button>
    <button class="tbtn ${stMode==='sesspdp'?'on':''}" onclick="stMode='sesspdp';renderStToggle();renderStTable()">By Sess-PDPs</button>`;
}

function renderStTable() {
  const tbl = document.getElementById('st-tbl');
  if (!allDates.length) { tbl.innerHTML='<tr><td class="empty">No data fetched yet.</td></tr>'; return; }
  const f = stMode === 'count' ? 'c' : stMode === 'users' ? 'u' : 's';

  tbl.innerHTML = `
    <thead><tr>
      <th>Date</th>
      <th>TP View</th>
      <th>TP Click</th><th>CTR</th>
      <th>Upload</th><th>CVR</th>
      <th>Show Result</th><th>CVR</th>
      <th>Check Result</th><th>CVR</th>
    </tr></thead>
    <tbody>${[...allDates].reverse().map(ds => {
      const tv = dayVal('style_tp_view',     ds)[f];
      const tc = dayVal('style_tp_click',    ds)[f];
      const up = dayVal('style_upload',      ds)[f];
      const sr = dayVal('style_show_result', ds)[f];
      const cr = dayVal('style_check_result',ds)[f];
      return `<tr>
        <td>${ds}</td>
        <td class="n1">${fmt(tv)}</td>
        <td class="n1">${fmt(tc)}</td><td>${cvrTag(pct(tc,tv))}</td>
        <td class="n1">${fmt(up)}</td><td>${cvrTag(pct(up,tc))}</td>
        <td class="n1">${fmt(sr)}</td><td>${cvrTag(pct(sr,up))}</td>
        <td class="n1">${fmt(cr)}</td><td>${cvrTag(pct(cr,sr))}</td>
      </tr>`;
    }).join('')}</tbody>`;
}

/* ─── Section 1 · Upload type ────────────────────────────────────────────── */
function renderUploadType() {
  let cap_c=0, cap_u=0, gal_c=0, gal_u=0;
  const dates = Object.keys(D.style_upload_type||{}).sort();
  dates.forEach(ds => {
    const day = D.style_upload_type[ds]||{};
    cap_c += day['capture-photo']?.c||0;
    cap_u += day['capture-photo']?.u||0;
    gal_c += day['select-photo-tryon']?.c||0;
    gal_u += day['select-photo-tryon']?.u||0;
  });

  /* doughnut */
  const ctx = document.getElementById('ut-chart').getContext('2d');
  if (cap_c + gal_c > 0) {
    new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels: ['📷 Capture Photo', '🖼 Gallery Upload'],
        datasets: [{ data: [cap_c, gal_c], backgroundColor: ['#6366f1','#22d3ee'], borderWidth:0 }]
      },
      options: {
        responsive:false,
        plugins: {
          legend: { labels: { color:'#94a3b8', font:{size:12} } },
          tooltip: { callbacks: { label: c => ` ${fmt(c.raw)}  (${pct(c.raw, cap_c+gal_c)})` } }
        }
      }
    });
  } else {
    ctx.canvas.parentElement.innerHTML = '<div class="empty">No upload-type data yet.</div>';
  }

  /* table */
  const tbl = document.getElementById('ut-tbl');
  if (!dates.length) { tbl.innerHTML='<tr><td class="empty">No data.</td></tr>'; return; }
  tbl.innerHTML = `
    <thead><tr>
      <th>Date</th>
      <th>Capture (count)</th><th>Capture (users)</th>
      <th>Gallery (count)</th><th>Gallery (users)</th>
      <th>Capture %</th><th>Gallery %</th>
    </tr></thead>
    <tbody>${[...dates].reverse().map(ds => {
      const day = D.style_upload_type[ds]||{};
      const cc = day['capture-photo']?.c||0,  cu = day['capture-photo']?.u||0;
      const gc = day['select-photo-tryon']?.c||0, gu = day['select-photo-tryon']?.u||0;
      const tot = cc+gc;
      return `<tr>
        <td>${ds}</td>
        <td class="n1">${fmt(cc)}</td><td class="n2">${fmt(cu)}</td>
        <td class="n1">${fmt(gc)}</td><td class="n2">${fmt(gu)}</td>
        <td>${cvrTag(pct(cc,tot))}</td>
        <td>${cvrTag(pct(gc,tot))}</td>
      </tr>`;
    }).join('')}</tbody>`;
}

/* ─── Section 2 · Makeup summary cards ──────────────────────────────────── */
const MK_EXCLUDE = new Set(['desktop', 'app_web_view']);
function renderMkSummary() {
  const byPlat = {};
  ['makeup_tp_view','makeup_tp_click'].forEach(k => {
    Object.values(D[k]||{}).forEach(dayObj => {
      Object.entries(dayObj).forEach(([p, v]) => {
        if (MK_EXCLUDE.has(p)) return;
        byPlat[p] = byPlat[p] || {vc:0,vu:0,vs:0,cc:0,cu:0,cs:0};
        if (k==='makeup_tp_view')  { byPlat[p].vc+=v.c; byPlat[p].vu+=v.u; byPlat[p].vs+=v.s; }
        else                       { byPlat[p].cc+=v.c; byPlat[p].cu+=v.u; byPlat[p].cs+=v.s; }
      });
    });
  });

  const el = document.getElementById('mk-sum');
  const entries = Object.entries(byPlat);
  if (!entries.length) { el.innerHTML='<div class="empty">No data yet.</div>'; return; }

  /* all-platforms total */
  let tv=0,tu=0,tc=0,tcu=0;
  entries.forEach(([,v])=>{ tv+=v.vc; tu+=v.vu; tc+=v.cc; tcu+=v.cu; });

  el.innerHTML = [
    ['All Platforms', tv, tu, tc, tcu],
    ...entries.map(([p,v]) => [p, v.vc, v.vu, v.cc, v.cu])
  ].map(([label, vc, vu, cc, cu]) => `
    <div class="sum-card">
      <div class="sum-label">${typeof label === 'string' && label !== 'All Platforms' ? platPill(label) : label}</div>
      <div class="sum-val" style="color:var(--accent)">${pct(cc,vc)}</div>
      <div class="sum-sub">CTR (by count)</div>
      <div style="font-size:11px;color:var(--muted);margin-top:6px">
        User CTR: ${pct(cu,vu)}<br>
        Views: ${fmt(vc)} · Clicks: ${fmt(cc)}
      </div>
    </div>`).join('');
}

/* ─── Section 2 · CTR line chart ─────────────────────────────────────────── */
function renderMkChart() {
  const datesSet = new Set([
    ...Object.keys(D.makeup_tp_view||{}),
    ...Object.keys(D.makeup_tp_click||{})
  ]);
  const dates = [...datesSet].sort();
  if (!dates.length) return;

  const platforms = new Set();
  Object.values(D.makeup_tp_view||{}).forEach(d => Object.keys(d).forEach(p => {
    if (!MK_EXCLUDE.has(p)) platforms.add(p);
  }));

  const COLORS = ['#6366f1','#22d3ee','#22c55e','#f59e0b','#f43f5e','#a855f7'];
  const datasets = [...platforms].map((p, i) => ({
    label: p,
    data: dates.map(ds => {
      const vc = D.makeup_tp_view?.[ds]?.[p]?.c || 0;
      const cc = D.makeup_tp_click?.[ds]?.[p]?.c || 0;
      return vc > 0 ? +(cc / vc * 100).toFixed(2) : null;
    }),
    borderColor: COLORS[i % COLORS.length],
    backgroundColor: COLORS[i % COLORS.length] + '22',
    tension: 0.3, fill: false, pointRadius: 5, pointHoverRadius: 7,
  }));

  new Chart(document.getElementById('mk-chart').getContext('2d'), {
    type: 'line',
    data: { labels: dates, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      scales: {
        x: { ticks:{color:'#94a3b8'}, grid:{color:'#1e293b'} },
        y: {
          ticks: { color:'#94a3b8', callback: v => v+'%' },
          grid:  { color:'#334155' },
          title: { display:true, text:'CTR %', color:'#94a3b8' }
        }
      },
      plugins: {
        legend: { labels:{color:'#f1f5f9',font:{size:12}} },
        tooltip: { callbacks: { label: c => ` ${c.dataset.label}: ${c.raw ?? '—'}%` } }
      }
    }
  });
}

/* ─── Section 2 · Daily table ────────────────────────────────────────────── */
let mkMode = 'count';
function renderMkToggle() {
  document.getElementById('mk-tog').innerHTML = `
    <button class="tbtn ${mkMode==='count'?'on':''}"   onclick="mkMode='count';renderMkToggle();renderMkTable()">By Count</button>
    <button class="tbtn ${mkMode==='users'?'on':''}"   onclick="mkMode='users';renderMkToggle();renderMkTable()">By Users</button>
    <button class="tbtn ${mkMode==='sesspdp'?'on':''}" onclick="mkMode='sesspdp';renderMkToggle();renderMkTable()">By Sess-PDPs</button>`;
}

function renderMkTable() {
  const tbl = document.getElementById('mk-tbl');
  const f = mkMode === 'count' ? 'c' : mkMode === 'users' ? 'u' : 's';

  const datesSet = new Set([
    ...Object.keys(D.makeup_tp_view||{}),
    ...Object.keys(D.makeup_tp_click||{})
  ]);
  const dates = [...datesSet].sort().reverse();

  if (!dates.length) { tbl.innerHTML='<tr><td class="empty" colspan="6">No data yet.</td></tr>'; return; }

  const rows = dates.flatMap(ds => {
    const allPlats = new Set([
      ...Object.keys(D.makeup_tp_view?.[ds]||{}),
      ...Object.keys(D.makeup_tp_click?.[ds]||{})
    ].filter(p => !MK_EXCLUDE.has(p)));
    return [...allPlats].map(p => {
      const v = D.makeup_tp_view?.[ds]?.[p]?.[f]  || 0;
      const c = D.makeup_tp_click?.[ds]?.[p]?.[f] || 0;
      const ctr = pct(c, v);
      return `<tr>
        <td>${ds}</td>
        <td>${platPill(p)}</td>
        <td class="n1">${fmt(v)}</td>
        <td class="n1">${fmt(c)}</td>
        <td>${cvrTag(ctr)}</td>
      </tr>`;
    });
  }).join('');

  tbl.innerHTML = `
    <thead><tr>
      <th>Date</th><th>Platform</th>
      <th>TP Views</th><th>TP Clicks</th><th>CTR</th>
    </tr></thead>
    <tbody>${rows}</tbody>`;
}

/* ─── Boot ───────────────────────────────────────────────────────────────── */
renderFunnel();
renderStToggle();
renderStTable();
renderUploadType();
renderMkSummary();
renderMkChart();
renderMkToggle();
renderMkTable();
</script>
</body>
</html>
"""

def generate_html(cache: dict) -> str:
    last = cache.get("last_updated") or "never"
    data = json.dumps(cache, ensure_ascii=False, default=str)
    return HTML.replace("__DATA__", data).replace("__LAST__", last)

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 54)
    print("   Try-On Dashboard Updater")
    print("=" * 54)

    cache   = load_cache()
    missing = get_missing_dates(cache)

    if missing:
        print(f"\n[Fetch] {len(missing)} new date(s): {missing[0]} → {missing[-1]}")
        raw    = run_queries(missing[0], missing[-1])
        cache  = merge(cache, raw, missing)
        save_cache(cache)
        print(f"  ✓ Cache saved → {CACHE_FILE.name}")
    else:
        print("\n[Cache] All dates up to yesterday are already fetched.")

    print(f"\n[Build] Generating {HTML_FILE.name} ...")
    html = generate_html(cache)
    HTML_FILE.write_text(html, encoding="utf-8")
    print(f"  ✓ Dashboard ready → {HTML_FILE}")
    print("\nOpen dashboard.html in your browser to view the results.\n")

if __name__ == "__main__":
    main()
