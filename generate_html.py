#!/usr/bin/env python3
"""
generate_html.py
================
Reads tjrv_h2s.csv and generates docs/index.html with fresh embedded data.

Usage:
    python generate_html.py
    python generate_html.py --csv data/tjrv_h2s.csv --out docs/index.html
"""

import csv, json, argparse, os
from collections import defaultdict
from datetime import datetime, date

# ── CLI ────────────────────────────────────────────────────────────────────────

parser = argparse.ArgumentParser()
parser.add_argument('--csv', default='data/tjrv_h2s.csv')
parser.add_argument('--out', default='docs/index.html')
parser.add_argument('--template', default='template.html')
args = parser.parse_args()

# ── Load CSV ───────────────────────────────────────────────────────────────────

rows = []
with open(args.csv, newline='') as f:
    for r in csv.DictReader(f):
        if r['location'] and r['date'] and ':' in r.get('time', ''):
            rows.append(r)

print(f'Loaded {len(rows):,} rows from {args.csv}')

LOCATIONS = ['IB CIVIC CTR', 'NESTOR - BES', 'SAN YSIDRO']

# ── Compute DATA (day-max ppb per location) ────────────────────────────────────

day_vals = defaultdict(lambda: defaultdict(list))
for r in rows:
    if r['ppb_h2s'].strip():
        day_vals[r['location']][r['date']].append(float(r['ppb_h2s']))

DATA = {}
for loc in LOCATIONS:
    DATA[loc] = {}
    for d, vals in day_vals[loc].items():
        DATA[loc][d] = round(max(vals), 1)

# ── Compute SAMPLES ────────────────────────────────────────────────────────────

SAMPLES = {}
for loc in LOCATIONS:
    loc_rows = [r for r in rows if r['location'] == loc]
    total  = len(loc_rows)
    nodata = sum(1 for r in loc_rows if not r['ppb_h2s'].strip())
    orange = sum(1 for r in loc_rows if r['ppb_h2s'].strip() and float(r['ppb_h2s']) >= 30)
    yellow = sum(1 for r in loc_rows if r['ppb_h2s'].strip() and 5 <= float(r['ppb_h2s']) < 30)
    green  = sum(1 for r in loc_rows if r['ppb_h2s'].strip() and float(r['ppb_h2s']) < 5)
    SAMPLES[loc] = {'total': total, 'nodata': nodata, 'orange': orange, 'yellow': yellow, 'green': green}

# ── Compute DETAIL (per-day stats for tooltips) ────────────────────────────────

DETAIL = {}
for loc in LOCATIONS:
    DETAIL[loc] = {}
    loc_rows = [r for r in rows if r['location'] == loc]
    by_day = defaultdict(list)
    for r in loc_rows:
        by_day[r['date']].append(float(r['ppb_h2s']) if r['ppb_h2s'].strip() else None)
    for d, vals in by_day.items():
        real = [v for v in vals if v is not None]
        DETAIL[loc][d] = {
            'pk': round(max(real), 1) if real else None,
            'av': round(sum(real) / len(real), 1) if real else None,
            'or': sum(1 for v in real if v >= 30),
            'ye': sum(1 for v in real if 5 <= v < 30),
            'gr': sum(1 for v in real if v < 5),
            'nd': sum(1 for v in vals if v is None),
        }

# ── Compute HOURLY_BY_DAY (24-element arrays per day per location) ─────────────

hourly_by_day = defaultdict(lambda: defaultdict(lambda: [None] * 24))
for r in rows:
    if not r['ppb_h2s'].strip():
        continue
    hour = int(r['time'].split(':')[0])
    val  = round(float(r['ppb_h2s']), 1)
    hourly_by_day[r['location']][r['date']][hour] = val

HOURLY_BY_DAY = {loc: dict(dates) for loc, dates in hourly_by_day.items()}

# ── Compute HOURLY (all-time average per hour per location) ────────────────────

HOURLY = {}
for loc in LOCATIONS:
    hour_vals = defaultdict(list)
    for r in rows:
        if r['location'] == loc and r['ppb_h2s'].strip():
            hour = int(r['time'].split(':')[0])
            hour_vals[hour].append(float(r['ppb_h2s']))
    HOURLY[loc] = {str(h): round(sum(v) / len(v), 2) for h, v in hour_vals.items()}

# ── Snapshot date ──────────────────────────────────────────────────────────────

all_datetimes = [r['datetime'] for r in rows if r.get('datetime', '').strip()]
if all_datetimes:
    latest_dt_str = max(all_datetimes)
    latest_dt = datetime.strptime(latest_dt_str, '%Y-%m-%d %H:%M')
else:
    latest_dt = datetime.utcnow()
snapshot_label = latest_dt.strftime('%b %-d %Y at %-I:%M %p')  # e.g. "Mar 19 2026 at 1:00 PM"

print(f'Latest data datetime: {latest_dt} → "{snapshot_label}"')

# ── Serialise constants ────────────────────────────────────────────────────────

sep = (',', ':')  # compact JSON

data_block = (
    f'const DATA = {json.dumps(DATA, separators=sep)};\n'
    f'const SAMPLES = {json.dumps(SAMPLES, separators=sep)};\n'
    f'const DETAIL = {json.dumps(DETAIL, separators=sep)};\n'
    f'const HOURLY_BY_DAY = {json.dumps(HOURLY_BY_DAY, separators=sep)};\n'
    f'const HOURLY = {json.dumps(HOURLY, separators=sep)};\n'
)

print(f'Data block size: {len(data_block) / 1024:.1f} KB')

# ── Read template, inject data, update snapshot date ──────────────────────────

with open(args.template) as f:
    template = f.read()

assert '{{DATA_BLOCK}}' in template, 'template missing {{DATA_BLOCK}} placeholder'
html = template.replace('{{DATA_BLOCK}}', data_block)
html = html.replace('{{SNAPSHOT_DATE}}', snapshot_label)

# ── Write output ───────────────────────────────────────────────────────────────

os.makedirs(os.path.dirname(args.out), exist_ok=True)
with open(args.out, 'w') as f:
    f.write(html)

print(f'Written {len(html) / 1024:.1f} KB → {args.out}')
