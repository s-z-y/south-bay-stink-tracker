# South Bay Stink Tracker

A dashboard visualizing hydrogen sulfide (H₂S) air quality readings from three
community monitoring stations in the Tijuana River Valley, San Diego.

**Live site:** https://s-z-y.github.io/south-bay-stink-tracker *(update after deploy)*

## Data source

San Diego Air Pollution Control District (SDAPCD) via their
[Tijuana River Valley monitoring program](https://www.sdapcd.org/content/sdapcd/about/tj-river-valley/tjrv-air-quality-monitoring.html).
Data is fetched from their Power BI dashboard API.

## Monitoring stations

| Station | Location |
|---------|----------|
| IB CIVIC CTR | Imperial Beach Civic Center |
| NESTOR - BES | Berry Elementary School, Nestor |
| SAN YSIDRO | Fire Station #29, San Ysidro |

## How it works

```
fetch_tjrv_latest.py    →    data/tjrv_h2s.csv    →    generate_html.py    →    docs/index.html
  (Power BI API)              (cumulative CSV)           (template.html)          (GitHub Pages)
```

A GitHub Actions cron job runs hourly:
1. Fetches the last 2 days of readings from the Power BI API
2. Appends new rows to `data/tjrv_h2s.csv` (deduplicates automatically)
3. Regenerates `docs/index.html` from the full CSV
4. Commits and pushes if data changed

## Setup

### 1. Fork / clone this repo

```bash
git clone https://github.com/yourusername/south-bay-stink-tracker
cd south-bay-stink-tracker
```

### 2. Enable GitHub Pages

In your repo: **Settings → Pages → Source: Deploy from branch → Branch: `main` / Folder: `/docs`**

### 3. Enable GitHub Actions

Actions should be enabled by default. The workflow runs automatically every hour.
You can also trigger it manually from the **Actions** tab → **Hourly data update** → **Run workflow**.

### 4. (Optional) Run locally

```bash
# Fetch latest data
python fetch_tjrv_latest.py --days 7 --out data/tjrv_h2s.csv --append

# Regenerate dashboard
python generate_html.py

# Open in browser
open docs/index.html
```

## Files

| File | Purpose |
|------|---------|
| `fetch_tjrv_latest.py` | Fetches H₂S data from SDAPCD Power BI API |
| `generate_html.py` | Reads CSV, computes data constants, renders template |
| `template.html` | Dashboard HTML with `{{DATA_BLOCK}}` and `{{SNAPSHOT_DATE}}` placeholders |
| `data/tjrv_h2s.csv` | Cumulative hourly readings (all stations, all dates) |
| `docs/index.html` | Generated dashboard served by GitHub Pages |
| `.github/workflows/update.yml` | Hourly GitHub Actions cron job |

## Thresholds

| Color | Range | Meaning |
|-------|-------|---------|
| 🟢 Green | ≤ 5 ppb | No odor |
| 🟡 Yellow | 5–30 ppb | Noticeable odor |
| 🔴 Red | ≥ 30 ppb | Odor advisory |

## Dashboard features

- **Station selector** — switch between IB Civic Center, Nestor (Berry Elementary), and San Ysidro
- **Calendar heatmap** — color-coded daily peak H₂S, most recent month on top
- **Stats strip** — counts of odor advisory / noticeable / no odor / no data days, plus all-time peak
- **View switcher** — toggle between raw day counts, % of days, and % of hourly samples
- **Hourly chart** — average H₂S by hour of day (all-time pattern)
- **Day drill-down** — click any calendar cell to see that day's hourly readings and per-hour stats
- **Theme selector** — Light / Auto / Dark (saved in browser localStorage)

## Known fragility

The Power BI API used here is undocumented. SDAPCD could update their dashboard
at any time, which would break the data fetch. If the hourly job stops producing
new data, check the Actions tab for errors and compare against the current
[SDAPCD dashboard](https://app.powerbigov.us/view?r=eyJrIjoiNzU5YzE4NjAtZTZhMS00ZDg3LTkwMzktYzRiMjkzNDY5ZmU2IiwidCI6IjQ1NjNhZjEzLWMwMjktNDFiMy1iNzRjLTk2NWU4ZWVjOGY5NiJ9).
