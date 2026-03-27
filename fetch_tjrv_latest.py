"""
fetch_tjrv_latest.py
====================
Fetches H2S data from two sources and merges them:
  1. SDAPCD Power BI API  — historical data, refreshed every ~3 hours
  2. SDAPCD web page      — today's data, refreshed more frequently

Usage:
    python fetch_tjrv_latest.py              # fetches last 2 days + today's web data
    python fetch_tjrv_latest.py --days 60    # fetches last 60 days + today's web data
    python fetch_tjrv_latest.py --days 2 --out data/tjrv_h2s.csv --append
    python fetch_tjrv_latest.py --no-web     # skip the web scrape, Power BI only
"""

import json, csv, sys, argparse, os
import urllib.request, urllib.error
from datetime import datetime, timedelta
from html.parser import HTMLParser

URL = "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net/public/reports/querydata?synchronous=true"
WEB_URL = "https://airquality.sdapcd.org/air/data/h2s.htm"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://app.powerbigov.us",
    "Referer": "https://app.powerbigov.us/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "X-PowerBI-ResourceKey": "759c1860-e6a1-4d87-9039-c4b293469fe6",
}

LOCATIONS = ["IB CIVIC CTR", "NESTOR - BES", "SAN YSIDRO"]


def build_commands(days_back, restart_tokens=None):
    window = {"Count": 500}
    if restart_tokens:
        window["RestartTokens"] = restart_tokens

    return [{
        "SemanticQueryDataShapeCommand": {
            "Query": {
                "Version": 2,
                "From": [{"Name": "s", "Entity": "Sharepoint-Data", "Type": 0}],
                "Select": [
                    {"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "DATE"},
                     "Name": "Sharepoint-Data.DATE", "NativeReferenceName": "Date1"},
                    {"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "TIME"},
                     "Name": "Sharepoint-Data.TIME", "NativeReferenceName": "Time1"},
                    {"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Location"},
                     "Name": "Sharepoint-Data.Location", "NativeReferenceName": "Location1"},
                    {"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "ppB"},
                     "Name": "Sum(Sharepoint-Data.ppB)", "NativeReferenceName": "ppB"},
                ],
                "Where": [
                    {
                        "Condition": {
                            "Between": {
                                "Expression": {"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "DATETIME"}},
                                "LowerBound": {
                                    "DateSpan": {
                                        "Expression": {
                                            "DateAdd": {
                                                "Expression": {
                                                    "DateAdd": {"Expression": {"Now": {}}, "Amount": 1, "TimeUnit": 0}
                                                },
                                                "Amount": -days_back,
                                                "TimeUnit": 0
                                            }
                                        },
                                        "TimeUnit": 0
                                    }
                                },
                                "UpperBound": {
                                    "DateSpan": {"Expression": {"Now": {}}, "TimeUnit": 0}
                                }
                            }
                        }
                    },
                    {
                        "Condition": {
                            "In": {
                                "Expressions": [{"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Location"}}],
                                "Values": [
                                    [{"Literal": {"Value": "'NESTOR - BES'"}}],
                                    [{"Literal": {"Value": "'SAN YSIDRO'"}}],
                                    [{"Literal": {"Value": "'IB CIVIC CTR'"}}],
                                ]
                            }
                        }
                    }
                ],
            },
            "Binding": {
                "Primary": {"Groupings": [{"Projections": [0, 1, 2, 3], "ShowItemsWithNoData": [0, 1, 2, 3]}]},
                "DataReduction": {"DataVolume": 3, "Primary": {"Window": window}},
                "Version": 1,
            },
            "ExecutionMetricsKind": 1,
        }
    }]


def build_body(days_back, restart_tokens=None):
    commands = build_commands(days_back, restart_tokens)
    cache_commands = build_commands(days_back, restart_tokens=None)
    cache_key = json.dumps({"Commands": cache_commands}, separators=(',', ':'))
    return {
        "version": "1.0.0",
        "queries": [{
            "Query": {"Commands": commands},
            "CacheKey": cache_key,
            "QueryId": "",
            "ApplicationContext": {
                "DatasetId": "ee1601d9-567f-497a-a73a-3ee5cf9f38d7",
                "Sources": [{"ReportId": "f7f5b839-2401-4567-a0a7-7ed465bcd32d",
                             "VisualId": "149cfb7183563c03b44d"}]
            }
        }],
        "cancelQueries": [],
        "modelId": 873247,
    }


SDAPCD_URL = "https://www.sdapcd.org/content/sdapcd/about/tj-river-valley/tjrv-air-quality-monitoring.html"

def _key_warning():
    print()
    print("⚠️  The Power BI resource key may have changed or been revoked.")
    print(f"   Check the SDAPCD monitoring page for an updated embed URL:")
    print(f"   {SDAPCD_URL}")
    print(f"   The current key in this script is: {HEADERS['X-PowerBI-ResourceKey']}")
    print()


def fetch(body):
    payload = json.dumps(body, separators=(',', ':')).encode("utf-8")
    req = urllib.request.Request(URL, data=payload, headers=HEADERS, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            if resp.headers.get("Content-Encoding") == "gzip":
                import gzip; raw = gzip.decompress(raw)
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8", errors="replace")
        print(f"HTTP {e.code}: {e.reason}")
        if e.code in (401, 403):
            _key_warning()
        else:
            print(err[:500])
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"Network error: {e.reason}")
        sys.exit(1)


def parse_dsr(dsr_data, locations=None):
    if locations is None:
        locations = dsr_data.get("ValueDicts", {}).get("D0", [])
    records = []
    last_full_row = None
    has_more = False

    for ds in dsr_data.get("DS", []):
        ds_locs = ds.get("ValueDicts", {}).get("D0", locations)
        for ph in ds.get("PH", []):
            rows = ph.get("DM0", [])
            if len(rows) == 500:
                has_more = True

            cur_date_ms = cur_time = cur_loc = cur_ppb = None
            for row in rows:
                C = row.get("C", []); R = row.get("R", 0)
                null_mask = row.get("\u00d8", 0); ci = 0

                def nv():
                    nonlocal ci
                    v = C[ci] if ci < len(C) else None; ci += 1; return v

                if not (R & 1):
                    v = nv()
                    if v is not None: cur_date_ms = v
                if not (R & 2):
                    v = nv()
                    if v is not None: cur_time = v
                if not (R & 4):
                    if null_mask & 4: nv(); cur_loc = None
                    else:
                        v = nv()
                        if v is not None: cur_loc = v
                if not (R & 8):
                    if null_mask & 8: nv(); cur_ppb = None
                    else: cur_ppb = nv()

                if cur_date_ms is not None and cur_time is not None:
                    d = datetime(1970, 1, 1) + timedelta(milliseconds=cur_date_ms)
                    try: t = cur_time.split("T")[1][:5]
                    except: t = str(cur_time)
                    loc = ds_locs[cur_loc] if cur_loc is not None and cur_loc < len(ds_locs) else f"loc_{cur_loc}"
                    records.append({"date": d.strftime("%Y-%m-%d"), "time": t,
                                    "datetime": f"{d.strftime('%Y-%m-%d')} {t}",
                                    "location": loc,
                                    "ppb_h2s": "" if cur_ppb is None else cur_ppb})
                    last_full_row = [cur_date_ms, cur_time, cur_loc, cur_ppb]

    return records, last_full_row if has_more else None, ds_locs


def make_restart_tokens(last_row, ds_locs):
    date_ms, time_str, loc_idx, ppb = last_row

    d = datetime(1970, 1, 1) + timedelta(milliseconds=date_ms)
    date_token = f"datetime'{d.strftime('%Y-%m-%dT%H:%M:%S')}'"

    time_token = f"datetime'{time_str}'" if 'T' in time_str else f"datetime'1899-12-30T{time_str}:00'"

    loc_name = ds_locs[loc_idx] if loc_idx is not None and loc_idx < len(ds_locs) else ""
    loc_token = f"'{loc_name}'"

    ppb_token = f"{ppb}D" if ppb is not None else "0D"

    return [[date_token, time_token, loc_token, ppb_token]]


def fetch_web():
    """
    Scrape today's H2S readings from the SDAPCD daily parameter report page.
    Returns a list of records in the same format as the Power BI fetcher.
    Non-numeric values (M = maintenance, blank = not yet posted) are skipped.
    The 3 summary columns (Avg, Max, Hr. of) at the right are ignored.
    """
    print(f"Fetching web page: {WEB_URL}")
    req = urllib.request.Request(
        WEB_URL,
        headers={"User-Agent": "Mozilla/5.0 (compatible; sdapcd-scraper/1.0)"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as e:
        print(f"  Web fetch error: {e.reason} — skipping web source")
        return []

    # ── Parse all <td> and <tr> cells from the HTML ───────────────────────────
    class TableParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.rows = []
            self._row = []
            self._in_td = False
            self._cell = ""

        def handle_starttag(self, tag, attrs):
            if tag == "tr":
                self._row = []
            elif tag in ("td", "th"):
                self._in_td = True
                self._cell = ""

        def handle_endtag(self, tag):
            if tag in ("td", "th"):
                # Normalise non-breaking spaces and collapse whitespace
                cell = self._cell.replace("\xa0", " ").strip()
                self._row.append(cell)
                self._in_td = False
                self._cell = ""
            elif tag == "tr":
                if self._row:
                    self.rows.append(self._row)

        def handle_data(self, data):
            if self._in_td:
                self._cell += data

        def handle_entityref(self, name):
            if self._in_td and name == "nbsp":
                self._cell += " "

    parser = TableParser()
    parser.feed(html)

    # ── Extract the date from the page ────────────────────────────────────────
    page_date = None
    for row in parser.rows:
        for cell in row:
            if cell and "/" in cell:
                try:
                    page_date = datetime.strptime(cell, "%m/%d/%Y").strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue
        if page_date:
            break

    if not page_date:
        print("  Could not find date on web page — skipping web source")
        return []

    print(f"  Web page date: {page_date}")

    # ── Find the header row containing hour numbers 0..23 ─────────────────────
    # Locate the row with a run of consecutive integers "0","1","2",...
    # hour_col_start is the column index of "0" in that row.
    hour_row_idx   = None
    hour_col_start = None

    for i, row in enumerate(parser.rows):
        for j, cell in enumerate(row):
            if cell == "0" and all(
                j + h < len(row) and row[j + h] == str(h)
                for h in range(8)
            ):
                hour_row_idx   = i
                hour_col_start = j
                break
        if hour_row_idx is not None:
            break

    if hour_row_idx is None:
        print("  Could not find hour header row — skipping web source")
        return []

    # ── Parse data rows that follow the header ────────────────────────────────
    # The HTML uses merged cells so <td> counts differ per row — we cannot use
    # fixed column offsets derived from the header row.
    #
    # Instead, for each row we:
    #   1. Find the station name: the last non-numeric, non-header cell that
    #      looks like a proper name (len >= 4 and contains a space).
    #   2. Treat the columns immediately following it as hour values 0, 1, 2...
    #   3. Stop 3 cols from the end of the row to exclude Avg/Max/Hr.of summary.
    #   4. Skip blanks (not yet posted) and non-numeric status codes (M, ND…).
    #
    # Station names are taken exactly as-is — new stations are picked up
    # automatically without any code change.

    SKIP = {'', 'parameter', 'sitename', '07 h2s ppb', 'summary',
            'avg', 'max', 'hr. of', 'hr.of'}

    def is_station_name(s):
        """Station names are multi-word (contain a space) and at least 4 chars."""
        return len(s) >= 4 and ' ' in s

    records = []
    current_loc = None

    for row in parser.rows[hour_row_idx + 1:]:
        # Find station name and where data starts
        station    = None
        data_start = None
        for j, cell in enumerate(row):
            if cell.lower() in SKIP:
                continue
            try:
                float(cell)
                # Numeric: data starts here (at the hour-0 column for this row)
                if station is not None and data_start is None:
                    data_start = j
                # Continue — subsequent numeric cells are more data
            except ValueError:
                if cell and is_station_name(cell):
                    station    = cell
                    data_start = None  # reset; data follows after this

        if station is not None:
            current_loc = station

        if current_loc is None or data_start is None:
            continue  # no data on this row

        # Read up to 24 hour values; exclude last 3 cols (Avg, Max, Hr. of)
        data_end = len(row) - 3
        hour = 0
        for col in range(data_start, data_end):
            if hour >= 24:
                break
            val = row[col]
            if not val:
                hour += 1  # blank = hour not yet posted, advance hour counter
                continue
            try:
                ppb = float(val)
                time_str = f"{hour:02d}:00"
                records.append({
                    "date":     page_date,
                    "time":     time_str,
                    "datetime": f"{page_date} {time_str}",
                    "location": current_loc,
                    "ppb_h2s":  ppb,
                })
            except ValueError:
                pass  # M, ND, or other status code — skip but advance hour
            hour += 1

    print(f"  Web page: {len(records)} records ({page_date})")
    return records


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=2, help="Days back to fetch (min 2)")
    ap.add_argument("--out", default="tjrv_latest.csv")
    ap.add_argument("--append", action="store_true", help="Merge new rows into existing CSV")
    ap.add_argument("--no-web", action="store_true", help="Skip the web page scrape, Power BI only")
    args = ap.parse_args()

    if args.days < 2:
        print("Warning: --days minimum is 2 due to DateSpan truncation, using 2")
        args.days = 2

    print(f"Fetching last {args.days} days...")
    all_records = []; restart_tokens = None; page = 0; ds_locs = LOCATIONS

    while True:
        page += 1
        resp = fetch(build_body(days_back=args.days, restart_tokens=restart_tokens))
        records = []; last_row = None; has_more = False

        for result in resp.get("results", []):
            try:
                # Check for error response embedded in the result
                if "error" in result:
                    code = result["error"].get("code", "")
                    msg  = result["error"].get("message", "")
                    print(f"  API error — code: {code!r}  message: {msg!r}")
                    if any(k in (code + msg).lower() for k in ("unauthorized", "forbidden", "invalid key", "token")):
                        _key_warning()
                    sys.exit(1)
                dsr = result["result"]["data"]["dsr"]
                recs, last_row_maybe, locs = parse_dsr(dsr)
                records.extend(recs)
                if last_row_maybe:
                    last_row = last_row_maybe
                    has_more = True
                    ds_locs = locs
            except (KeyError, TypeError) as ex:
                print(f"  Parse error: {ex}")
                print(f"  Unexpected response shape — the API may have changed.")
                print(f"  Raw result keys: {list(result.keys()) if isinstance(result, dict) else type(result)}")

        print(f"  Page {page}: {len(records)} records", end="")
        all_records.extend(records)

        if has_more and last_row:
            restart_tokens = make_restart_tokens(last_row, ds_locs)
            print(f" → next page from {restart_tokens[0][0]}")
        else:
            print(" → done")
            break

    if not all_records:
        print("No records returned — the API responded but contained no data.")
        _key_warning()
        sys.exit(1)

    # ── Fetch today's data from the web page and merge ────────────────────────
    if not args.no_web:
        web_records = fetch_web()
        all_records.extend(web_records)
    else:
        print("Skipping web page scrape (--no-web)")

    # ── Deduplicate: Power BI wins on conflict (it comes first) ───────────────
    seen = set(); unique = []
    for r in all_records:
        k = (r["date"], r["time"], r["location"])
        if k not in seen: seen.add(k); unique.append(r)
    unique.sort(key=lambda r: (r["date"], r["time"], r["location"]))

    if args.append and os.path.exists(args.out):
        existing = []
        with open(args.out, newline="", encoding="utf-8") as f:
            existing = list(csv.DictReader(f))
        existing_keys = {(r["date"], r["time"], r["location"]) for r in existing}
        new_rows = [r for r in unique if (r["date"], r["time"], r["location"]) not in existing_keys]
        merged = sorted(existing + new_rows, key=lambda r: (r["date"], r["time"], r["location"]))
        print(f"  Existing rows : {len(existing)}")
        print(f"  New rows added: {len(new_rows)}")
        unique = merged

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["date","time","datetime","location","ppb_h2s"])
        w.writeheader(); w.writerows(unique)

    locs = sorted(set(r["location"] for r in unique))
    dates = sorted(set(r["date"] for r in unique))
    print(f"\n✓ {len(unique)} total records → {args.out}")
    print(f"  Locations : {locs}")
    if dates:
        print(f"  Date range: {dates[0]} → {dates[-1]}  ({len(dates)} days)")

if __name__ == "__main__":
    main()