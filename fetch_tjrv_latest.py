"""
fetch_tjrv_latest.py
====================
Fetches H2S data directly from the SDAPCD Power BI API with correct pagination.

Usage:
    python fetch_tjrv_latest.py              # fetches last 2 days
    python fetch_tjrv_latest.py --days 60    # fetches last 60 days
    python fetch_tjrv_latest.py --days 60 --out latest.csv
    python fetch_tjrv_latest.py --days 2 --out data/tjrv_h2s.csv --append
"""

import json, csv, sys, argparse, os
import urllib.request, urllib.error
from datetime import datetime, timedelta

URL = "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net/public/reports/querydata?synchronous=true"

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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=2, help="Days back to fetch (min 2)")
    ap.add_argument("--out", default="tjrv_latest.csv")
    ap.add_argument("--append", action="store_true", help="Merge new rows into existing CSV")
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
