#!/usr/bin/env python3
"""
PowderWatch Snow Scraper Agent
===============================
Runs twice daily via GitHub Actions (or cron).
Fetches OnTheSnow ski report pages for 50 US resorts,
parses resort-reported snow data, and writes to snow_data.json + snow_data.csv.

Parser logic (tested against Park City, Mammoth, Mt. Baker, Vail):
  - base_depth:   from "### Base" section
  - snowfall_7d:  sum of day-named columns in "Recent Snowfall" (ignores "24h" column)
  - snowfall_24h: last day-named column value (ignores "24h" column)
  - lifts:        from "with X of Y lifts open" in summary
  - status:       "Closed" if lifts_open == 0, otherwise from page status

Usage:
  python scraper.py              # Run once, update data files
  python scraper.py --json       # Also print JSON to stdout
"""

import re
import json
import csv
import sys
import time
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

OUTPUT_CSV = "snow_data.csv"
OUTPUT_JSON = "snow_data.json"
REQUEST_DELAY = 1.5
MAX_RETRIES = 2
REQUEST_TIMEOUT = 15

RESORTS = [
    {"name":"Vail","state":"CO","lat":39.6403,"lng":-106.3742,"url":"https://www.onthesnow.com/colorado/vail/skireport"},
    {"name":"Breckenridge","state":"CO","lat":39.4817,"lng":-106.0384,"url":"https://www.onthesnow.com/colorado/breckenridge/skireport"},
    {"name":"Keystone","state":"CO","lat":39.6045,"lng":-105.9498,"url":"https://www.onthesnow.com/colorado/keystone/skireport"},
    {"name":"Steamboat","state":"CO","lat":40.457,"lng":-106.804,"url":"https://www.onthesnow.com/colorado/steamboat/skireport"},
    {"name":"Beaver Creek","state":"CO","lat":39.6042,"lng":-106.5165,"url":"https://www.onthesnow.com/colorado/beaver-creek/skireport"},
    {"name":"Winter Park","state":"CO","lat":39.8868,"lng":-105.7625,"url":"https://www.onthesnow.com/colorado/winter-park-resort/skireport"},
    {"name":"Copper Mountain","state":"CO","lat":39.5022,"lng":-106.1497,"url":"https://www.onthesnow.com/colorado/copper-mountain-resort/skireport"},
    {"name":"Snowmass","state":"CO","lat":39.2084,"lng":-106.949,"url":"https://www.onthesnow.com/colorado/snowmass/skireport"},
    {"name":"Telluride","state":"CO","lat":37.9375,"lng":-107.8123,"url":"https://www.onthesnow.com/colorado/telluride/skireport"},
    {"name":"Aspen Mountain","state":"CO","lat":39.1869,"lng":-106.8182,"url":"https://www.onthesnow.com/colorado/aspen-mountain/skireport"},
    {"name":"Aspen Highlands","state":"CO","lat":39.1822,"lng":-106.8552,"url":"https://www.onthesnow.com/colorado/aspen-highlands/skireport"},
    {"name":"Crested Butte","state":"CO","lat":38.8986,"lng":-106.965,"url":"https://www.onthesnow.com/colorado/crested-butte-mountain-resort/skireport"},
    {"name":"Arapahoe Basin","state":"CO","lat":39.6426,"lng":-105.8719,"url":"https://www.onthesnow.com/colorado/arapahoe-basin-ski-area/skireport"},
    {"name":"Loveland","state":"CO","lat":39.68,"lng":-105.8978,"url":"https://www.onthesnow.com/colorado/loveland/skireport"},
    {"name":"Wolf Creek","state":"CO","lat":37.4747,"lng":-106.7934,"url":"https://www.onthesnow.com/colorado/wolf-creek-ski-area/skireport"},
    {"name":"Park City","state":"UT","lat":40.6461,"lng":-111.498,"url":"https://www.onthesnow.com/utah/park-city-mountain-resort/skireport"},
    {"name":"Snowbird","state":"UT","lat":40.583,"lng":-111.6538,"url":"https://www.onthesnow.com/utah/snowbird-ski-summer-resort/skireport"},
    {"name":"Alta","state":"UT","lat":40.5884,"lng":-111.6386,"url":"https://www.onthesnow.com/utah/alta-ski-area/skireport"},
    {"name":"Deer Valley","state":"UT","lat":40.6374,"lng":-111.4783,"url":"https://www.onthesnow.com/utah/deer-valley-resort/skireport"},
    {"name":"Brighton","state":"UT","lat":40.598,"lng":-111.5832,"url":"https://www.onthesnow.com/utah/brighton-resort/skireport"},
    {"name":"Solitude","state":"UT","lat":40.6199,"lng":-111.5927,"url":"https://www.onthesnow.com/utah/solitude-mountain-resort/skireport"},
    {"name":"Snowbasin","state":"UT","lat":41.216,"lng":-111.8569,"url":"https://www.onthesnow.com/utah/snowbasin-resort/skireport"},
    {"name":"Mammoth Mountain","state":"CA","lat":37.6308,"lng":-119.0326,"url":"https://www.onthesnow.com/california/mammoth-mountain-ski-area/skireport"},
    {"name":"Heavenly","state":"CA","lat":38.9353,"lng":-119.94,"url":"https://www.onthesnow.com/california/heavenly-mountain-resort/skireport"},
    {"name":"Palisades Tahoe","state":"CA","lat":39.1968,"lng":-120.2354,"url":"https://www.onthesnow.com/california/palisades-tahoe/skireport"},
    {"name":"Northstar","state":"CA","lat":39.2746,"lng":-120.121,"url":"https://www.onthesnow.com/california/northstar-california/skireport"},
    {"name":"Bear Mountain","state":"CA","lat":34.2275,"lng":-116.859,"url":"https://www.onthesnow.com/california/bear-mountain/skireport"},
    {"name":"Snow Summit","state":"CA","lat":34.2326,"lng":-116.8728,"url":"https://www.onthesnow.com/california/snow-summit/skireport"},
    {"name":"Sugar Bowl","state":"CA","lat":39.3049,"lng":-120.3344,"url":"https://www.onthesnow.com/california/sugar-bowl-resort/skireport"},
    {"name":"Jackson Hole","state":"WY","lat":43.5877,"lng":-110.828,"url":"https://www.onthesnow.com/wyoming/jackson-hole/skireport"},
    {"name":"Grand Targhee","state":"WY","lat":43.7903,"lng":-110.9572,"url":"https://www.onthesnow.com/wyoming/grand-targhee-resort/skireport"},
    {"name":"Big Sky","state":"MT","lat":45.2833,"lng":-111.4014,"url":"https://www.onthesnow.com/montana/big-sky-resort/skireport"},
    {"name":"Whitefish","state":"MT","lat":48.4816,"lng":-114.3582,"url":"https://www.onthesnow.com/montana/whitefish-mountain-resort/skireport"},
    {"name":"Sun Valley","state":"ID","lat":43.6977,"lng":-114.3514,"url":"https://www.onthesnow.com/idaho/sun-valley/skireport"},
    {"name":"Schweitzer","state":"ID","lat":48.3678,"lng":-116.623,"url":"https://www.onthesnow.com/idaho/schweitzer-mountain-resort/skireport"},
    {"name":"Crystal Mountain","state":"WA","lat":46.9282,"lng":-121.5045,"url":"https://www.onthesnow.com/washington/crystal-mountain/skireport"},
    {"name":"Stevens Pass","state":"WA","lat":47.7448,"lng":-121.089,"url":"https://www.onthesnow.com/washington/stevens-pass-resort/skireport"},
    {"name":"Mt. Baker","state":"WA","lat":48.8566,"lng":-121.6647,"url":"https://www.onthesnow.com/washington/mt-baker/skireport"},
    {"name":"Mt. Bachelor","state":"OR","lat":43.9792,"lng":-121.6886,"url":"https://www.onthesnow.com/oregon/mt-bachelor/skireport"},
    {"name":"Mt. Hood Meadows","state":"OR","lat":45.3307,"lng":-121.6649,"url":"https://www.onthesnow.com/oregon/mt-hood-meadows/skireport"},
    {"name":"Alyeska","state":"AK","lat":60.9604,"lng":-149.0987,"url":"https://www.onthesnow.com/alaska/alyeska-resort/skireport"},
    {"name":"Killington","state":"VT","lat":43.6045,"lng":-72.8201,"url":"https://www.onthesnow.com/vermont/killington-resort/skireport"},
    {"name":"Okemo","state":"VT","lat":43.4012,"lng":-72.7173,"url":"https://www.onthesnow.com/vermont/okemo-mountain-resort/skireport"},
    {"name":"Stowe","state":"VT","lat":44.5303,"lng":-72.7814,"url":"https://www.onthesnow.com/vermont/stowe-mountain-resort/skireport"},
    {"name":"Sugarbush","state":"VT","lat":44.1359,"lng":-72.9013,"url":"https://www.onthesnow.com/vermont/sugarbush/skireport"},
    {"name":"Stratton","state":"VT","lat":43.1134,"lng":-72.9079,"url":"https://www.onthesnow.com/vermont/stratton-mountain/skireport"},
    {"name":"Jay Peak","state":"VT","lat":44.9267,"lng":-72.529,"url":"https://www.onthesnow.com/vermont/jay-peak/skireport"},
    {"name":"Sunday River","state":"ME","lat":44.4733,"lng":-70.8567,"url":"https://www.onthesnow.com/maine/sunday-river/skireport"},
    {"name":"Sugarloaf","state":"ME","lat":45.0314,"lng":-70.3131,"url":"https://www.onthesnow.com/maine/sugarloaf/skireport"},
    {"name":"Snowshoe","state":"WV","lat":38.4073,"lng":-79.994,"url":"https://www.onthesnow.com/west-virginia/snowshoe-mountain-resort/skireport"},
]


def fetch_page(url, attempt=1):
    req = Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; PowderWatch/1.0; snow conditions aggregator)",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    })
    try:
        with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except (URLError, HTTPError, TimeoutError) as e:
        if attempt <= MAX_RETRIES:
            time.sleep(2)
            return fetch_page(url, attempt + 1)
        print(f"  FAILED: {url} ({e})", file=sys.stderr)
        return None


def parse_snow_report(html, resort_name):
    d = {
        "name": resort_name,
        "base_depth": 0, "mid_depth": 0, "summit_depth": 0,
        "surface_condition": "",
        "snowfall_7d": 0, "snowfall_24h": 0,
        "lifts_open": 0, "lifts_total": 0,
        "runs_open": 0, "runs_total": 0,
        "status": "Unknown", "last_updated": "",
    }
    if not html:
        return d

    # Status + last updated
    m = re.search(r':\s*(Open|Closed)\s*Snow Report Last Updated:\s*([A-Za-z]+ \d+)', html)
    if m:
        d["status"] = m.group(1)
        d["last_updated"] = m.group(2)

    # BASE DEPTH: "### Base\n\n63""
    m = re.search(r'###\s*Base\s*\n+\s*(\d+)"', html)
    if m:
        d["base_depth"] = int(m.group(1))

    # MID DEPTH
    m = re.search(r'###\s*Mid\s*Mountain\s*\n+\s*(\d+)"', html)
    if m:
        d["mid_depth"] = int(m.group(1))

    # SUMMIT DEPTH
    m = re.search(r'###\s*Summit\s*\n+\s*(\d+)"', html)
    if m:
        d["summit_depth"] = int(m.group(1))

    # SURFACE CONDITION
    m = re.search(r'###\s*Base\s*\n+\s*\d+"\s*\n+\s*([A-Za-z][A-Za-z /]+)', html)
    if m:
        cond = m.group(1).strip().split("\n")[0].strip()
        if cond and cond not in ("N", "N.A.", "N/A", "Depth") and not cond.startswith("###"):
            d["surface_condition"] = cond

    # LIFTS: "with X of Y lifts open"
    m = re.search(r'with\s+(\d+)\s+of\s+(\d+)\s+lifts\s+open', html)
    if m:
        d["lifts_open"] = int(m.group(1))
        d["lifts_total"] = int(m.group(2))

    # RUNS
    m = re.search(r'Runs\s+Open\s*\n+\s*(\d+)/(\d+)\s+open', html)
    if m:
        d["runs_open"] = int(m.group(1))
        d["runs_total"] = int(m.group(2))

    # RECENT SNOWFALL
    # Count day-name headers (Mon-Sun) to know how many day columns exist.
    # Use only those columns. Ignore "24h" column entirely.
    # 24h = last day column. 7d = sum of all day columns.
    section = re.search(r'###\s*Recent\s+Snowfall(.*?)###\s*Forecasted\s+Snow', html, re.DOTALL)
    if section:
        text = section.group(1)
        headers = re.findall(r'(Mon|Tue|Wed|Thu|Fri|Sat|Sun)', text)
        amounts = re.findall(r'(\d+(?:\.\d+)?)"', text)
        if headers and amounts:
            num_days = len(headers)
            day_amounts = [float(x) for x in amounts[:num_days]]
            if day_amounts:
                d["snowfall_7d"] = round(sum(day_amounts), 1)
                d["snowfall_24h"] = day_amounts[-1]

    # CLOSED: if 0 lifts are open, resort is closed regardless of page status
    if d["lifts_open"] == 0 and d["lifts_total"] > 0:
        d["status"] = "Closed"

    return d


def scrape_all():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    results = []
    total = len(RESORTS)
    for i, resort in enumerate(RESORTS):
        print(f"[{i+1}/{total}] {resort['name']}...", file=sys.stderr, end=" ", flush=True)
        html = fetch_page(resort["url"])
        d = parse_snow_report(html, resort["name"])
        d.update({"state": resort["state"], "lat": resort["lat"], "lng": resort["lng"],
                   "url": resort["url"], "scraped_at": ts})
        results.append(d)
        print(f"Base:{d['base_depth']}\" 7d:{d['snowfall_7d']}\" 24h:{d['snowfall_24h']}\" "
              f"Lifts:{d['lifts_open']}/{d['lifts_total']} {d['status']}", file=sys.stderr)
        if i < total - 1:
            time.sleep(REQUEST_DELAY)
    return results


def write_csv(results):
    fields = ["name","state","lat","lng","base_depth","mid_depth","summit_depth",
              "surface_condition","snowfall_7d","snowfall_24h","lifts_open","lifts_total",
              "runs_open","runs_total","status","last_updated","scraped_at","url"]
    with open(OUTPUT_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in results:
            w.writerow({k: r.get(k, "") for k in fields})
    print(f"\nWrote {len(results)} resorts to {OUTPUT_CSV}", file=sys.stderr)


def write_json(results):
    with open(OUTPUT_JSON, "w") as f:
        json.dump({"resorts": results, "generated_at": results[0]["scraped_at"] if results else "",
                    "resort_count": len(results)}, f, indent=2)
    print(f"Wrote {len(results)} resorts to {OUTPUT_JSON}", file=sys.stderr)


if __name__ == "__main__":
    print(f"PowderWatch Scraper — {len(RESORTS)} resorts", file=sys.stderr)
    print(f"Estimated time: ~{int(len(RESORTS) * (REQUEST_DELAY + 1))}s\n", file=sys.stderr)
    results = scrape_all()
    write_csv(results)
    write_json(results)
    ok = sum(1 for r in results if r["base_depth"] > 0 or r["status"] != "Unknown")
    print(f"\nDone: {ok}/{len(results)} resorts returned data", file=sys.stderr)
    if "--json" in sys.argv:
        print(json.dumps(results, indent=2))
