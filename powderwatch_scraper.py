#!/usr/bin/env python3
"""
PowderWatch Snow Scraper — visual text parsing only
"""

import re
import json
import csv
import sys
import time
import html as html_lib
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

OUTPUT_CSV  = "snow_data.csv"
OUTPUT_JSON = "snow_data.json"
REQUEST_DELAY   = 2.0
MAX_RETRIES     = 2
REQUEST_TIMEOUT = 20

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

RESORTS = [
    # Colorado
    {"name":"Vail","state":"CO","lat":39.6403,"lng":-106.3742,"url":"https://www.onthesnow.com/colorado/vail/skireport"},
    {"name":"Breckenridge","state":"CO","lat":39.4817,"lng":-106.0384,"url":"https://www.onthesnow.com/colorado/breckenridge/skireport"},
    {"name":"Keystone","state":"CO","lat":39.6045,"lng":-105.9498,"url":"https://www.onthesnow.com/colorado/keystone/skireport"},
    {"name":"Steamboat","state":"CO","lat":40.457,"lng":-106.804,"url":"https://www.onthesnow.com/colorado/steamboat/skireport"},
    {"name":"Beaver Creek","state":"CO","lat":39.6042,"lng":-106.5165,"url":"https://www.onthesnow.com/colorado/beaver-creek/skireport"},
    {"name":"Winter Park","state":"CO","lat":39.8868,"lng":-105.7625,"url":"https://www.onthesnow.com/colorado/winter-park-resort/skireport"},
    {"name":"Copper Mountain","state":"CO","lat":39.5022,"lng":-106.1497,"url":"https://www.onthesnow.com/colorado/copper-mountain-resort/skireport"},
    {"name":"Aspen/Snowmass","state":"CO","lat":39.2084,"lng":-106.949,"url":"https://www.onthesnow.com/colorado/aspen-snowmass/skireport"},
    {"name":"Telluride","state":"CO","lat":37.9375,"lng":-107.8123,"url":"https://www.onthesnow.com/colorado/telluride/skireport"},
    {"name":"Crested Butte","state":"CO","lat":38.8986,"lng":-106.965,"url":"https://www.onthesnow.com/colorado/crested-butte-mountain-resort/skireport"},
    {"name":"Arapahoe Basin","state":"CO","lat":39.6426,"lng":-105.8719,"url":"https://www.onthesnow.com/colorado/arapahoe-basin-ski-area/skireport"},
    {"name":"Loveland","state":"CO","lat":39.68,"lng":-105.8978,"url":"https://www.onthesnow.com/colorado/loveland/skireport"},
    {"name":"Wolf Creek","state":"CO","lat":37.4747,"lng":-106.7934,"url":"https://www.onthesnow.com/colorado/wolf-creek-ski-area/skireport"},
    # Utah
    {"name":"Park City","state":"UT","lat":40.6461,"lng":-111.498,"url":"https://www.onthesnow.com/utah/park-city-mountain-resort/skireport"},
    {"name":"Snowbird","state":"UT","lat":40.583,"lng":-111.6538,"url":"https://www.onthesnow.com/utah/snowbird/skireport"},
    {"name":"Alta","state":"UT","lat":40.5884,"lng":-111.6386,"url":"https://www.onthesnow.com/utah/alta-ski-area/skireport"},
    {"name":"Deer Valley","state":"UT","lat":40.6374,"lng":-111.4783,"url":"https://www.onthesnow.com/utah/deer-valley-resort/skireport"},
    {"name":"Brighton","state":"UT","lat":40.598,"lng":-111.5832,"url":"https://www.onthesnow.com/utah/brighton-resort/skireport"},
    {"name":"Solitude","state":"UT","lat":40.6199,"lng":-111.5927,"url":"https://www.onthesnow.com/utah/solitude-mountain-resort/skireport"},
    {"name":"Snowbasin","state":"UT","lat":41.216,"lng":-111.8569,"url":"https://www.onthesnow.com/utah/snowbasin/skireport"},
    # California
    {"name":"Mammoth Mountain","state":"CA","lat":37.6308,"lng":-119.0326,"url":"https://www.onthesnow.com/california/mammoth-mountain-ski-area/skireport"},
    {"name":"Heavenly","state":"CA","lat":38.9353,"lng":-119.94,"url":"https://www.onthesnow.com/california/heavenly-mountain-resort/skireport"},
    {"name":"Palisades Tahoe","state":"CA","lat":39.1968,"lng":-120.2354,"url":"https://www.onthesnow.com/california/palisades-tahoe/skireport"},
    {"name":"Northstar","state":"CA","lat":39.2746,"lng":-120.121,"url":"https://www.onthesnow.com/california/northstar-california/skireport"},
    {"name":"Bear Mountain","state":"CA","lat":34.2275,"lng":-116.859,"url":"https://www.onthesnow.com/california/bear-mountain/skireport"},
    {"name":"Snow Summit","state":"CA","lat":34.2326,"lng":-116.8728,"url":"https://www.onthesnow.com/california/snow-summit/skireport"},
    {"name":"Sugar Bowl","state":"CA","lat":39.3049,"lng":-120.3344,"url":"https://www.onthesnow.com/california/sugar-bowl-resort/skireport"},
    # Wyoming
    {"name":"Jackson Hole","state":"WY","lat":43.5877,"lng":-110.828,"url":"https://www.onthesnow.com/wyoming/jackson-hole/skireport"},
    {"name":"Grand Targhee","state":"WY","lat":43.7903,"lng":-110.9572,"url":"https://www.onthesnow.com/wyoming/grand-targhee-resort/skireport"},
    # Montana
    {"name":"Big Sky","state":"MT","lat":45.2833,"lng":-111.4014,"url":"https://www.onthesnow.com/montana/big-sky-resort/skireport"},
    {"name":"Whitefish","state":"MT","lat":48.4816,"lng":-114.3582,"url":"https://www.onthesnow.com/montana/whitefish-mountain-resort/skireport"},
    # Idaho
    {"name":"Sun Valley","state":"ID","lat":43.6977,"lng":-114.3514,"url":"https://www.onthesnow.com/idaho/sun-valley/skireport"},
    {"name":"Schweitzer","state":"ID","lat":48.3678,"lng":-116.623,"url":"https://www.onthesnow.com/idaho/schweitzer/skireport"},
    # Washington
    {"name":"Crystal Mountain","state":"WA","lat":46.9282,"lng":-121.5045,"url":"https://www.onthesnow.com/washington/crystal-mountain-wa/skireport"},
    {"name":"Stevens Pass","state":"WA","lat":47.7448,"lng":-121.089,"url":"https://www.onthesnow.com/washington/stevens-pass-resort/skireport"},
    {"name":"Mt. Baker","state":"WA","lat":48.8566,"lng":-121.6647,"url":"https://www.onthesnow.com/washington/mt-baker/skireport"},
    # Oregon
    {"name":"Mt. Bachelor","state":"OR","lat":43.9792,"lng":-121.6886,"url":"https://www.onthesnow.com/oregon/mt-bachelor/skireport"},
    {"name":"Mt. Hood Meadows","state":"OR","lat":45.3307,"lng":-121.6649,"url":"https://www.onthesnow.com/oregon/mt-hood-meadows/skireport"},
    # Alaska
    {"name":"Alyeska","state":"AK","lat":60.9604,"lng":-149.0987,"url":"https://www.onthesnow.com/alaska/alyeska-resort/skireport"},
    # Vermont
    {"name":"Killington","state":"VT","lat":43.6045,"lng":-72.8201,"url":"https://www.onthesnow.com/vermont/killington-resort/skireport"},
    {"name":"Okemo","state":"VT","lat":43.4012,"lng":-72.7173,"url":"https://www.onthesnow.com/vermont/okemo-mountain-resort/skireport"},
    {"name":"Stowe","state":"VT","lat":44.5303,"lng":-72.7814,"url":"https://www.onthesnow.com/vermont/stowe-mountain-resort/skireport"},
    {"name":"Sugarbush","state":"VT","lat":44.1359,"lng":-72.9013,"url":"https://www.onthesnow.com/vermont/sugarbush/skireport"},
    {"name":"Stratton","state":"VT","lat":43.1134,"lng":-72.9079,"url":"https://www.onthesnow.com/vermont/stratton-mountain/skireport"},
    {"name":"Jay Peak","state":"VT","lat":44.9267,"lng":-72.529,"url":"https://www.onthesnow.com/vermont/jay-peak/skireport"},
    # Maine
    {"name":"Sunday River","state":"ME","lat":44.4733,"lng":-70.8567,"url":"https://www.onthesnow.com/maine/sunday-river/skireport"},
    {"name":"Sugarloaf","state":"ME","lat":45.0314,"lng":-70.3131,"url":"https://www.onthesnow.com/maine/sugarloaf/skireport"},
    # West Virginia
    {"name":"Snowshoe","state":"WV","lat":38.4073,"lng":-79.994,"url":"https://www.onthesnow.com/west-virginia/snowshoe-mountain-resort/skireport"},
]


def fetch_page(url, attempt=1):
    ua = USER_AGENTS[(attempt - 1) % len(USER_AGENTS)]
    req = Request(url, headers={
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "identity",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    try:
        with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except (URLError, HTTPError, TimeoutError) as e:
        if attempt <= MAX_RETRIES:
            time.sleep(3 * attempt)
            return fetch_page(url, attempt + 1)
        print(f"  FAILED: {url} ({e})", file=sys.stderr)
        return None


def strip_html(html):
    text = re.sub(r'<(script|style)[^>]*>.*?</\1>', ' ', html, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html_lib.unescape(text)
    return re.sub(r'\s+', ' ', text).strip()


def parse_snowfall_section(text):
    """
    Parse the Recent Snowfall table from OnTheSnow.
    The table has two rows: day labels then inch amounts.
    Labels are day abbreviations (Mon Tue Wed etc) and/or period labels (24h 48h 7d season).

    OnTheSnow layout (stripped text) looks like:
      "Recent Snowfall Mon Tue Wed Thu Fri Sat Sun 24h 7d Season
       0" 0" 0" 1" 2" 0" 0" 3" 3" 28""

    Strategy:
    - Find "Recent Snowfall" anchor
    - Stop before "Forecast" or "Snow Depths" to avoid bleed
    - Extract all labels and all inch-values in that window
    - Pair them up positionally
    - Sum Mon-Sun for 7d; take the explicit 24h label if present,
      otherwise use today's most-recent day column only
    """
    snow_7d = 0.0
    snow_24h = 0.0

    idx = text.lower().find("recent snowfall")
    if idx == -1:
        return snow_7d, snow_24h

    # Cap the window before forecast or snow depths sections
    end = len(text)
    for stopper in ["forecast", "snow depths", "trail report", "lift report"]:
        s = text.lower().find(stopper, idx + 1)
        if s != -1 and s < end:
            end = s

    sub = text[idx: min(idx + 600, end)]

    # Extract labels (day abbreviations + period labels)
    DAY_ABBREVS = {"mon", "tue", "wed", "thu", "fri", "sat", "sun"}
    PERIOD_LABELS = {"24h", "48h", "72h", "7d", "season"}

    label_pattern = re.compile(
        r'\b(Mon|Tue|Wed|Thu|Fri|Sat|Sun|24h|48h|72h|7d|Season)\b',
        re.IGNORECASE
    )
    labels = [m.group(1).lower() for m in label_pattern.finditer(sub)]

    # Extract inch values in order
    values = [float(m.group(1)) for m in re.finditer(r'(\d+(?:\.\d+)?)\s*"', sub)]

    if not labels or not values:
        return snow_7d, snow_24h

    # Pair labels with values positionally
    pairs = list(zip(labels, values))

    # Pull explicit labeled values first
    for label, val in pairs:
        if label == "7d":
            snow_7d = val
        elif label == "24h":
            snow_24h = val

    # If 7d not explicitly labeled, sum all day-of-week columns
    if snow_7d == 0.0:
        day_vals = [val for label, val in pairs if label in DAY_ABBREVS]
        if day_vals:
            snow_7d = round(sum(day_vals), 1)

    # If 24h not explicitly labeled, take only the LAST day-of-week column
    # (most recent day), NOT the first (which could be 7 days ago)
    if snow_24h == 0.0:
        day_pairs = [(label, val) for label, val in pairs if label in DAY_ABBREVS]
        if day_pairs:
            snow_24h = day_pairs[-1][1]  # last = most recent day

    return snow_7d, snow_24h


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

    text = strip_html(html)

    # ── Status + last updated ─────────────────────────────────────────────────
    m = re.search(r'\b(Open|Closed)\b.*?Last\s+Updated[:\s]+([A-Za-z]+ \d+)', text, re.IGNORECASE)
    if m:
        d["status"] = m.group(1).title()
        d["last_updated"] = m.group(2)
    else:
        m = re.search(r'\b(Open|Closed)\b', text[:3000], re.IGNORECASE)
        if m: d["status"] = m.group(1).title()
        m = re.search(r'Last\s+Updated[:\s]+([A-Za-z]+ \d+)', text, re.IGNORECASE)
        if m: d["last_updated"] = m.group(1)

    # ── Lifts ─────────────────────────────────────────────────────────────────
    m = re.search(r'(\d+)\s*/\s*(\d+)\s+Lifts', text, re.IGNORECASE)
    if m:
        d["lifts_open"]  = int(m.group(1))
        d["lifts_total"] = int(m.group(2))
    else:
        m = re.search(r'Lifts(?:\s+Open)?\s+(\d+)\s*/\s*(\d+)', text, re.IGNORECASE)
        if m:
            d["lifts_open"]  = int(m.group(1))
            d["lifts_total"] = int(m.group(2))

    # ── Runs ──────────────────────────────────────────────────────────────────
    m = re.search(r'(\d+)\s*/\s*(\d+)\s+(?:Runs|Trails)', text, re.IGNORECASE)
    if m:
        d["runs_open"]  = int(m.group(1))
        d["runs_total"] = int(m.group(2))
    else:
        m = re.search(r'(?:Runs|Trails)(?:\s+Open)?\s+(\d+)\s*/\s*(\d+)', text, re.IGNORECASE)
        if m:
            d["runs_open"]  = int(m.group(1))
            d["runs_total"] = int(m.group(2))

    # ── Snow depths ───────────────────────────────────────────────────────────
    m = re.search(r'\bBase\b[^\d"]{0,15}(\d+)\s*"', text, re.IGNORECASE)
    if m: d["base_depth"] = int(m.group(1))

    m = re.search(r'\bMid[- ]?Mountain\b[^\d"]{0,15}(\d+)\s*"', text, re.IGNORECASE)
    if m: d["mid_depth"] = int(m.group(1))

    m = re.search(r'\bSummit\b[^\d"]{0,15}(\d+)\s*"', text, re.IGNORECASE)
    if m: d["summit_depth"] = int(m.group(1))

    # ── Surface condition ─────────────────────────────────────────────────────
    m = re.search(
        r'(Powder|Packed Powder|Machine Groomed|Loose Granular|Hard Packed|Spring Snow|Corn Snow|Wet Snow|Wet Pack)',
        text, re.IGNORECASE
    )
    if m: d["surface_condition"] = m.group(1).title()

    # ── Snowfall ──────────────────────────────────────────────────────────────
    d["snowfall_7d"], d["snowfall_24h"] = parse_snowfall_section(text)

    # ── Enforce closed = 0 open lifts/runs ───────────────────────────────────
    if d["status"] == "Closed":
        d["lifts_open"] = 0
        d["runs_open"]  = 0

    # ── Infer status from lifts if still unknown ──────────────────────────────
    if d["status"] == "Unknown" and d["lifts_total"] > 0:
        d["status"] = "Open" if d["lifts_open"] > 0 else "Closed"

    return d


def scrape_all():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    results = []
    total = len(RESORTS)
    for i, resort in enumerate(RESORTS):
        print(f"[{i+1}/{total}] {resort['name']}...", file=sys.stderr, end=" ", flush=True)
        html = fetch_page(resort["url"])
        d = parse_snow_report(html, resort["name"])
        d.update({
            "state": resort["state"],
            "lat":   resort["lat"],
            "lng":   resort["lng"],
            "url":   resort["url"],
            "scraped_at": ts,
        })
        results.append(d)
        print(
            f"Base:{d['base_depth']}\" 7d:{d['snowfall_7d']}\" 24h:{d['snowfall_24h']}\" "
            f"Lifts:{d['lifts_open']}/{d['lifts_total']} [{d['status']}]",
            file=sys.stderr
        )
        if i < total - 1:
            time.sleep(REQUEST_DELAY)
    return results


def write_csv(results):
    fields = [
        "name","state","lat","lng",
        "base_depth","mid_depth","summit_depth","surface_condition",
        "snowfall_7d","snowfall_24h",
        "lifts_open","lifts_total","runs_open","runs_total",
        "status","last_updated","scraped_at","url",
    ]
    with open(OUTPUT_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in results:
            w.writerow({k: r.get(k, "") for k in fields})


def write_json(results):
    with open(OUTPUT_JSON, "w") as f:
        json.dump(
            {
                "resorts": results,
                "generated_at": results[0]["scraped_at"] if results else "",
                "resort_count": len(results),
            },
            f, indent=2
        )


if __name__ == "__main__":
    print(f"PowderWatch Scraper — {len(RESORTS)} resorts", file=sys.stderr)
    print(f"Estimated time: ~{int(len(RESORTS) * (REQUEST_DELAY + 1.5))}s\n", file=sys.stderr)
    results = scrape_all()
    write_csv(results)
    write_json(results)
    ok = sum(1 for r in results if r["snowfall_7d"] > 0 or r["lifts_total"] > 0)
    print(f"\nDone: {ok}/{len(results)} resorts returned usable data", file=sys.stderr)
    if "--json" in sys.argv:
        print(json.dumps(results, indent=2))
