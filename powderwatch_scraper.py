#!/usr/bin/env python3
"""
PowderWatch Snow Scraper Agent
===============================
Runs twice daily via GitHub Actions (or cron).
Fetches OnTheSnow ski report pages for 50 US resorts,
parses resort-reported snow data, and writes to snow_data.json + snow_data.csv.

Parsing strategy (in priority order):
  1. __NEXT_DATA__ JSON block (SSR'd by Next.js — present in raw HTML)
  2. Visual text regex fallbacks on stripped page text
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
REQUEST_DELAY   = 2.0   # seconds between requests — be polite
MAX_RETRIES     = 2
REQUEST_TIMEOUT = 20

# Rotate through realistic browser user-agents to avoid blocks
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

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
    # Rotate user agents across resorts to look more like organic traffic
    ua = USER_AGENTS[attempt % len(USER_AGENTS)]
    req = Request(url, headers={
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    try:
        with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            raw = resp.read()
            # Handle gzip-compressed responses
            encoding = resp.headers.get("Content-Encoding", "")
            if encoding == "gzip":
                import gzip
                raw = gzip.decompress(raw)
            return raw.decode("utf-8", errors="replace")
    except (URLError, HTTPError, TimeoutError) as e:
        if attempt <= MAX_RETRIES:
            time.sleep(3 * attempt)
            return fetch_page(url, attempt + 1)
        print(f"  FAILED: {url} ({e})", file=sys.stderr)
        return None


def _int(val, default=0):
    """Safe int conversion."""
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return default


def _float(val, default=0.0):
    """Safe float conversion."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def parse_next_data(html):
    """
    Extract the SSR'd __NEXT_DATA__ JSON block that Next.js embeds in every page.
    OnTheSnow's actual key names (confirmed from their page structure):
      newSnow24, newSnow48, newSnow7Day  — snowfall in inches
      baseDepth, midDepth, summitDepth  — snow depths
      liftsOpen, liftsTotal
      trailsOpen, trailsTotal
      status (string: "Open" / "Closed")
      lastReportedAt / reportDate
    Returns a flat dict of whatever was found, empty dict if nothing.
    """
    result = {}
    m = re.search(
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE
    )
    if not m:
        return result

    try:
        data = json.loads(m.group(1))
    except json.JSONDecodeError:
        return result

    # Flatten the nested structure — resort data can live at different depths
    raw = json.dumps(data)

    def first_match(patterns):
        for pat in patterns:
            hit = re.search(pat, raw, re.IGNORECASE)
            if hit:
                return hit.group(1)
        return None

    # 7-day snowfall — OnTheSnow key is newSnow7Day (inches)
    v = first_match([
        r'"newSnow7Day"\s*:\s*"?(\d+(?:\.\d+)?)"?',
        r'"snow7Day"\s*:\s*"?(\d+(?:\.\d+)?)"?',
        r'"snowfall7Day"\s*:\s*"?(\d+(?:\.\d+)?)"?',
        r'"snowfall7d"\s*:\s*"?(\d+(?:\.\d+)?)"?',
        r'"newSnow168h"\s*:\s*"?(\d+(?:\.\d+)?)"?',
    ])
    if v: result["snowfall_7d"] = _float(v)

    # 24h snowfall — OnTheSnow key is newSnow24
    v = first_match([
        r'"newSnow24"\s*:\s*"?(\d+(?:\.\d+)?)"?',
        r'"snow24Hour"\s*:\s*"?(\d+(?:\.\d+)?)"?',
        r'"snowfall24h"\s*:\s*"?(\d+(?:\.\d+)?)"?',
        r'"newSnow24h"\s*:\s*"?(\d+(?:\.\d+)?)"?',
        r'"newSnow24Hour"\s*:\s*"?(\d+(?:\.\d+)?)"?',
    ])
    if v: result["snowfall_24h"] = _float(v)

    # Lifts
    v = first_match([r'"liftsOpen"\s*:\s*(\d+)', r'"lifts_open"\s*:\s*(\d+)'])
    if v: result["lifts_open"] = _int(v)

    v = first_match([r'"liftsTotal"\s*:\s*(\d+)', r'"lifts_total"\s*:\s*(\d+)', r'"liftsCount"\s*:\s*(\d+)'])
    if v: result["lifts_total"] = _int(v)

    # Trails / runs
    v = first_match([r'"trailsOpen"\s*:\s*(\d+)', r'"runsOpen"\s*:\s*(\d+)', r'"runs_open"\s*:\s*(\d+)'])
    if v: result["runs_open"] = _int(v)

    v = first_match([r'"trailsTotal"\s*:\s*(\d+)', r'"runsTotal"\s*:\s*(\d+)', r'"runs_total"\s*:\s*(\d+)'])
    if v: result["runs_total"] = _int(v)

    # Snow depths
    v = first_match([r'"baseDepth"\s*:\s*(\d+)', r'"base_depth"\s*:\s*(\d+)', r'"baseSnow"\s*:\s*(\d+)'])
    if v: result["base_depth"] = _int(v)

    v = first_match([r'"midDepth"\s*:\s*(\d+)', r'"midMountainDepth"\s*:\s*(\d+)'])
    if v: result["mid_depth"] = _int(v)

    v = first_match([r'"summitDepth"\s*:\s*(\d+)', r'"summitSnow"\s*:\s*(\d+)'])
    if v: result["summit_depth"] = _int(v)

    # Status — look for explicit open/closed string
    v = first_match([
        r'"status"\s*:\s*"(Open|Closed|Opening Soon|Partially Open)"',
        r'"resortStatus"\s*:\s*"(Open|Closed|Opening Soon|Partially Open)"',
        r'"isOpen"\s*:\s*(true|false)',
    ])
    if v:
        if v.lower() == "true":
            result["status"] = "Open"
        elif v.lower() == "false":
            result["status"] = "Closed"
        else:
            result["status"] = v.title()

    # Surface condition
    v = first_match([r'"surfaceCondition"\s*:\s*"([^"]{3,40})"', r'"surface"\s*:\s*"([^"]{3,40})"'])
    if v and v.lower() not in ("null", "undefined", ""):
        result["surface_condition"] = v.title()

    # Last updated date
    v = first_match([
        r'"reportDate"\s*:\s*"([^"]+)"',
        r'"lastReportedAt"\s*:\s*"([^"]+)"',
        r'"lastUpdated"\s*:\s*"([^"]+)"',
        r'"updatedAt"\s*:\s*"([^"]+)"',
    ])
    if v:
        # Try to format as "Apr 05" style
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
            result["last_updated"] = dt.strftime("%b %d").lstrip("0")
        except Exception:
            result["last_updated"] = v[:10]  # fallback: raw date string

    return result


def parse_visual_text(text, d):
    """
    Fallback: regex patterns on stripped visible page text.
    Only fills fields still missing (value == 0 or empty) in d.
    OnTheSnow page text consistently shows patterns like:
      "7 Day Snow  12""   "24 Hr Snow  3""   "Lifts Open  8 / 33"
    """

    # ── Snowfall ──────────────────────────────────────────────────────────────
    # "7 Day Snow 12"" or "7-Day: 12"" or "7 day 12 inches"
    if not d.get("snowfall_7d"):
        m = re.search(
            r'7[\s\-]?day(?:\s+snow(?:fall)?)?\s*[:\-]?\s*(\d+(?:\.\d+)?)\s*["\']',
            text, re.IGNORECASE
        )
        if m: d["snowfall_7d"] = _float(m.group(1))

    # "24 Hr Snow 3"" or "New Snow 24h: 3""
    if not d.get("snowfall_24h"):
        m = re.search(
            r'(?:24[\s\-]?h(?:r|our)?(?:\s+snow(?:fall)?)?|new\s+snow\s+24)\s*[:\-]?\s*(\d+(?:\.\d+)?)\s*["\']',
            text, re.IGNORECASE
        )
        if m: d["snowfall_24h"] = _float(m.group(1))

    # ── Lifts ─────────────────────────────────────────────────────────────────
    # "Lifts Open 8 / 33" or "8 of 33 lifts open"
    if not d.get("lifts_open") and not d.get("lifts_total"):
        m = re.search(
            r'lifts?\s+open\s+(\d+)\s*/\s*(\d+)',
            text, re.IGNORECASE
        )
        if m:
            d["lifts_open"]  = _int(m.group(1))
            d["lifts_total"] = _int(m.group(2))

    if not d.get("lifts_open") and not d.get("lifts_total"):
        m = re.search(
            r'(\d+)\s*/\s*(\d+)\s+lifts?\s+open',
            text, re.IGNORECASE
        )
        if m:
            d["lifts_open"]  = _int(m.group(1))
            d["lifts_total"] = _int(m.group(2))

    if not d.get("lifts_open") and not d.get("lifts_total"):
        m = re.search(
            r'(\d+)\s+of\s+(\d+)\s+lifts',
            text, re.IGNORECASE
        )
        if m:
            d["lifts_open"]  = _int(m.group(1))
            d["lifts_total"] = _int(m.group(2))

    # ── Runs ──────────────────────────────────────────────────────────────────
    if not d.get("runs_open") and not d.get("runs_total"):
        m = re.search(
            r'(?:trails?|runs?)\s+open\s+(\d+)\s*/\s*(\d+)',
            text, re.IGNORECASE
        )
        if m:
            d["runs_open"]  = _int(m.group(1))
            d["runs_total"] = _int(m.group(2))

    # ── Snow depths ───────────────────────────────────────────────────────────
    if not d.get("base_depth"):
        m = re.search(r'\bbase(?:\s+depth)?\s*[:\-]?\s*(\d+)\s*["\']', text, re.IGNORECASE)
        if m: d["base_depth"] = _int(m.group(1))

    if not d.get("mid_depth"):
        m = re.search(r'mid(?:[- ]mountain)?\s*[:\-]?\s*(\d+)\s*["\']', text, re.IGNORECASE)
        if m: d["mid_depth"] = _int(m.group(1))

    if not d.get("summit_depth"):
        m = re.search(r'summit\s*[:\-]?\s*(\d+)\s*["\']', text, re.IGNORECASE)
        if m: d["summit_depth"] = _int(m.group(1))

    # ── Surface condition ─────────────────────────────────────────────────────
    if not d.get("surface_condition"):
        m = re.search(
            r'(?:surface|conditions?)\s*[:\-]?\s*'
            r'(Powder|Packed Powder|Machine Groomed|Loose Granular|Hard Packed|Spring Snow|Corn Snow|Wet Snow|Wet Pack)',
            text, re.IGNORECASE
        )
        if m: d["surface_condition"] = m.group(1).title()

    # ── Status ────────────────────────────────────────────────────────────────
    if not d.get("status") or d["status"] == "Unknown":
        # Look for explicit open/closed label near the resort name area
        m = re.search(r'\b(Open|Closed)\b', text[:2000], re.IGNORECASE)
        if m: d["status"] = m.group(1).title()

    # ── Last updated ──────────────────────────────────────────────────────────
    if not d.get("last_updated"):
        m = re.search(
            r'(?:last\s+updated|as\s+of|reported)\s*[:\-]?\s*([A-Za-z]{3,9}\s+\d{1,2})',
            text, re.IGNORECASE
        )
        if m: d["last_updated"] = m.group(1)

    return d


def parse_snow_report(html, resort_name):
    d = {
        "name": resort_name,
        "base_depth": 0, "mid_depth": 0, "summit_depth": 0,
        "surface_condition": "",
        "snowfall_7d": 0.0, "snowfall_24h": 0.0,
        "lifts_open": 0, "lifts_total": 0,
        "runs_open": 0, "runs_total": 0,
        "status": "Unknown", "last_updated": "",
    }
    if not html:
        return d

    # ── Pass 1: __NEXT_DATA__ JSON (most reliable) ────────────────────────────
    nd = parse_next_data(html)
    d.update({k: v for k, v in nd.items() if v not in (0, 0.0, "", None)})

    # ── Pass 2: Visual text fallbacks ─────────────────────────────────────────
    # Strip scripts/styles first, then collapse whitespace
    text = re.sub(r'<(script|style)[^>]*>.*?</\1>', ' ', html, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html_lib.unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()

    d = parse_visual_text(text, d)

    # ── Final status inference ────────────────────────────────────────────────
    # If we have lift data and status is still unknown, infer from lifts
    if d["status"] == "Unknown":
        if d["lifts_total"] > 0:
            d["status"] = "Open" if d["lifts_open"] > 0 else "Closed"
        else:
            # Last resort: look for open/closed in page title area
            m = re.search(r'<title[^>]*>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
            if m and re.search(r'\bclosed\b', m.group(1), re.IGNORECASE):
                d["status"] = "Closed"

    # ── Enforce closed = 0 open lifts ────────────────────────────────────────
    # If a resort is confirmed closed, lifts_open must be 0 regardless of
    # whatever stale number may have been scraped from the page.
    if d["status"] == "Closed":
        d["lifts_open"] = 0
        d["runs_open"] = 0

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
