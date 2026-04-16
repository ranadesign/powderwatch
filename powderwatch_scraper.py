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

    # ── Additional 200 resorts ────────────────────────────────────────────────

    # Colorado (additional)
    {"name":"Eldora","state":"CO","lat":39.9373,"lng":-105.5831,"url":"https://www.onthesnow.com/colorado/eldora-mountain-resort/skireport"},
    {"name":"Purgatory Resort","state":"CO","lat":37.6289,"lng":-107.8623,"url":"https://www.onthesnow.com/colorado/durango-mountain-resort/skireport"},
    {"name":"Monarch","state":"CO","lat":38.5123,"lng":-106.3328,"url":"https://www.onthesnow.com/colorado/monarch-mountain/skireport"},
    {"name":"Powderhorn","state":"CO","lat":39.0669,"lng":-108.1267,"url":"https://www.onthesnow.com/colorado/powderhorn/skireport"},
    {"name":"Sunlight Mountain","state":"CO","lat":39.4481,"lng":-107.4078,"url":"https://www.onthesnow.com/colorado/sunlight-mountain-resort/skireport"},
    {"name":"Silverton Mountain","state":"CO","lat":37.8699,"lng":-107.6619,"url":"https://www.onthesnow.com/colorado/silverton-mountain/skireport"},
    {"name":"Cooper","state":"CO","lat":39.0648,"lng":-106.3491,"url":"https://www.onthesnow.com/colorado/ski-cooper/skireport"},
    {"name":"Granby Ranch","state":"CO","lat":40.0579,"lng":-105.8943,"url":"https://www.onthesnow.com/colorado/ski-granby-ranch/skireport"},
    {"name":"Howelsen Hill","state":"CO","lat":40.4844,"lng":-106.8315,"url":"https://www.onthesnow.com/colorado/howelsen-hill/skireport"},

    # Utah (additional)
    {"name":"Brian Head Resort","state":"UT","lat":37.7003,"lng":-112.8491,"url":"https://www.onthesnow.com/utah/brian-head-resort/skireport"},
    {"name":"Beaver Mountain","state":"UT","lat":41.9699,"lng":-111.5390,"url":"https://www.onthesnow.com/utah/beaver-mountain/skireport"},
    {"name":"Eagle Point","state":"UT","lat":38.3298,"lng":-112.3985,"url":"https://www.onthesnow.com/utah/eagle-point/skireport"},

    # California (additional)
    {"name":"Kirkwood","state":"CA","lat":38.6840,"lng":-120.0651,"url":"https://www.onthesnow.com/california/kirkwood/skireport"},
    {"name":"Sierra-at-Tahoe","state":"CA","lat":38.7942,"lng":-120.0801,"url":"https://www.onthesnow.com/california/sierra-at-tahoe/skireport"},
    {"name":"Mt. Shasta Ski Park","state":"CA","lat":41.3731,"lng":-122.2098,"url":"https://www.onthesnow.com/california/mount-shasta-board-ski-park/skireport"},
    {"name":"Dodge Ridge","state":"CA","lat":38.1776,"lng":-119.9546,"url":"https://www.onthesnow.com/california/dodge-ridge/skireport"},
    {"name":"Homewood Mountain","state":"CA","lat":39.0863,"lng":-120.1607,"url":"https://www.onthesnow.com/california/homewood-mountain-resort/skireport"},
    {"name":"Boreal Mountain Resort","state":"CA","lat":39.3376,"lng":-120.3578,"url":"https://www.onthesnow.com/california/boreal/skireport"},
    {"name":"Soda Springs","state":"CA","lat":39.3298,"lng":-120.3799,"url":"https://www.onthesnow.com/california/soda-springs/skireport"},
    {"name":"Donner Ski Ranch","state":"CA","lat":39.3248,"lng":-120.3346,"url":"https://www.onthesnow.com/california/donner-ski-ranch/skireport"},
    {"name":"Bear Valley","state":"CA","lat":38.4686,"lng":-120.0448,"url":"https://www.onthesnow.com/california/bear-valley/skireport"},
    {"name":"Mt. Baldy","state":"CA","lat":34.2493,"lng":-117.6465,"url":"https://www.onthesnow.com/california/mt-baldy/skireport"},
    {"name":"Mountain High","state":"CA","lat":34.3726,"lng":-117.6432,"url":"https://www.onthesnow.com/california/mountain-high/skireport"},

    # Wyoming (additional)
    {"name":"White Pine Ski Area","state":"WY","lat":42.8404,"lng":-109.7712,"url":"https://www.onthesnow.com/wyoming/white-pine-ski-area/skireport"},

    # Montana (additional)
    {"name":"Bridger Bowl","state":"MT","lat":45.8168,"lng":-110.8989,"url":"https://www.onthesnow.com/montana/bridger-bowl/skireport"},
    {"name":"Blacktail Mountain","state":"MT","lat":48.0029,"lng":-114.4307,"url":"https://www.onthesnow.com/montana/blacktail-mountain-ski-area/skireport"},
    {"name":"Red Lodge Mountain","state":"MT","lat":45.1723,"lng":-109.2543,"url":"https://www.onthesnow.com/montana/red-lodge-mountain/skireport"},
    {"name":"Showdown Ski Area","state":"MT","lat":46.9209,"lng":-110.4498,"url":"https://www.onthesnow.com/montana/showdown-ski-area/skireport"},

    # Idaho (additional)
    {"name":"Bogus Basin","state":"ID","lat":43.7620,"lng":-116.1009,"url":"https://www.onthesnow.com/idaho/bogus-basin/skireport"},
    {"name":"Brundage Mountain","state":"ID","lat":44.9654,"lng":-116.4312,"url":"https://www.onthesnow.com/idaho/brundage-mountain-resort/skireport"},
    {"name":"Tamarack Resort","state":"ID","lat":44.7148,"lng":-116.0938,"url":"https://www.onthesnow.com/idaho/tamarack-resort/skireport"},
    {"name":"Pomerelle Mountain","state":"ID","lat":42.2276,"lng":-113.7196,"url":"https://www.onthesnow.com/idaho/pomerelle-mountain-resort/skireport"},
    {"name":"Magic Mountain","state":"ID","lat":42.3576,"lng":-114.3829,"url":"https://www.onthesnow.com/idaho/magic-mountain/skireport"},

    # Washington (additional)
    {"name":"49 Degrees North","state":"WA","lat":48.5326,"lng":-117.8027,"url":"https://www.onthesnow.com/washington/49-degrees-north/skireport"},
    {"name":"White Pass","state":"WA","lat":46.6387,"lng":-121.3929,"url":"https://www.onthesnow.com/washington/white-pass/skireport"},
    {"name":"Mission Ridge","state":"WA","lat":47.2929,"lng":-120.3973,"url":"https://www.onthesnow.com/washington/mission-ridge/skireport"},
    {"name":"Alpental","state":"WA","lat":47.4248,"lng":-121.4252,"url":"https://www.onthesnow.com/washington/alpental/skireport"},
    {"name":"Bluewood","state":"WA","lat":46.0854,"lng":-117.8454,"url":"https://www.onthesnow.com/washington/bluewood/skireport"},

    # Oregon (additional)
    {"name":"Timberline Lodge","state":"OR","lat":45.3311,"lng":-121.7107,"url":"https://www.onthesnow.com/oregon/timberline-lodge/skireport"},
    {"name":"Anthony Lakes","state":"OR","lat":44.9648,"lng":-118.2287,"url":"https://www.onthesnow.com/oregon/anthony-lakes-mountain-resort/skireport"},
    {"name":"Hoodoo Ski Area","state":"OR","lat":44.3962,"lng":-121.8719,"url":"https://www.onthesnow.com/oregon/hoodoo-ski-area/skireport"},
    {"name":"Mt. Hood Skibowl","state":"OR","lat":45.3060,"lng":-121.7812,"url":"https://www.onthesnow.com/oregon/mt-hood-ski-bowl/skireport"},
    {"name":"Willamette Pass","state":"OR","lat":43.5887,"lng":-122.0500,"url":"https://www.onthesnow.com/oregon/willamette-pass/skireport"},

    # Nevada
    {"name":"Mt. Rose - Ski Tahoe","state":"NV","lat":39.3147,"lng":-119.8818,"url":"https://www.onthesnow.com/nevada/mt-rose-ski-tahoe/skireport"},
    {"name":"Diamond Peak","state":"NV","lat":39.2569,"lng":-119.9188,"url":"https://www.onthesnow.com/nevada/diamond-peak/skireport"},
    {"name":"Lee Canyon","state":"NV","lat":36.3225,"lng":-115.6737,"url":"https://www.onthesnow.com/nevada/lee-canyon/skireport"},
    {"name":"Heavenly (NV side)","state":"NV","lat":38.9353,"lng":-119.94,"url":"https://www.onthesnow.com/nevada/heavenly-mountain-resort/skireport"},

    # New Mexico
    {"name":"Taos Ski Valley","state":"NM","lat":36.5945,"lng":-105.4482,"url":"https://www.onthesnow.com/new-mexico/taos-ski-valley/skireport"},
    {"name":"Angel Fire","state":"NM","lat":36.3918,"lng":-105.2868,"url":"https://www.onthesnow.com/new-mexico/angel-fire-resort/skireport"},
    {"name":"Ski Santa Fe","state":"NM","lat":35.7886,"lng":-105.8163,"url":"https://www.onthesnow.com/new-mexico/ski-santa-fe/skireport"},
    {"name":"Red River Ski Area","state":"NM","lat":36.7075,"lng":-105.4070,"url":"https://www.onthesnow.com/new-mexico/red-river/skireport"},
    {"name":"Ski Apache","state":"NM","lat":33.4102,"lng":-105.7720,"url":"https://www.onthesnow.com/new-mexico/ski-apache/skireport"},
    {"name":"Sandia Peak","state":"NM","lat":35.2035,"lng":-106.4503,"url":"https://www.onthesnow.com/new-mexico/sandia-peak/skireport"},
    {"name":"Sipapu Ski Area","state":"NM","lat":36.2095,"lng":-105.5290,"url":"https://www.onthesnow.com/new-mexico/sipapu-ski-and-summer-resort/skireport"},

    # Arizona
    {"name":"Arizona Snowbowl","state":"AZ","lat":35.3312,"lng":-111.7082,"url":"https://www.onthesnow.com/arizona/arizona-snowbowl/skireport"},
    {"name":"Sunrise Park Resort","state":"AZ","lat":33.9828,"lng":-109.5680,"url":"https://www.onthesnow.com/arizona/sunrise-park-resort/skireport"},

    # New Hampshire
    {"name":"Loon Mountain","state":"NH","lat":44.0318,"lng":-71.6228,"url":"https://www.onthesnow.com/new-hampshire/loon-mountain/skireport"},
    {"name":"Cannon Mountain","state":"NH","lat":44.1573,"lng":-71.6990,"url":"https://www.onthesnow.com/new-hampshire/cannon-mountain/skireport"},
    {"name":"Waterville Valley","state":"NH","lat":43.9684,"lng":-71.5148,"url":"https://www.onthesnow.com/new-hampshire/waterville-valley/skireport"},
    {"name":"Attitash","state":"NH","lat":44.0773,"lng":-71.2220,"url":"https://www.onthesnow.com/new-hampshire/attitash/skireport"},
    {"name":"Bretton Woods","state":"NH","lat":44.2660,"lng":-71.4418,"url":"https://www.onthesnow.com/new-hampshire/bretton-woods/skireport"},
    {"name":"Wildcat Mountain","state":"NH","lat":44.2584,"lng":-71.2387,"url":"https://www.onthesnow.com/new-hampshire/wildcat-mountain/skireport"},
    {"name":"Gunstock Mountain","state":"NH","lat":43.5173,"lng":-71.3749,"url":"https://www.onthesnow.com/new-hampshire/gunstock/skireport"},
    {"name":"Whaleback Mountain","state":"NH","lat":43.6548,"lng":-72.1329,"url":"https://www.onthesnow.com/new-hampshire/whaleback-mountain/skireport"},
    {"name":"Black Mountain","state":"NH","lat":44.0426,"lng":-71.1929,"url":"https://www.onthesnow.com/new-hampshire/black-mountain/skireport"},
    {"name":"Pat's Peak","state":"NH","lat":43.1879,"lng":-71.5921,"url":"https://www.onthesnow.com/new-hampshire/pats-peak/skireport"},
    {"name":"Crotched Mountain","state":"NH","lat":42.9737,"lng":-71.9166,"url":"https://www.onthesnow.com/new-hampshire/crotched-mountain/skireport"},

    # Vermont (additional)
    {"name":"Mad River Glen","state":"VT","lat":44.1920,"lng":-72.9118,"url":"https://www.onthesnow.com/vermont/mad-river-glen/skireport"},
    {"name":"Burke Mountain","state":"VT","lat":44.5951,"lng":-71.9069,"url":"https://www.onthesnow.com/vermont/burke-mountain/skireport"},
    {"name":"Bromley Mountain","state":"VT","lat":43.2118,"lng":-72.9421,"url":"https://www.onthesnow.com/vermont/bromley-mountain/skireport"},
    {"name":"Bolton Valley","state":"VT","lat":44.4073,"lng":-72.8610,"url":"https://www.onthesnow.com/vermont/bolton-valley/skireport"},
    {"name":"Magic Mountain","state":"VT","lat":43.2868,"lng":-72.8082,"url":"https://www.onthesnow.com/vermont/magic-mountain/skireport"},
    {"name":"Smugglers' Notch","state":"VT","lat":44.5665,"lng":-72.7897,"url":"https://www.onthesnow.com/vermont/smugglers-notch-resort/skireport"},

    # Maine (additional)
    {"name":"Saddleback Maine","state":"ME","lat":44.9012,"lng":-70.5099,"url":"https://www.onthesnow.com/maine/saddleback-inc/skireport"},
    {"name":"Black Mountain of Maine","state":"ME","lat":44.5343,"lng":-70.5410,"url":"https://www.onthesnow.com/maine/black-mountain-of-maine/skireport"},
    {"name":"Big Moose Mountain","state":"ME","lat":45.3648,"lng":-69.6507,"url":"https://www.onthesnow.com/maine/big-squaw-mountain-ski-resort/skireport"},
    {"name":"Camden Snow Bowl","state":"ME","lat":44.2215,"lng":-69.0687,"url":"https://www.onthesnow.com/maine/camden-snow-bowl/skireport"},

    # New York
    {"name":"Whiteface Mountain","state":"NY","lat":44.3659,"lng":-73.9021,"url":"https://www.onthesnow.com/new-york/whiteface-mountain-resort/skireport"},
    {"name":"Hunter Mountain","state":"NY","lat":42.1937,"lng":-74.2237,"url":"https://www.onthesnow.com/new-york/hunter-mountain/skireport"},
    {"name":"Gore Mountain","state":"NY","lat":43.6663,"lng":-74.0048,"url":"https://www.onthesnow.com/new-york/gore-mountain/skireport"},
    {"name":"Belleayre","state":"NY","lat":42.1459,"lng":-74.5027,"url":"https://www.onthesnow.com/new-york/belleayre/skireport"},
    {"name":"Bristol Mountain","state":"NY","lat":42.7221,"lng":-77.3840,"url":"https://www.onthesnow.com/new-york/bristol-mountain/skireport"},
    {"name":"Holiday Valley","state":"NY","lat":42.2651,"lng":-78.6792,"url":"https://www.onthesnow.com/new-york/holiday-valley/skireport"},
    {"name":"Windham Mountain","state":"NY","lat":42.2971,"lng":-74.2595,"url":"https://www.onthesnow.com/new-york/windham-mountain/skireport"},
    {"name":"Catamount Ski Area","state":"NY","lat":42.1501,"lng":-73.4710,"url":"https://www.onthesnow.com/new-york/catamount-ski-ride-area/skireport"},
    {"name":"Greek Peak Mountain","state":"NY","lat":42.5298,"lng":-76.1509,"url":"https://www.onthesnow.com/united-states/greek-peak/skireport"},
    {"name":"Song Mountain","state":"NY","lat":42.8890,"lng":-75.9707,"url":"https://www.onthesnow.com/new-york/song-mountain/skireport"},

    # Pennsylvania
    {"name":"Camelback Mountain Resort","state":"PA","lat":41.0437,"lng":-75.3466,"url":"https://www.onthesnow.com/pennsylvania/camelback-mountain-resort/skireport"},
    {"name":"Blue Mountain Resort","state":"PA","lat":40.6959,"lng":-75.5141,"url":"https://www.onthesnow.com/pennsylvania/blue-mountain-ski-area/skireport"},
    {"name":"Seven Springs Mountain Resort","state":"PA","lat":39.9265,"lng":-79.2876,"url":"https://www.onthesnow.com/pennsylvania/seven-springs/skireport"},
    {"name":"Hidden Valley Resort","state":"PA","lat":39.9187,"lng":-79.1371,"url":"https://www.onthesnow.com/pennsylvania/hidden-valley-resort/skireport"},
    {"name":"Montage Mountain","state":"PA","lat":41.3593,"lng":-75.6785,"url":"https://www.onthesnow.com/pennsylvania/montage-mountain/skireport"},
    {"name":"Bear Creek Mountain Resort","state":"PA","lat":40.3601,"lng":-75.6724,"url":"https://www.onthesnow.com/pennsylvania/bear-creek-mountain-resort/skireport"},
    {"name":"Ski Big Bear","state":"PA","lat":41.3526,"lng":-75.3743,"url":"https://www.onthesnow.com/pennsylvania/big-bear/skireport"},
    {"name":"Jack Frost Mountain","state":"PA","lat":41.0687,"lng":-75.7060,"url":"https://www.onthesnow.com/pennsylvania/jack-frost/skireport"},
    {"name":"Blue Knob","state":"PA","lat":40.2798,"lng":-78.5546,"url":"https://www.onthesnow.com/pennsylvania/blue-knob/skireport"},

    # Massachusetts
    {"name":"Jiminy Peak","state":"MA","lat":42.6357,"lng":-73.2924,"url":"https://www.onthesnow.com/massachusetts/jiminy-peak/skireport"},
    {"name":"Wachusett Mountain","state":"MA","lat":42.4882,"lng":-71.8885,"url":"https://www.onthesnow.com/massachusetts/wachusett-mountain-ski-area/skireport"},
    {"name":"Berkshire East","state":"MA","lat":42.6112,"lng":-72.7710,"url":"https://www.onthesnow.com/massachusetts/berkshire-east/skireport"},

    # Connecticut
    {"name":"Mohawk Mountain","state":"CT","lat":41.8512,"lng":-73.1443,"url":"https://www.onthesnow.com/connecticut/mohawk-mountain/skireport"},
    {"name":"Ski Sundown","state":"CT","lat":41.8965,"lng":-73.0109,"url":"https://www.onthesnow.com/connecticut/ski-sundown/skireport"},

    # New Jersey

    # Maryland

    # Virginia
    {"name":"Bryce Resort","state":"VA","lat":38.8289,"lng":-78.7565,"url":"https://www.onthesnow.com/virginia/bryce-resort/skireport"},
    {"name":"The Homestead","state":"VA","lat":37.9892,"lng":-79.9074,"url":"https://www.onthesnow.com/virginia/the-homestead/skireport"},

    # North Carolina
    {"name":"Beech Mountain Resort","state":"NC","lat":36.1876,"lng":-81.8918,"url":"https://www.onthesnow.com/north-carolina/ski-beech-mountain-resort/skireport"},
    {"name":"Sugar Mountain Resort","state":"NC","lat":36.1251,"lng":-81.8677,"url":"https://www.onthesnow.com/north-carolina/sugar-mountain-resort/skireport"},
    {"name":"Appalachian Ski Mountain","state":"NC","lat":36.2132,"lng":-81.7043,"url":"https://www.onthesnow.com/north-carolina/appalachian-ski-mtn/skireport"},
    {"name":"Cataloochee Ski Area","state":"NC","lat":35.5501,"lng":-83.0676,"url":"https://www.onthesnow.com/north-carolina/cataloochee-ski-area/skireport"},
    {"name":"Wolf Ridge Ski Resort","state":"NC","lat":35.7065,"lng":-82.2026,"url":"https://www.onthesnow.com/north-carolina/wolf-ridge-ski-resort/skireport"},

    # Tennessee

    # Michigan
    {"name":"Boyne Mountain Resort","state":"MI","lat":45.1798,"lng":-84.9213,"url":"https://www.onthesnow.com/michigan/boyne-mountain-resort/skireport"},
    {"name":"Boyne Highlands","state":"MI","lat":45.3726,"lng":-84.8832,"url":"https://www.onthesnow.com/michigan/boyne-highlands/skireport"},
    {"name":"Crystal Mountain","state":"MI","lat":44.5251,"lng":-85.9346,"url":"https://www.onthesnow.com/michigan/crystal-mountain/skireport"},
    {"name":"Nub's Nob","state":"MI","lat":45.3707,"lng":-84.8879,"url":"https://www.onthesnow.com/michigan/nubs-nob-ski-area/skireport"},
    {"name":"Shanty Creek Resorts","state":"MI","lat":44.9626,"lng":-85.1465,"url":"https://www.onthesnow.com/michigan/shanty-creek/skireport"},
    {"name":"Mount Bohemia","state":"MI","lat":47.3629,"lng":-88.1887,"url":"https://www.onthesnow.com/michigan/mount-bohemia/skireport"},
    {"name":"Big Powderhorn Mountain","state":"MI","lat":46.5337,"lng":-90.1337,"url":"https://www.onthesnow.com/michigan/big-powderhorn-mountain/skireport"},
    {"name":"Indianhead Mountain","state":"MI","lat":46.5084,"lng":-90.1612,"url":"https://www.onthesnow.com/michigan/snowriver-mountain-resort/skireport"},
    {"name":"Caberfae Peaks","state":"MI","lat":44.2298,"lng":-85.7326,"url":"https://www.onthesnow.com/michigan/caberfae-peaks-ski-golf-resort/skireport"},
    {"name":"Ski Brule","state":"MI","lat":46.3937,"lng":-89.0154,"url":"https://www.onthesnow.com/michigan/ski-brule/skireport"},
    {"name":"Pine Mountain Resort","state":"MI","lat":45.7890,"lng":-88.0648,"url":"https://www.onthesnow.com/michigan/pine-mountain/skireport"},
    {"name":"Bittersweet Ski Area","state":"MI","lat":42.3751,"lng":-85.8501,"url":"https://www.onthesnow.com/michigan/bittersweet-ski-area/skireport"},
    {"name":"Searchmont Resort","state":"MI","lat":46.6068,"lng":-83.9237,"url":"https://www.onthesnow.com/michigan/searchmont-resort/skireport"},

    # Wisconsin
    {"name":"Granite Peak","state":"WI","lat":44.8946,"lng":-89.7040,"url":"https://www.onthesnow.com/wisconsin/granite-peak-ski-area/skireport"},
    {"name":"Devil's Head Resort","state":"WI","lat":43.4418,"lng":-89.5737,"url":"https://www.onthesnow.com/wisconsin/devils-head/skireport"},
    {"name":"Cascade Mountain","state":"WI","lat":43.3976,"lng":-89.6085,"url":"https://www.onthesnow.com/wisconsin/cascade-mountain/skireport"},
    {"name":"Trollhaugen","state":"WI","lat":45.0229,"lng":-92.5101,"url":"https://www.onthesnow.com/wisconsin/trollhaugen/skireport"},
    {"name":"Tyrol Basin","state":"WI","lat":43.0287,"lng":-89.8376,"url":"https://www.onthesnow.com/wisconsin/tyrol-basin/skireport"},

    # Minnesota
    {"name":"Lutsen Mountains","state":"MN","lat":47.6499,"lng":-90.6865,"url":"https://www.onthesnow.com/minnesota/lutsen-mountains/skireport"},
    {"name":"Spirit Mountain","state":"MN","lat":46.7182,"lng":-92.2237,"url":"https://www.onthesnow.com/minnesota/spirit-mountain/skireport"},
    {"name":"Afton Alps","state":"MN","lat":44.8593,"lng":-92.7890,"url":"https://www.onthesnow.com/minnesota/afton-alps/skireport"},
    {"name":"Buck Hill","state":"MN","lat":44.7426,"lng":-93.3301,"url":"https://www.onthesnow.com/minnesota/buck-hill/skireport"},
    {"name":"Welch Village","state":"MN","lat":44.6012,"lng":-92.6968,"url":"https://www.onthesnow.com/minnesota/welch-village/skireport"},

    # Ohio
    {"name":"Mad River Mountain","state":"OH","lat":40.2943,"lng":-83.5368,"url":"https://www.onthesnow.com/ohio/mad-river-mountain/skireport"},
    {"name":"Boston Mills","state":"OH","lat":41.2418,"lng":-81.5648,"url":"https://www.onthesnow.com/ohio/boston-mills/skireport"},
    {"name":"Brandywine","state":"OH","lat":41.2312,"lng":-81.5151,"url":"https://www.onthesnow.com/ohio/brandywine/skireport"},
    {"name":"Snow Trails","state":"OH","lat":40.6937,"lng":-82.6418,"url":"https://www.onthesnow.com/ohio/snow-trails/skireport"},

    # Indiana
    {"name":"Perfect North Slopes","state":"IN","lat":38.9654,"lng":-84.7290,"url":"https://www.onthesnow.com/indiana/perfect-north-slopes/skireport"},

    # Iowa
    {"name":"Sundown Mountain","state":"IA","lat":42.4479,"lng":-90.5501,"url":"https://www.onthesnow.com/iowa/sundown-mountain/skireport"},

    # Missouri

    # South Dakota

    # North Dakota

    # Nebraska

    # Kansas
    {"name":"Snow Creek Ski Area","state":"KS","lat":39.4498,"lng":-94.8404,"url":"https://www.onthesnow.com/missouri/snow-creek/skireport"},


    {"name":"Eaglecrest Ski Area","state":"AK","lat":58.3998,"lng":-134.6312,"url":"https://www.onthesnow.com/alaska/eaglecrest-ski-area/skireport"},

    # Hawaii
    # No ski resorts

    # Additional high-traffic resorts across all regions
    {"name":"Echo Mountain","state":"CO","lat":39.6751,"lng":-105.5001,"url":"https://www.onthesnow.com/colorado/echo-mountain/skireport"},

    # More California
    {"name":"Tahoe Donner","state":"CA","lat":39.3487,"lng":-120.3196,"url":"https://www.onthesnow.com/california/tahoe-donner/skireport"},

    # More Washington

    # More New England states
    {"name":"Sunapee","state":"NH","lat":43.3876,"lng":-72.0679,"url":"https://www.onthesnow.com/new-hampshire/mount-sunapee/skireport"},
    {"name":"Cranmore Mountain Resort","state":"NH","lat":44.0687,"lng":-71.0899,"url":"https://www.onthesnow.com/new-hampshire/cranmore-mountain-resort/skireport"},

    # More Vermont

    # More Michigan
    {"name":"Alpine Valley Ski Area","state":"MI","lat":42.7043,"lng":-83.5701,"url":"https://www.onthesnow.com/michigan/alpine-valley-ski-area/skireport"},

    # More Wisconsin
    {"name":"Wilmot Mountain","state":"WI","lat":42.5265,"lng":-88.2026,"url":"https://www.onthesnow.com/wisconsin/wilmot-mountain/skireport"},
    {"name":"Alpine Valley Resort","state":"WI","lat":42.5751,"lng":-88.3929,"url":"https://www.onthesnow.com/wisconsin/alpine-valley-resort/skireport"},

    # More Minnesota
    {"name":"Andes Tower Hills","state":"MN","lat":45.7751,"lng":-95.2390,"url":"https://www.onthesnow.com/minnesota/andes-tower-hills-ski-area/skireport"},

    # More New York
    {"name":"Plattekill Mountain","state":"NY","lat":42.2390,"lng":-74.6140,"url":"https://www.onthesnow.com/new-york/plattekill-mountain/skireport"},
    {"name":"Ski Butternut","state":"NY","lat":42.1501,"lng":-73.3887,"url":"https://www.onthesnow.com/new-york/ski-butternut/skireport"},
    {"name":"Titus Mountain","state":"NY","lat":44.8501,"lng":-74.5651,"url":"https://www.onthesnow.com/new-york/titus-mountain/skireport"},
    {"name":"Oak Mountain","state":"NY","lat":43.3948,"lng":-74.4132,"url":"https://www.onthesnow.com/new-york/oak-mountain/skireport"},
    {"name":"West Mountain","state":"NY","lat":43.3426,"lng":-73.6518,"url":"https://www.onthesnow.com/new-york/west-mountain/skireport"},
    {"name":"Willard Mountain","state":"NY","lat":43.2376,"lng":-73.4629,"url":"https://www.onthesnow.com/new-york/willard-mountain/skireport"},

    # More Pennsylvania
    {"name":"Big Boulder","state":"PA","lat":41.0851,"lng":-75.6529,"url":"https://www.onthesnow.com/pennsylvania/big-boulder/skireport"},
    {"name":"Spring Mountain Ski Area","state":"PA","lat":40.2465,"lng":-75.4276,"url":"https://www.onthesnow.com/pennsylvania/spring-mountain-ski-area/skireport"},
    {"name":"Whitetail Resort","state":"PA","lat":39.7729,"lng":-77.5865,"url":"https://www.onthesnow.com/pennsylvania/whitetail-resort/skireport"},

    # Additional western resorts
    {"name":"Sundance Mountain Resort","state":"UT","lat":40.3920,"lng":-111.5887,"url":"https://www.onthesnow.com/utah/sundance/skireport"},

    # More Oregon
    {"name":"Mt. Ashland Ski Area","state":"OR","lat":42.0776,"lng":-122.7146,"url":"https://www.onthesnow.com/oregon/mount-ashland/skireport"},
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
