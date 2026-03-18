#!/usr/bin/env python3
import os, re, json, requests, io, csv
from datetime import datetime

SLACK_TOKEN  = os.environ["SLACK_TOKEN"]
CHANNEL_ID   = os.environ.get("SLACK_CHANNEL_ID", "C0AFW1CECM9")
HEADERS      = {"Authorization": f"Bearer {SLACK_TOKEN}"}
OUTPUT_PATH  = "data.json"

VEL_AT_NOW = 0.9204
DOW_BENCH  = {0:0.8713,1:0.7081,2:0.8588,3:0.8631,4:0.9610,5:0.9893,6:0.9951}
SLOT_MAP_WD = {'0930':1,'1110':2,'1250':3,'1430':4,'1610':5,'1750':6,'1930':7,'2110':8}
SLOT_MAP_WE = {'0900':1,'1040':2,'1220':3,'1400':4,'1540':5,'1720':6,'1900':7,'2040':8,'2220':9}

def get_dow(date_str):
    try:
        return datetime.strptime(str(date_str).strip(), "%Y%m%d").weekday()
    except:
        return -1

def show_dt(date_str):
    try:
        return datetime.strptime(str(date_str).strip(), "%Y%m%d")
    except:
        return None

def fetch_csv_files():
    files = []
    cursor = None
    page = 0
    while True:
        page += 1
        params = {"channel": CHANNEL_ID, "limit": 200}
        if cursor:
            params["cursor"] = cursor
        r = requests.get("https://slack.com/api/conversations.history",
                         headers=HEADERS, params=params).json()
        if not r.get("ok"):
            print(f"API error: {r.get('error')}")
            break
        for msg in r.get("messages", []):
            for f in msg.get("files", []):
                name = f.get("name", "")
                if "TicketReservation" in name and "Full" in name and name.endswith(".csv"):
                    # url_private_download 우선, 없으면 url_private
                    url = f.get("url_private_download") or f.get("url_private")
                    file_id = f.get("id", "")
                    if url:
                        files.append({"name": name, "url": url, "id": file_id})
        cursor = r.get("response_metadata", {}).get("next_cursor")
        if not cursor or page > 20:
            break

    seen, unique = set(), []
    for f in files:
        m = re.search(r'\d{8}_\d{6}', f["name"])
        key = m.group(0) if m else f["name"]
        if key not in seen:
            seen.add(key)
            unique.append({**f, "key": key})
    unique.sort(key=lambda x: x["key"])
    print(f"Found {len(unique)} Full CSV files")
    return unique

def download_csv_by_id(file_id):
    """files.info API로 다운로드 URL 가져와서 시도"""
    try:
        info = requests.get("https://slack.com/api/files.info",
                            headers=HEADERS, params={"file": file_id}).json()
        if not info.get("ok"):
            return None
        url = info["file"].get("url_private_download") or info["file"].get("url_private")
        if not url:
            return None
        r = requests.get(url, params={"t": SLACK_TOKEN}, timeout=30, allow_redirects=True)
        return r
    except Exception as e:
        return None

def parse_csv_response(r):
    """response -> rows 파싱"""
    try:
        if r.status_code != 200:
            return []
        content_type = r.headers.get("Content-Type", "")
        if "html" in content_type or "<html" in r.text[:200].lower():
            return []
        content = r.content.decode('utf-8-sig').strip()
        if not content:
            return []
        reader = csv.DictReader(io.StringIO(content))
        rows = [{k.strip(): v.strip() for k, v in row.items() if k} for row in reader]
        return rows
    except Exception as e:
        return []

def download_csv(url, file_id):
    """URL 직접 시도 → 실패시 files.info API로 재시도"""
    try:
        r = requests.get(url, headers=HEADERS, params={"token": SLACK_TOKEN}, timeout=30, allow_redirects=True)
        rows = parse_csv_response(r)
        if rows:
            return rows
    except:
        pass
    # 재시도: files.info API
    if file_id:
        r2 = download_csv_by_id(file_id)
        if r2:
            return parse_csv_response(r2)
    return []

def safe_int(val, default=0):
    try:
        return int(str(val).strip().replace(',', ''))
    except:
        return default

def analyze(files_meta):
    if not files_meta:
        return None

    # 첫 번째 파일 다운로드 테스트
    test = files_meta[-1]
    print(f"Testing download: {test['name']}")
    r_test = requests.get(test["url"], headers=HEADERS, timeout=30, allow_redirects=True)
    print(f"  status: {r_test.status_code}")
    print(f"  content-type: {r_test.headers.get('Content-Type','?')}")
    print(f"  content preview: {r_test.text[:200]!r}")

    all_snapshots = []
    for fm in files_meta:
        rows = download_csv(fm["url"], fm.get("id",""))
        if rows:
            m = re.search(r'(\d{8})_(\d{6})', fm["name"])
            if m:
                sdt = datetime.strptime(m.group(1)+m.group(2), "%Y%m%d%H%M%S")
                all_snapshots.append({"dt": sdt, "rows": rows})

    if not all_snapshots:
        print("No snapshots loaded after all attempts!")
        return None

    print(f"Loaded {len(all_snapshots)} snapshots")
    latest = all_snapshots[-1]
    print(f"Latest: {latest['dt']}, rows: {len(latest['rows'])}")
    if latest["rows"]:
        print(f"Columns: {list(latest['rows'][0].keys())}")
        print(f"Sample: {dict(list(latest['rows'][0].items())[:3])}")

    # booking_open per show
    first_seen = {}
    for snap in all_snapshots:
        for row in snap["rows"]:
            key = f"{row.get('Date','')}_{ row.get('Start Time','')}"
            if key not in first_seen or snap["dt"] < first_seen[key]:
                first_seen[key] = snap["dt"]

    latest_dt_str = latest["dt"].strftime("%Y.%m.%d %H:%M")

    snap_prog = []
    for snap in all_snapshots:
        total = sum(safe_int(r.get("# of Tickets Sold", 0)) for r in snap["rows"])
        snap_prog.append({"dt": snap["dt"].strftime("%m/%d %H:%M"), "total": total, "isNew": False})

    cutoff_2cha = datetime(2026, 3, 1)
    g1_rows = [r for r in latest["rows"]
               if first_seen.get(f"{r.get('Date','')}_{ r.get('Start Time','')}", datetime(2099,1,1)) < cutoff_2cha]

    dow_sum, dow_cnt = {}, {}
    for r in g1_rows:
        d = get_dow(r.get("Date",""))
        sold  = safe_int(r.get("# of Tickets Sold", 0))
        seats = safe_int(r.get("# of Seats", 152), 152) or 152
        if d < 0 or seats == 0: continue
        dow_sum[d] = dow_sum.get(d, 0) + sold/seats
        dow_cnt[d] = dow_cnt.get(d, 0) + 1

    dow_bench_live = {}
    for d in range(7):
        if d in dow_sum and dow_cnt.get(d, 0) > 0:
            dow_bench_live[d] = dow_sum[d] / dow_cnt[d]
        else:
            dow_bench_live[d] = DOW_BENCH[d]

    now_dt = latest["dt"].replace(hour=0, minute=0, second=0, microsecond=0)
    past_map, fut_map = {}, {}
    dow_labels = ["월","화","수","목","금","토","일"]

    for r in latest["rows"]:
        sdt = show_dt(r.get("Date",""))
        if not sdt: continue
        sold  = safe_int(r.get("# of Tickets Sold", 0))
        seats = safe_int(r.get("# of Seats", 152), 152) or 152
        dow = get_dow(r.get("Date",""))
        key = str(r.get("Date","")).strip()

        if sdt < now_dt:
            if key not in past_map:
                past_map[key] = {"sold":0,"cap":0,"dow":dow,"date_str":sdt.strftime("%m/%d")}
            past_map[key]["sold"] += sold
            past_map[key]["cap"]  += seats
        else:
            if key not in fut_map:
                fut_map[key] = {"sold":0,"cap":0,"dow":dow,"shows":0,
                                "daysLeft":(sdt-now_dt).days,"date_str":sdt.strftime("%m/%d")}
            fut_map[key]["sold"]  += sold
            fut_map[key]["cap"]   += seats
            fut_map[key]["shows"] += 1

    past_daily = []
    for k in sorted(past_map):
        pm = past_map[k]
        dl = dow_labels[pm["dow"]] if 0<=pm["dow"]<=6 else "?"
        past_daily.append({"date":f"{pm['date_str']}({dl})","sold":pm["sold"],"cap":pm["cap"],"dow":pm["dow"]})

    future_shows = []
    for k in sorted(fut_map):
        fm = fut_map[k]
        bench = dow_bench_live.get(fm["dow"], 0.85)
        vi = min((fm["sold"]/fm["cap"])/VEL_AT_NOW, 1.0) if fm["cap"] else 0
        dl = dow_labels[fm["dow"]] if 0<=fm["dow"]<=6 else "?"
        future_shows.append({
            "date":f"{fm['date_str']}({dl})","dow":fm["dow"],"shows":fm["shows"],
            "curSold":fm["sold"],"cap":fm["cap"],"daysLeft":fm["daysLeft"],
            "bench":round(bench,4),"vi":round(vi,4)
        })

    current_total = sum(pm["sold"] for pm in past_map.values()) + sum(fm["sold"] for fm in fut_map.values())
    total_cap     = sum(pm["cap"]  for pm in past_map.values()) + sum(fm["cap"]  for fm in fut_map.values())
    print(f"current_total={current_total}, total_cap={total_cap}")

    hm_data = {}
    for r in latest["rows"]:
        sdt = show_dt(r.get("Date",""))
        if not sdt: continue
        dow  = get_dow(r.get("Date",""))
        ts   = str(r.get("Start Time","")).strip().zfill(4)
        is_we = dow in [5,6]
        slot = (SLOT_MAP_WE if is_we else SLOT_MAP_WD).get(ts)
        if slot is None: continue
        sold  = safe_int(r.get("# of Tickets Sold", 0))
        seats = safe_int(r.get("# of Seats", 152), 152) or 152
        d_s, s_s = str(dow), str(slot)
        if d_s not in hm_data: hm_data[d_s] = {}
        if s_s not in hm_data[d_s]: hm_data[d_s][s_s] = {"sold":0,"seats":0,"n":0}
        hm_data[d_s][s_s]["sold"]  += sold
        hm_data[d_s][s_s]["seats"] += seats
        hm_data[d_s][s_s]["n"]     += 1

    hm_final = {}
    for d in hm_data:
        hm_final[d] = {}
        for s in hm_data[d]:
            c = hm_data[d][s]
            occ = c["sold"]/c["seats"] if c["seats"] else 0
            rem = c["seats"] - c["sold"]
            hm_final[d][s] = {"occ":round(occ,4),"remaining":rem,
                               "avgRemaining":round(rem/c["n"]),"sold":c["sold"],"seats":c["seats"],"n":c["n"]}

    dow_avg_hm, slot_avg_hm = {}, {}
    for d in hm_final:
        occs = [hm_final[d][s]["occ"] for s in hm_final[d]]
        rems = [hm_final[d][s]["avgRemaining"] for s in hm_final[d]]
        dow_avg_hm[d] = {"occ":round(sum(occs)/len(occs),4),"avgRemaining":round(sum(rems)/len(rems))}
    for s in set(s for d in hm_final for s in hm_final[d]):
        vals = [hm_final[d][s]["occ"] for d in hm_final if s in hm_final[d]]
        rems = [hm_final[d][s]["avgRemaining"] for d in hm_final if s in hm_final[d]]
        if vals:
            slot_avg_hm[s] = {"occ":round(sum(vals)/len(vals),4),"avgRemaining":round(sum(rems)/len(rems))}

    return {
        "meta": {
            "generated_at":  datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "latest_snap":   latest_dt_str,
            "n_snapshots":   len(all_snapshots),
            "current_total": current_total,
            "total_cap":     total_cap,
        },
        "snapshot_progression": snap_prog,
        "past_daily":           past_daily,
        "future_shows":         future_shows,
        "dow_bench":            {str(k):round(v,4) for k,v in dow_bench_live.items()},
        "heatmap":              hm_final,
        "heatmap_dow_avg":      dow_avg_hm,
        "heatmap_slot_avg":     slot_avg_hm,
    }

if __name__ == "__main__":
    print("Fetching CSV files from Slack...")
    files = fetch_csv_files()
    if not files:
        print("No files found, exiting.")
        exit(1)
    print("Analyzing data...")
    data = analyze(files)
    if data:
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"✅ Saved — snapshots:{data['meta']['n_snapshots']}, total:{data['meta']['current_total']:,}")
    else:
        print("Analysis failed.")
        exit(1)
