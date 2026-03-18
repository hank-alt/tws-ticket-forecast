#!/usr/bin/env python3
import os, re, json, requests, csv, io
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
    page = 1
    while True:
        r = requests.get(
            "https://slack.com/api/files.list",
            headers=HEADERS,
            params={"channel": CHANNEL_ID, "types": "csv", "count": 100, "page": page}
        ).json()
        if not r.get("ok"):
            print(f"files.list error: {r.get('error')}")
            break
        for f in r.get("files", []):
            name = f.get("name", "")
            if "TicketReservation" in name and "Full" in name and name.endswith(".csv"):
                url = f.get("url_private_download") or f.get("url_private")
                if url:
                    files.append({"name": name, "url": url, "id": f.get("id")})
        paging = r.get("paging", {})
        if page >= paging.get("pages", 1):
            break
        page += 1
        if page > 30:
            break
    seen = set()
    unique = []
    for f in files:
        m = re.search(r'\d{8}_\d{6}', f["name"])
        key = m.group(0) if m else f["name"]
        if key not in seen:
            seen.add(key)
            unique.append({**f, "key": key})
    unique.sort(key=lambda x: x["key"])
    print(f"Found {len(unique)} Full CSV files")
    return unique

def download_csv(file_info):
    try:
        info_r = requests.get(
            "https://slack.com/api/files.info",
            headers=HEADERS,
            params={"file": file_info["id"]}
        ).json()
        if info_r.get("ok"):
            url = info_r["file"].get("url_private_download") or info_r["file"].get("url_private")
        else:
            url = file_info["url"]
        r = requests.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
        if r.status_code != 200:
            return []
        content = r.text.strip()
        if content.startswith("<!"):
            return []
        reader = csv.DictReader(io.StringIO(content))
        rows = []
        for row in reader:
            clean = {k.strip(): v.strip() for k, v in row.items() if k}
            if clean:
                rows.append(clean)
        return rows
    except Exception as e:
        print(f"  Error: {e}")
        return []

def analyze(files_meta):
    if not files_meta:
        return None
    all_snapshots = []
    print("Downloading CSVs...")
    for i, fm in enumerate(files_meta):
        rows = download_csv(fm)
        if rows:
            m = re.search(r'(\d{8})_(\d{6})', fm["name"])
            if m:
                sdt = datetime.strptime(m.group(1)+m.group(2), "%Y%m%d%H%M%S")
                all_snapshots.append({"dt": sdt, "rows": rows})
                if i == len(files_meta) - 1:
                    print(f"  Latest: {len(rows)} rows, keys: {list(rows[0].keys()) if rows else []}")
        if (i+1) % 10 == 0:
            print(f"  {i+1}/{len(files_meta)} done...")
    if not all_snapshots:
        print("No snapshots loaded!")
        return None
    all_snapshots.sort(key=lambda x: x["dt"])
    print(f"Loaded {len(all_snapshots)} snapshots")
    first_seen = {}
    for snap in all_snapshots:
        for row in snap["rows"]:
            key = f"{row.get('Date','')}_{ row.get('Start Time','')}"
            if key not in first_seen or snap["dt"] < first_seen[key]:
                first_seen[key] = snap["dt"]
    latest = all_snapshots[-1]
    latest_dt_str = latest["dt"].strftime("%Y.%m.%d %H:%M")
    snap_prog = []
    for snap in all_snapshots:
        total = sum(int(r.get("# of Tickets Sold", 0) or 0) for r in snap["rows"] if str(r.get("# of Tickets Sold","")).strip().isdigit())
        snap_prog.append({"dt": snap["dt"].strftime("%m/%d %H:%M"), "total": total, "isNew": False})
    cutoff_2cha = datetime(2026, 3, 1)
    g1_rows = [r for r in latest["rows"] if first_seen.get(f"{r.get('Date','')}_{ r.get('Start Time','')}", datetime(2099,1,1)) < cutoff_2cha]
    dow_sum, dow_cnt = {}, {}
    for r in g1_rows:
        d = get_dow(r.get("Date",""))
        try:
            sold = int(r.get("# of Tickets Sold", 0) or 0)
            seats = int(r.get("# of Seats", 152) or 152)
        except:
            continue
        if d < 0 or seats == 0: continue
        dow_sum[d] = dow_sum.get(d, 0) + sold/seats
        dow_cnt[d] = dow_cnt.get(d, 0) + 1
    dow_bench_live = {d: (dow_sum[d]/dow_cnt[d] if d in dow_sum and dow_cnt.get(d,0)>0 else DOW_BENCH[d]) for d in range(7)}
    now_dt = latest["dt"].replace(hour=0, minute=0, second=0, microsecond=0)
    past_map, fut_map = {}, {}
    for r in latest["rows"]:
        sdt = show_dt(r.get("Date",""))
        if not sdt: continue
        try:
            sold  = int(r.get("# of Tickets Sold", 0) or 0)
            seats = int(r.get("# of Seats", 152) or 152)
        except:
            continue
        dow = get_dow(r.get("Date",""))
        key = str(r.get("Date","")).strip()
        if sdt < now_dt:
            if key not in past_map:
                past_map[key] = {"sold":0,"cap":0,"dow":dow,"date_str":sdt.strftime("%m/%d")}
            past_map[key]["sold"] += sold
            past_map[key]["cap"]  += seats
        else:
            if key not in fut_map:
                fut_map[key] = {"sold":0,"cap":0,"dow":dow,"shows":0,"daysLeft":(sdt-now_dt).days,"date_str":sdt.strftime("%m/%d")}
            fut_map[key]["sold"]  += sold
            fut_map[key]["cap"]   += seats
            fut_map[key]["shows"] += 1
    dow_labels = ["월","화","수","목","금","토","일"]
    past_daily = [{"date":f"{pm['date_str']}({dow_labels[pm['dow']] if 0<=pm['dow']<=6 else '?'})","sold":pm["sold"],"cap":pm["cap"],"dow":pm["dow"]} for k,pm in sorted(past_map.items())]
    future_shows = []
    for k,fm in sorted(fut_map.items()):
        bench = dow_bench_live.get(fm["dow"], 0.85)
        vi = min((fm["sold"]/fm["cap"])/VEL_AT_NOW, 1.0) if fm["cap"] else 0
        dl = dow_labels[fm["dow"]] if 0<=fm["dow"]<=6 else "?"
        future_shows.append({"date":f"{fm['date_str']}({dl})","dow":fm["dow"],"shows":fm["shows"],"curSold":fm["sold"],"cap":fm["cap"],"daysLeft":fm["daysLeft"],"bench":round(bench,4),"vi":round(vi,4)})
    current_total = sum(pm["sold"] for pm in past_map.values()) + sum(fm["sold"] for fm in fut_map.values())
    total_cap = sum(pm["cap"] for pm in past_map.values()) + sum(fm["cap"] for fm in fut_map.values())
    print(f"current_total: {current_total}, total_cap: {total_cap}")
    hm_data = {}
    for r in latest["rows"]:
        sdt = show_dt(r.get("Date",""))
        if not sdt: continue
        dow = get_dow(r.get("Date",""))
        ts = str(r.get("Start Time","")).strip().zfill(4)
        is_we = dow in [5,6]
        slot = (SLOT_MAP_WE if is_we else SLOT_MAP_WD).get(ts)
        if slot is None: continue
        try:
            sold  = int(r.get("# of Tickets Sold",0) or 0)
            seats = int(r.get("# of Seats",152) or 152)
        except:
            continue
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
            hm_final[d][s] = {"occ":round(occ,4),"remaining":rem,"avgRemaining":round(rem/c["n"]),"sold":c["sold"],"seats":c["seats"],"n":c["n"]}
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
        "meta": {"generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "latest_snap": latest_dt_str, "n_snapshots": len(all_snapshots), "current_total": current_total, "total_cap": total_cap},
        "snapshot_progression": snap_prog,
        "past_daily": past_daily,
        "future_shows": future_shows,
        "dow_bench": {str(k):round(v,4) for k,v in dow_bench_live.items()},
        "heatmap": hm_final,
        "heatmap_dow_avg": dow_avg_hm,
        "heatmap_slot_avg": slot_avg_hm,
    }

if __name__ == "__main__":
    print("Fetching CSV files from Slack...")
    files = fetch_csv_files()
    if not files:
        print("No files found, exiting.")
        exit(1)
    data = analyze(files)
    if data:
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"✅ Saved — snapshots:{data['meta']['n_snapshots']}, total:{data['meta']['current_total']:,}, cap:{data['meta']['total_cap']:,}")
    else:
        print("Analysis failed.")
        exit(1)
