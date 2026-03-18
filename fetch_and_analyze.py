#!/usr/bin/env python3
"""
TWS Korea Ticket Forecast — Slack 자동 수집 & 분석 스크립트
GitHub Actions에서 1시간마다 실행됨

필요 환경변수:
  SLACK_TOKEN : Slack Bot User OAuth Token (xoxb-...)
  SLACK_CHANNEL_ID : C0AFW1CECM9
"""

import os, re, json, io, requests
from datetime import datetime
import numpy as np

SLACK_TOKEN      = os.environ["SLACK_TOKEN"]
CHANNEL_ID       = os.environ.get("SLACK_CHANNEL_ID", "C0AFW1CECM9")
HEADERS          = {"Authorization": f"Bearer {SLACK_TOKEN}"}
OUTPUT_PATH      = "data.json"

# ── 1. Slack에서 Full CSV 파일 목록 수집 ─────────────────────────────────────
def fetch_csv_files():
    """채널에서 TWS TicketReservation Full CSV 파일들을 모두 가져옴"""
    files = []
    cursor = None
    while True:
        params = {
            "channel": CHANNEL_ID,
            "types": "csv",
            "count": 100,
        }
        if cursor:
            params["cursor"] = cursor

        r = requests.get(
            "https://slack.com/api/conversations.history",
            headers=HEADERS,
            params={"channel": CHANNEL_ID, "limit": 200, **({"cursor": cursor} if cursor else {})}
        ).json()

        if not r.get("ok"):
            print(f"Slack API error: {r.get('error')}")
            break

        for msg in r.get("messages", []):
            for f in msg.get("files", []):
                name = f.get("name", "")
                if "TicketReservation" in name and "Full" in name and name.endswith(".csv"):
                    files.append({
                        "name": name,
                        "url": f.get("url_private_download"),
                        "ts": msg.get("ts")
                    })

        cursor = r.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break

    # 날짜순 정렬
    def snap_dt(f):
        m = re.search(r'(\d{8}_\d{6})', f["name"])
        return m.group(1) if m else "0"

    files.sort(key=snap_dt)
    print(f"Found {len(files)} Full CSV files")
    return files


# ── 2. CSV 다운로드 & 파싱 ────────────────────────────────────────────────────
def download_csv(url):
    r = requests.get(url, headers=HEADERS)
    lines = r.text.splitlines()
    if not lines:
        return []
    headers_row = [h.strip() for h in lines[0].split(",")]
    rows = []
    for line in lines[1:]:
        vals = [v.strip() for v in line.split(",")]
        if len(vals) >= len(headers_row):
            rows.append(dict(zip(headers_row, vals)))
    return rows


# ── 3. 분석 엔진 ──────────────────────────────────────────────────────────────
VEL_AT_NOW = 0.9204   # 2차 예매 day 6.56 기준 velocity
DOW_BENCH  = {0:0.8713,1:0.7081,2:0.8588,3:0.8631,4:0.9610,5:0.9893,6:0.9951}

SLOT_MAP_WD = {'0930':1,'1110':2,'1250':3,'1430':4,'1610':5,'1750':6,'1930':7,'2110':8}
SLOT_MAP_WE = {'0900':1,'1040':2,'1220':3,'1400':4,'1540':5,'1720':6,'1900':7,'2040':8,'2220':9}

def analyze(files_meta):
    """모든 스냅샷을 분석해서 대시보드용 data.json 생성"""

    # 모든 스냅샷 로드
    all_snapshots = []
    for fm in files_meta:
        rows = download_csv(fm["url"])
        if rows:
            m = re.search(r'(\d{8})_(\d{6})', fm["name"])
            if m:
                sdt = datetime.strptime(m.group(1)+m.group(2), "%Y%m%d%H%M%S")
                all_snapshots.append({"dt": sdt, "rows": rows, "name": fm["name"]})

    if not all_snapshots:
        print("No snapshots loaded!")
        return None

    # booking_open per show
    first_seen = {}
    for snap in all_snapshots:
        for row in snap["rows"]:
            key = f"{row.get('Date','')}_{ row.get('Start Time','')}"
            if key not in first_seen or snap["dt"] < first_seen[key]:
                first_seen[key] = snap["dt"]

    # 최신 스냅샷
    latest = all_snapshots[-1]
    latest_dt_str = latest["dt"].strftime("%Y.%m.%d %H:%M")

    def get_dow(date_str):
        """YYYYMMDD → 0=Mon..6=Sun"""
        try:
            d = datetime.strptime(str(date_str), "%Y%m%d")
            return (d.weekday())  # 0=Mon
        except:
            return -1

    def show_dt(date_str):
        try:
            return datetime.strptime(str(date_str), "%Y%m%d")
        except:
            return None

    # ── 스냅샷 진행 추이
    snap_prog = []
    for snap in all_snapshots:
        total = sum(int(r.get("# of Tickets Sold", 0) or 0) for r in snap["rows"])
        snap_prog.append({
            "dt":    snap["dt"].strftime("%m/%d %H:%M"),
            "total": total,
            "isNew": False
        })

    # ── 1차 DoW benchmark
    cutoff_2cha = datetime(2026, 3, 1)
    g1_rows = [r for r in latest["rows"]
               if first_seen.get(f"{r.get('Date','')}_{ r.get('Start Time','')}") < cutoff_2cha]

    dow_sum, dow_cnt = {}, {}
    for r in g1_rows:
        d = get_dow(r.get("Date",""))
        sold = int(r.get("# of Tickets Sold", 0) or 0)
        seats= int(r.get("# of Seats", 152) or 152)
        if d < 0 or seats == 0: continue
        dow_sum[d] = dow_sum.get(d, 0) + sold/seats
        dow_cnt[d] = dow_cnt.get(d, 0) + 1

    dow_bench_live = {d: dow_sum[d]/dow_cnt[d] for d in dow_sum}
    # fallback to hardcoded if not enough data
    for d in range(7):
        if d not in dow_bench_live:
            dow_bench_live[d] = DOW_BENCH[d]

    # ── Past & future shows (from latest snapshot)
    cutoff = datetime(2026, 3, 18)   # baseline for daysLeft; update dynamically
    # Use latest snapshot date as cutoff for future
    now_dt = latest["dt"].replace(hour=0, minute=0, second=0, microsecond=0)

    past_map, fut_map = {}, {}
    for r in latest["rows"]:
        sdt = show_dt(r.get("Date",""))
        if not sdt: continue
        sold  = int(r.get("# of Tickets Sold", 0) or 0)
        seats = int(r.get("# of Seats", 152) or 152)
        time_str = str(r.get("Start Time","")).zfill(4)
        dow   = get_dow(r.get("Date",""))
        key   = r.get("Date","")

        if sdt < now_dt:
            if key not in past_map:
                past_map[key] = {"sold":0,"cap":0,"dow":dow,"date_str":sdt.strftime("%m/%d")}
            past_map[key]["sold"] += sold
            past_map[key]["cap"]  += seats
        else:
            if key not in fut_map:
                fut_map[key] = {"sold":0,"cap":0,"dow":dow,"shows":0,
                                "daysLeft": (sdt-now_dt).days,
                                "date_str": sdt.strftime("%m/%d"),
                                "dow_name": ["월","화","수","목","금","토","일"][dow]}
            fut_map[key]["sold"]  += sold
            fut_map[key]["cap"]   += seats
            fut_map[key]["shows"] += 1

    past_daily = []
    for k in sorted(past_map):
        pm = past_map[k]
        occ = pm["sold"]/pm["cap"] if pm["cap"] else 0
        dow_label = ["월","화","수","목","금","토","일"][pm["dow"]] if 0<=pm["dow"]<=6 else "?"
        past_daily.append({
            "date":  f"{pm['date_str']}({dow_label})",
            "sold":  pm["sold"],
            "cap":   pm["cap"],
            "dow":   pm["dow"]
        })

    future_shows = []
    for k in sorted(fut_map):
        fm_row = fut_map[k]
        bench = dow_bench_live.get(fm_row["dow"], 0.85)
        vel_implied = min((fm_row["sold"]/fm_row["cap"]) / VEL_AT_NOW, 1.0) if fm_row["cap"] else 0
        dow_label = ["월","화","수","목","금","토","일"][fm_row["dow"]] if 0<=fm_row["dow"]<=6 else "?"
        future_shows.append({
            "date":     f"{fm_row['date_str']}({dow_label})",
            "dow":      fm_row["dow"],
            "shows":    fm_row["shows"],
            "curSold":  fm_row["sold"],
            "cap":      fm_row["cap"],
            "daysLeft": fm_row["daysLeft"],
            "bench":    round(bench, 4),
            "vi":       round(vel_implied, 4)
        })

    # ── 1차 확정, 2차 과거 합계
    past1_total = sum(r["sold"] for r in past_daily
                      if first_seen.get(list(past_map.keys())[past_daily.index(r)]+"_dummy",
                      datetime(2099,1,1)) < cutoff_2cha) if False else \
                  sum(int(r.get("# of Tickets Sold",0) or 0) for r in g1_rows)

    # Simpler: all past sold from latest
    total_past_sold = sum(pm["sold"] for pm in past_map.values())
    total_cap = sum(int(r.get("# of Seats",152) or 152) for r in latest["rows"])
    current_total = sum(int(r.get("# of Tickets Sold",0) or 0) for r in latest["rows"])

    # ── 히트맵 데이터
    hm_data = {}
    for r in latest["rows"]:
        sdt   = show_dt(r.get("Date",""))
        if not sdt: continue
        dow   = get_dow(r.get("Date",""))
        ts    = str(r.get("Start Time","")).zfill(4)
        is_we = dow in [5, 6]
        slot  = (SLOT_MAP_WE if is_we else SLOT_MAP_WD).get(ts)
        if slot is None: continue
        sold  = int(r.get("# of Tickets Sold", 0) or 0)
        seats = int(r.get("# of Seats", 152) or 152)
        d_str = str(dow)
        s_str = str(slot)
        if d_str not in hm_data: hm_data[d_str] = {}
        if s_str not in hm_data[d_str]:
            hm_data[d_str][s_str] = {"sold":0,"seats":0,"n":0}
        hm_data[d_str][s_str]["sold"]  += sold
        hm_data[d_str][s_str]["seats"] += seats
        hm_data[d_str][s_str]["n"]     += 1

    hm_final = {}
    for d in hm_data:
        hm_final[d] = {}
        for s in hm_data[d]:
            cell = hm_data[d][s]
            occ = cell["sold"]/cell["seats"] if cell["seats"] else 0
            rem = cell["seats"] - cell["sold"]
            hm_final[d][s] = {
                "occ": round(occ, 4),
                "remaining": rem,
                "avgRemaining": round(rem/cell["n"]),
                "sold":  cell["sold"],
                "seats": cell["seats"],
                "n":     cell["n"]
            }

    # dow avg & slot avg for heatmap
    dow_avg_hm, slot_avg_hm = {}, {}
    for d in hm_final:
        occs = [hm_final[d][s]["occ"] for s in hm_final[d]]
        rems = [hm_final[d][s]["avgRemaining"] for s in hm_final[d]]
        dow_avg_hm[d] = {"occ": round(sum(occs)/len(occs),4), "avgRemaining": round(sum(rems)/len(rems))}
    all_slots = set(s for d in hm_final for s in hm_final[d])
    for s in all_slots:
        vals = [hm_final[d][s]["occ"] for d in hm_final if s in hm_final[d]]
        rems = [hm_final[d][s]["avgRemaining"] for d in hm_final if s in hm_final[d]]
        if vals:
            slot_avg_hm[s] = {"occ": round(sum(vals)/len(vals),4), "avgRemaining": round(sum(rems)/len(rems))}

    # ── 최종 output
    result = {
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
        "dow_bench":            {str(k): round(v,4) for k,v in dow_bench_live.items()},
        "heatmap":              hm_final,
        "heatmap_dow_avg":      dow_avg_hm,
        "heatmap_slot_avg":     slot_avg_hm,
    }
    return result


# ── 4. Main ───────────────────────────────────────────────────────────────────
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
        print(f"✅ data.json saved — {data['meta']['n_snapshots']} snapshots, "
              f"current total: {data['meta']['current_total']:,}")
    else:
        print("Analysis failed.")
        exit(1)
