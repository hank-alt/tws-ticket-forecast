# TWS Korea Ticket Forecast Dashboard

Slack의 `#ticket-sales-status-tws-kr` 채널에 올라오는 CSV를 자동으로 수집해 대시보드를 갱신합니다.

---

## 🚀 초기 세팅 (15분)

### 1. 이 레포를 GitHub에 올리기

```bash
# GitHub에서 새 레포 생성 후:
git init
git add .
git commit -m "init"
git remote add origin https://github.com/YOUR_ORG/tws-ticket-forecast.git
git push -u origin main
```

### 2. Slack Bot Token 등록

1. GitHub 레포 → **Settings** → **Secrets and variables** → **Actions**
2. **New repository secret** 클릭
3. Name: `SLACK_TOKEN`
4. Value: Slack Bot Token (`xoxb-...`)

> 토큰에 필요한 권한: `channels:history`, `files:read`

### 3. GitHub Pages 활성화

1. GitHub 레포 → **Settings** → **Pages**
2. Source: **Deploy from a branch**
3. Branch: `main` / `/ (root)` 선택 → Save

약 1분 후 `https://YOUR_ORG.github.io/tws-ticket-forecast/` 에서 대시보드 접근 가능

### 4. 슬랙 채널에 URL 고정 핀 등록

```
https://YOUR_ORG.github.io/tws-ticket-forecast/
```

채널 설명이나 북마크에 등록해두면 팀원 누구나 클릭 한 번으로 최신 대시보드 확인 가능

---

## ⚙️ 작동 방식

```
매시 :15분
    → GitHub Actions 실행
    → Slack API로 #ticket-sales-status-tws-kr 에서 Full CSV 전체 수집
    → 분석 실행 (로지스틱 속도 곡선 + 요일 벤치마크)
    → data.json 생성 후 레포에 커밋
    → GitHub Pages 자동 배포

대시보드 (브라우저)
    → 열릴 때 data.json 자동 로드
    → 30분마다 백그라운드에서 자동 갱신
    → 수동 CSV 업로드도 여전히 가능
```

---

## 📁 파일 구조

```
.
├── index.html              # 대시보드 (브라우저에서 바로 열림)
├── fetch_and_analyze.py    # Slack 수집 + 분석 스크립트
├── data.json               # 최신 분석 결과 (Actions가 자동 생성)
└── .github/
    └── workflows/
        └── update.yml      # 1시간마다 실행되는 GitHub Action
```

---

## 🔧 수동 실행

GitHub Actions 탭 → **Update TWS Ticket Forecast** → **Run workflow**

---

## 📊 대시보드 기능

- 예매 진행 추이 (스냅샷별 누적 판매량)
- 로지스틱 속도 곡선 기반 최종 판매량 예측
- 확률 구간 분포 (P5 ~ P95)
- 요일 × 회차별 좌석 점유율 히트맵
- 슬라이더로 예측 파라미터 실시간 조정
