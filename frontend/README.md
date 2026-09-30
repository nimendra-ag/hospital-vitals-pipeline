# Ward Patient Monitor (frontend)

A plain-language dashboard for clinicians who don't want to read Grafana —
just patients, their current vitals, and whether anything needs attention.
It answers the project's business question directly: *which patients show
concerning vital-sign trends right now, and how do yesterday's lab results
change the risk picture?*

Built with Next.js (App Router) + TypeScript + Tailwind CSS + Recharts. It
is a thin client over the FastAPI serving layer (`serving/api.py`) — no
business logic lives here beyond how to color a number.

## Pages

| Route | Purpose |
|---|---|
| `/` | Ward overview — every patient as a card, worst-first, current vitals color-coded, active alert count |
| `/patients/[id]` | One patient: current vitals, trend charts, the daily risk explanation in plain sentences, recent lab results |
| `/alerts` | All unacknowledged alerts, grouped by severity, with an Acknowledge button |
| `/reports` | The daily consolidated risk report — every patient, worst-risk-first, expandable to see *why* |

All pages poll the API (5s for live vitals/alerts, 15–30s for lab/report
data, which only change once per simulated day) rather than using
WebSockets — simple, and matches how infrequently the underlying batch
data actually changes.

## Running locally (without Docker)

```bash
cd frontend
npm install
cp .env.local.example .env.local   # points at http://localhost:8000 by default
npm run dev
```

Open http://localhost:3001. The FastAPI server (`python -m serving.api`)
must already be running — this dashboard has no backend of its own.

## Running via Docker Compose

```bash
docker-compose up -d frontend
```

`NEXT_PUBLIC_API_BASE_URL` is baked in at build time (Next.js inlines
`NEXT_PUBLIC_*` vars into the client bundle) as `http://localhost:8000` —
correct as long as the FastAPI server is reachable at that address from
the browser, which it is since `serving/api.py` isn't containerized in
this project and publishes port 8000 directly to the host.

## CORS

`serving/api.py` allows the origin in `CORS_ALLOW_ORIGINS` (`.env`,
defaults to `http://localhost:3001`) to call it directly from the browser.
If you run the frontend on a different port, update that variable too.
