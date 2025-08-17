# Real-Time Sensor Pipeline (AirQuality)

A small, production-style pipeline that:
- watches `data/incoming/` for CSVs,
- validates + transforms AirQuality data,
- stores raw rows + per-file aggregates in PostgreSQL,
- routes files to `processed/` or `quarantine/`.

---

## Quickstart

```powershell
# 1) Start Postgres
docker compose up -d

# 2) Create venv + install deps (Windows)
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

# 3) Create local env (do NOT commit .env)
copy .env.example .env
# (Ensure PGPASSWORD and other values are set in .env)

# 4) Run the poller (checks inbox every 5s)
python -m src.main
### One-shot mode
```powershell
python -m src.main --once

