# OPERATIONS — Real-Time Sensor Pipeline

A hands-on runbook for daily ops: start/stop, health checks, triage, backfills, schema changes, and performance hygiene.

---

## 1) Start / Stop / Status

### Services (PostgreSQL via Docker)
```powershell
# start
docker compose up -d

# status
docker ps
docker logs rt_postgres --tail 80

# stop
docker compose down
