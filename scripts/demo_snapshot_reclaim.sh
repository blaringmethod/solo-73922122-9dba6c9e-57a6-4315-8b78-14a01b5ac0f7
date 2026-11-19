#!/usr/bin/env bash
# Snapshot-reclaim demo (runs against the Docker Compose environment):
#   1. a live snapshot serves complete aggregate data;
#   2. once released, the same snapshot refuses queries (404) — a successful
#      response can never come from a reclaimed snapshot;
#   3. re-sending the create request for the same round is rejected (409)
#      within the site's terminal-state retention window — no silent swap to
#      a new snapshot — while the freed slot immediately serves a new round;
#   4. end-to-end: if a site's snapshot is reclaimed mid-round, the
#      coordinator fails the round instead of publishing a curve that would
#      silently count the site as zero.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COORD="${COORD:-http://localhost:8000}"
A="${SITE_A:-http://localhost:8001}"

echo "== create a snapshot directly on site A =="
SID=$(curl -s -X POST "$A/snapshots" -H 'Content-Type: application/json' \
  -d '{"round_id": "demo-reclaim"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["snapshot_id"])')
echo "snapshot_id=$SID"

echo "== live snapshot serves complete aggregate data =="
curl -s "$A/snapshots/$SID/death-days" \
  | python3 -c 'import sys,json; d=json.load(sys.stdin)["death_days"]; print(f"  death-days: {len(d)} distinct days, first five: {d[:5]}")'
curl -s -X POST "$A/snapshots/$SID/aggregate" -H 'Content-Type: application/json' -d '{"points":[1,200,400]}' \
  | python3 -c 'import sys,json; [print("  t=%4d  n_risk=%4d  d=%d" % (r["t"], r["n_risk"], r["d"])) for r in json.load(sys.stdin)["rows"]]'

echo "== release the snapshot =="
curl -s -o /dev/null -w "  DELETE /snapshots/{id} -> HTTP %{http_code}\n" -X DELETE "$A/snapshots/$SID"

echo "== a released snapshot refuses subsequent queries =="
curl -s -o /dev/null -w "  GET  death-days -> HTTP %{http_code}\n" "$A/snapshots/$SID/death-days"
curl -s -o /dev/null -w "  POST aggregate  -> HTTP %{http_code}\n" \
  -X POST "$A/snapshots/$SID/aggregate" -H 'Content-Type: application/json' -d '{"points":[1]}'

echo "== re-sending the create request for the same round is rejected =="
curl -s -o /dev/null -w "  POST /snapshots round=demo-reclaim -> HTTP %{http_code}\n" \
  -X POST "$A/snapshots" -H 'Content-Type: application/json' -d '{"round_id": "demo-reclaim"}'

echo "== the freed slot is usable by a new round =="
curl -s -o /dev/null -w "  POST /snapshots round=demo-reclaim-new -> HTTP %{http_code}\n" \
  -X POST "$A/snapshots" -H 'Content-Type: application/json' -d '{"round_id": "demo-reclaim-new"}'
NEWSID=$(curl -s "$A/snapshots" | python3 -c 'import sys,json; print([s["snapshot_id"] for s in json.load(sys.stdin)["active"] if s["round_id"]=="demo-reclaim-new"][0])')
curl -s -o /dev/null -X DELETE "$A/snapshots/$NEWSID"

echo "== end-to-end: a round whose snapshot is reclaimed mid-flight fails =="
RID=$(curl -s -X POST "$COORD/rounds" | python3 -c 'import sys,json;print(json.load(sys.stdin)["round_id"])')
echo "  round_id=$RID"
sleep 2   # site A's snapshot exists; site B is still slow (5s per call)
ASID=$(curl -s "$A/snapshots" | python3 -c "import sys,json; print([s['snapshot_id'] for s in json.load(sys.stdin)['active'] if s['round_id']=='$RID'][0])")
echo "  expiring site A's snapshot $ASID mid-round (simulates TTL reclaim)"
docker compose -f "$ROOT/docker-compose.yml" exec -T site-a-db \
  psql -U sitea -d site_a -c "UPDATE snapshots SET expires_at = now() - interval '1 hour' WHERE id = '$ASID';" >/dev/null
while true; do
  ST=$(curl -s "$COORD/rounds/$RID" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
  echo "  status: $ST"
  case "$ST" in succeeded|failed|cancelled) break;; esac
  sleep 1
done
curl -s "$COORD/rounds/$RID" | python3 -c '
import sys, json
r = json.load(sys.stdin)
assert r["status"] == "failed", r
assert "result" not in r, r
print("  round failed as required; no curve published")
print("  error:", r.get("error"))
'
echo "== site slots after the failed round =="
echo "site A: $(curl -s "$A/snapshots" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["active_count"], "active")')"
