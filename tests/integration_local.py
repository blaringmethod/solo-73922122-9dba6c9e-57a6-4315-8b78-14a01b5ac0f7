#!/usr/bin/env python3
"""End-to-end self-test: runs the coordinator and both site nodes as real
HTTP services (uvicorn servers in threads, SQLite backends) inside one
process, and exercises the federated KM workflow:

  A. happy path + curve verified against an independent computation
  B. slow site: status queries and cancel stay responsive; round stops
  C. snapshot slot limit (2) -> round fails; both sites released
  D. late snapshot create after terminal state is rejected
  E. idempotent snapshot create per round
  F. direct SQL correction during computation is not blocked
  G. coordinator crash -> TTL auto-release -> restart -> recovery -> new round
  H. graceful coordinator stop releases in-flight snapshots
  I. read/reclaim interleaving: queries are complete-or-refused, never
     silently zeroed; a round whose snapshot is reclaimed mid-flight fails
  J. same-round create resend: same id while live, 409 after expiry/release,
     freed slot usable by a new round, rebuild allowed after retention

A "crash" is simulated by freezing the coordinator's event loop (no cleanup
code runs), which is exactly what a SIGKILL would look like to the sites.

Run:  python tests/integration_local.py
"""
from __future__ import annotations

import asyncio
import concurrent.futures as cf
import json
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "site"))
sys.path.insert(0, str(ROOT / "coordinator"))

import uvicorn  # noqa: E402

from coord_app.config import CoordConfig  # noqa: E402
from coord_app.main import create_app as create_coord_app  # noqa: E402
from site_app.config import SiteConfig  # noqa: E402
from site_app.main import create_app as create_site_app  # noqa: E402

TMP = Path(tempfile.mkdtemp(prefix="km-itest-"))
SNAPSHOT_TTL = 20.0   # short so the auto-release test is fast
B_DELAY = 4.0         # slow site B

failures: list[str] = []
servers: list[tuple[uvicorn.Server, asyncio.AbstractEventLoop, threading.Thread]] = []


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def check(name: str, cond: bool, detail: str = "") -> None:
    tag = "PASS" if cond else "FAIL"
    print(f"[{tag}] {name}" + (f"  -- {detail}" if (detail and not cond) else ""))
    if not cond:
        failures.append(name)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def run_server(app, port: int, name: str):
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", access_log=False)
    server = uvicorn.Server(config)
    loop = asyncio.new_event_loop()

    def runner():
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(server.serve())
        except RuntimeError:
            pass  # frozen ("crashed") event loop

    t = threading.Thread(target=runner, name=name, daemon=True)
    t.start()
    servers.append((server, loop, t))
    return server, loop, t


def freeze(server_loop) -> None:
    """Simulate a coordinator crash: stop its event loop immediately, without
    running any cleanup (what a SIGKILL would look like to the sites)."""
    _server, loop, _t = server_loop
    loop.call_soon_threadsafe(loop.stop)


def http(method: str, url: str, body: dict | None = None, timeout: float = 35):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            payload = json.loads(raw)
        except Exception:
            payload = raw
        return e.code, payload


def wait_health(url: str, name: str, timeout: float = 60) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            st, _ = http("GET", url + "/health", timeout=3)
            if st == 200:
                return
        except Exception:
            pass
        time.sleep(0.3)
    raise RuntimeError(f"{name} did not become healthy")


def wait_final(coord: str, round_id: str, timeout: float = 90) -> dict:
    t0 = time.time()
    while time.time() - t0 < timeout:
        st, body = http("GET", f"{coord}/rounds/{round_id}")
        assert st == 200, body
        if body["status"] in ("succeeded", "failed", "cancelled"):
            return body
        time.sleep(0.4)
    raise RuntimeError(f"round {round_id} did not reach a terminal state")


def active_snapshots(base: str) -> int:
    st, body = http("GET", base + "/snapshots")
    assert st == 200, body
    return body["active_count"]


def backdate_snapshot(db: Path, snapshot_id: str) -> None:
    """Force a snapshot's expiry into the past (simulates TTL expiry without
    waiting); the site sweeper reclaims it on its next iteration."""
    con = sqlite3.connect(db)
    try:
        con.execute("UPDATE snapshots SET expires_at = datetime('now', '-1 hour') WHERE id = ?",
                    (snapshot_id,))
        con.commit()
    finally:
        con.close()


def load_cases(db: Path) -> list[tuple[int, str]]:
    con = sqlite3.connect(db)
    try:
        return con.execute("SELECT days, event FROM cases").fetchall()
    finally:
        con.close()


def expected_curve(dbs: list[Path]) -> list[tuple[int, int, int, float]]:
    cases = [c for db in dbs for c in load_cases(db)]
    death_days = sorted({d for d, ev in cases if ev == "death"})
    s = 1.0
    out = []
    for t in death_days:
        n = sum(1 for d, _ in cases if d >= t)
        dd = sum(1 for d, ev in cases if d == t and ev == "death")
        s *= 1 - dd / n
        out.append((t, n, dd, s))
    return out


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def main() -> int:
    db_a, db_b, db_c = TMP / "site_a.db", TMP / "site_b.db", TMP / "coord.db"
    port_a, port_b = free_port(), free_port()
    A, B = f"http://127.0.0.1:{port_a}", f"http://127.0.0.1:{port_b}"

    run_server(create_site_app(SiteConfig(
        site_id="A", database_url=f"sqlite+aiosqlite:///{db_a}", seed="A", seed_cases=160,
        snapshot_ttl_seconds=SNAPSHOT_TTL)), port_a, "site-a")
    run_server(create_site_app(SiteConfig(
        site_id="B", database_url=f"sqlite+aiosqlite:///{db_b}", seed="B", seed_cases=140,
        response_delay_seconds=B_DELAY, snapshot_ttl_seconds=SNAPSHOT_TTL)), port_b, "site-b")

    def new_coordinator() -> tuple[str, tuple]:
        port = free_port()
        app = create_coord_app(CoordConfig(
            database_url=f"sqlite+aiosqlite:///{db_c}",
            site_a_url=A, site_b_url=B, site_request_timeout=30))
        handle = run_server(app, port, f"coordinator-{port}")
        return f"http://127.0.0.1:{port}", handle

    wait_health(A, "site-a")
    wait_health(B, "site-b")
    COORD, coord_handle = new_coordinator()
    wait_health(COORD, "coordinator")
    print("== all three services healthy ==\n")

    # ---- A. happy path -----------------------------------------------------
    st, r = http("POST", f"{COORD}/rounds")
    check("A0 create round -> 202", st == 202, f"got {st}: {r}")
    rid = r["round_id"]
    time.sleep(1.0)  # round is running; site B is slow
    t0 = time.time()
    st, body = http("GET", f"{COORD}/rounds/{rid}")
    poll_ms = (time.time() - t0) * 1000
    check("A1 status query fast while slow site computes", st == 200 and poll_ms < 1500,
          f"status={st} poll={poll_ms:.0f}ms")
    check("A2 no result published while running", st == 200 and "result" not in body, str(body))
    final = wait_final(COORD, rid)
    check("A3 round succeeded", final["status"] == "succeeded", str(final.get("error")))
    curve = final.get("result", {}).get("curve", [])
    want = expected_curve([db_a, db_b])
    ok = len(curve) == len(want) and all(
        row["t"] == t and row["n_risk"] == n and row["d"] == dd and abs(row["survival"] - s) < 1e-9
        for row, (t, n, dd, s) in zip(curve, want))
    check("A4 curve matches independent KM computation", ok,
          f"got {len(curve)} points, want {len(want)}")
    # no sleep: the moment the terminal state is visible, releases must be done
    check("A5 both sites released before terminal state", active_snapshots(A) == 0 and active_snapshots(B) == 0)
    st, doc = http("GET", f"{COORD}/openapi.json")
    check("A6 /docs openapi available", st == 200 and "paths" in doc)

    # ---- B. cancel a round while the slow site is mid-call -----------------
    st, r = http("POST", f"{COORD}/rounds")
    rid_b = r["round_id"]
    time.sleep(1.0)  # site B is inside its delay
    t0 = time.time()
    st, r = http("POST", f"{COORD}/rounds/{rid_b}/cancel")
    cancel_ms = (time.time() - t0) * 1000
    check("B1 cancel returns fast despite slow site", st == 200 and cancel_ms < 2000,
          f"status={st} {cancel_ms:.0f}ms")
    check("B2 status becomes cancelling", r.get("status") == "cancelling", str(r))
    final = wait_final(COORD, rid_b, timeout=30)
    check("B3 round reaches cancelled", final["status"] == "cancelled", str(final))
    check("B4 no result after cancel", "result" not in final)
    # no sleep: release happens strictly before the terminal state is published
    check("B5 both sites released before terminal state", active_snapshots(A) == 0 and active_snapshots(B) == 0)

    # ---- C. slot limit: round fails, both sites released -------------------
    st, s1 = http("POST", f"{A}/snapshots", {"round_id": "manual-1"})
    st2, s2 = http("POST", f"{A}/snapshots", {"round_id": "manual-2"})
    check("C0 two manual snapshots occupy both slots",
          st == 201 and st2 == 201 and active_snapshots(A) == 2)
    st, r = http("POST", f"{A}/snapshots", {"round_id": "manual-3"})
    check("C1 third snapshot rejected (409)", st == 409, f"got {st}: {r}")
    st, r = http("POST", f"{COORD}/rounds")
    rid_c = r["round_id"]
    final = wait_final(COORD, rid_c, timeout=30)
    check("C2 round fails when a site has no free slot", final["status"] == "failed", str(final))
    check("C3 failure mentions slots", "slot" in (final.get("error") or "").lower(),
          str(final.get("error")))
    check("C4 site B snapshot released after failure", active_snapshots(B) == 0)
    http("DELETE", f"{A}/snapshots/{s1['snapshot_id']}")
    http("DELETE", f"{A}/snapshots/{s2['snapshot_id']}")
    check("C5 manual snapshots cleaned up", active_snapshots(A) == 0)

    # ---- D. late create after terminal state is rejected -------------------
    st, r = http("POST", f"{A}/snapshots", {"round_id": rid_c})
    check("D1 late create for failed round rejected", st == 409, f"got {st}: {r}")
    st, r = http("POST", f"{B}/snapshots", {"round_id": rid_b})
    check("D2 late create for cancelled round rejected", st == 409, f"got {st}: {r}")
    check("D3 no slot occupied by late creates", active_snapshots(A) == 0 and active_snapshots(B) == 0)

    # ---- E. idempotent create per round ------------------------------------
    st, i1 = http("POST", f"{A}/snapshots", {"round_id": "idem-1"})
    st2, i2 = http("POST", f"{A}/snapshots", {"round_id": "idem-1"})
    check("E1 retry returns same snapshot", st == 201 and st2 in (200, 201)
          and i1["snapshot_id"] == i2["snapshot_id"], f"{i1} vs {i2}")
    check("E2 only one slot occupied", active_snapshots(A) == 1)
    http("DELETE", f"{A}/snapshots/{i1['snapshot_id']}")

    # ---- F. direct SQL correction does not wait for computation ------------
    st, r = http("POST", f"{COORD}/rounds")
    rid_f = r["round_id"]
    time.sleep(1.0)  # round in flight
    t0 = time.time()
    con = sqlite3.connect(db_a)
    con.execute("PRAGMA busy_timeout=5000")
    victim = con.execute("SELECT case_no FROM cases WHERE event='censored' LIMIT 1").fetchone()[0]
    con.execute("UPDATE cases SET days = days + 1 WHERE case_no = ?", (victim,))
    con.commit()
    con.close()
    fix_ms = (time.time() - t0) * 1000
    check("F1 direct SQL correction immediate during computation", fix_ms < 2000, f"{fix_ms:.0f}ms")
    final = wait_final(COORD, rid_f)
    check("F2 round still completes after correction", final["status"] == "succeeded",
          str(final.get("error")))

    # ---- G. coordinator crash -> TTL release -> restart -> recovery --------
    st, r = http("POST", f"{COORD}/rounds")
    rid_g = r["round_id"]
    time.sleep(6.0)  # snapshots created on both sites, site B still computing
    check("G0 snapshots active while round runs", active_snapshots(A) == 1 and active_snapshots(B) == 1)
    freeze(coord_handle)  # "SIGKILL": no cleanup runs any more
    time.sleep(0.5)
    check("G1 slots stay occupied right after crash",
          active_snapshots(A) == 1 and active_snapshots(B) == 1)
    t0 = time.time()
    while time.time() - t0 < SNAPSHOT_TTL + 20:
        if active_snapshots(A) == 0 and active_snapshots(B) == 0:
            break
        time.sleep(1.0)
    check("G2 TTL auto-releases both sites after coordinator death",
          active_snapshots(A) == 0 and active_snapshots(B) == 0)

    COORD2, coord2_handle = new_coordinator()  # same DB, new process/port
    wait_health(COORD2, "coordinator2")
    st, body = http("GET", f"{COORD2}/rounds/{rid_g}")
    check("G3 interrupted round marked failed after restart",
          st == 200 and body["status"] == "failed" and "restart" in (body.get("error") or "").lower(),
          str(body))
    st, r = http("POST", f"{COORD2}/rounds")
    rid_g2 = r["round_id"]
    final = wait_final(COORD2, rid_g2)
    check("G4 new round completes after restart (no stale snapshot reuse)",
          final["status"] == "succeeded", str(final.get("error")))
    check("G5 sites clean after recovery round", active_snapshots(A) == 0 and active_snapshots(B) == 0)

    # ---- H. graceful coordinator stop releases in-flight snapshots ---------
    st, r = http("POST", f"{COORD2}/rounds")
    time.sleep(6.0)  # in flight, snapshots occupied on both sites
    check("H0 snapshots occupied mid-round", active_snapshots(A) == 1 and active_snapshots(B) == 1)
    coord2_handle[0].should_exit = True  # like `docker stop` (SIGTERM)
    coord2_handle[2].join(timeout=20)
    time.sleep(0.5)
    check("H1 graceful stop releases both sites", active_snapshots(A) == 0 and active_snapshots(B) == 0)

    # ---- I. read/reclaim interleaving --------------------------------------
    # I1: a live snapshot serves complete data (site A seeded 160 cases)
    st, s_i = http("POST", f"{A}/snapshots", {"round_id": "reclaim-1"})
    sid_i = s_i["snapshot_id"]
    st, dd = http("GET", f"{A}/snapshots/{sid_i}/death-days")
    check("I1 live snapshot returns real death days", st == 200 and len(dd["death_days"]) > 0,
          f"got {st}: {dd}")
    st, ag = http("POST", f"{A}/snapshots/{sid_i}/aggregate", {"points": [1]})
    check("I1b live snapshot returns the full risk set",
          st == 200 and ag["rows"][0]["n_risk"] == 160, f"got {st}: {ag}")

    # I2: an expired snapshot refuses queries (whether or not the sweeper
    # has already reclaimed it) instead of answering with empty data
    backdate_snapshot(db_a, sid_i)
    st, dd = http("GET", f"{A}/snapshots/{sid_i}/death-days")
    check("I2 expired snapshot refuses death-days", st == 404, f"got {st}: {dd}")
    st, ag = http("POST", f"{A}/snapshots/{sid_i}/aggregate", {"points": [1]})
    check("I2b expired snapshot refuses aggregate", st == 404, f"got {st}: {ag}")
    time.sleep(3.0)  # let the sweeper reclaim it (sweep interval 2s)
    st, dd = http("GET", f"{A}/snapshots/{sid_i}/death-days")
    check("I2c reclaimed snapshot still refuses death-days", st == 404, f"got {st}: {dd}")
    st, ag = http("POST", f"{A}/snapshots/{sid_i}/aggregate", {"points": [1]})
    check("I2d reclaimed snapshot still refuses aggregate", st == 404, f"got {st}: {ag}")

    # I3: a released snapshot refuses queries
    st, s_i3 = http("POST", f"{A}/snapshots", {"round_id": "reclaim-2"})
    sid_i3 = s_i3["snapshot_id"]
    http("DELETE", f"{A}/snapshots/{sid_i3}")
    st, dd = http("GET", f"{A}/snapshots/{sid_i3}/death-days")
    check("I3 released snapshot refuses death-days", st == 404, f"got {st}: {dd}")
    st, ag = http("POST", f"{A}/snapshots/{sid_i3}/aggregate", {"points": [1]})
    check("I3b released snapshot refuses aggregate", st == 404, f"got {st}: {ag}")

    # I4: a read racing a release is always complete-or-refused, never a
    # partial (zeroed) histogram
    interleave_ok, interleave_detail = True, ""
    for i in range(10):
        st, s_x = http("POST", f"{A}/snapshots", {"round_id": f"race-read-{i}"})
        sid_x = s_x["snapshot_id"]
        with cf.ThreadPoolExecutor(max_workers=2) as ex:
            f_read = ex.submit(http, "POST", f"{A}/snapshots/{sid_x}/aggregate", {"points": [1]})
            f_del = ex.submit(http, "DELETE", f"{A}/snapshots/{sid_x}")
            st_r, ag_x = f_read.result()
            f_del.result()
        if not (st_r == 404 or (st_r == 200 and ag_x["rows"][0]["n_risk"] == 160)):
            interleave_ok, interleave_detail = False, f"iter {i}: {st_r} {ag_x}"
    check("I4 read racing release is complete-or-refused (never zeroed)",
          interleave_ok, interleave_detail)

    # I5: if a site's snapshot is reclaimed mid-round, the round fails and
    # no curve is published (the site is never silently counted as zero)
    COORD3, _coord3_handle = new_coordinator()  # COORD/COORD2 are stopped by now
    wait_health(COORD3, "coordinator3")
    st, r = http("POST", f"{COORD3}/rounds")
    rid_i = r["round_id"]
    time.sleep(1.0)  # A's snapshot exists; site B is still inside its delay
    st, lst = http("GET", f"{A}/snapshots")
    sid_a = [s["snapshot_id"] for s in lst["active"] if s["round_id"] == rid_i][0]
    backdate_snapshot(db_a, sid_a)  # sweeper reclaims it within ~2s
    final = wait_final(COORD3, rid_i, timeout=60)
    check("I5 round fails when a site's snapshot is reclaimed mid-round",
          final["status"] == "failed", str(final))
    check("I5b no curve published for the failed round", "result" not in final)
    check("I5c both sites released after the failure",
          active_snapshots(A) == 0 and active_snapshots(B) == 0)

    # ---- J. same-round create resend ---------------------------------------
    # J1: resend while the snapshot is live -> the original snapshot id
    st, j1a = http("POST", f"{A}/snapshots", {"round_id": "resend-1"})
    st2, j1b = http("POST", f"{A}/snapshots", {"round_id": "resend-1"})
    check("J1 resend while live returns the original snapshot id",
          st == 201 and j1a["snapshot_id"] == j1b["snapshot_id"], f"{j1a} vs {j1b}")
    check("J1b still exactly one slot occupied", active_snapshots(A) == 1)

    # J2: resend after the snapshot expired -> 409 (no silent swap to a new
    # snapshot); the slot is reclaimed and usable by a new round
    backdate_snapshot(db_a, j1a["snapshot_id"])
    st, j2 = http("POST", f"{A}/snapshots", {"round_id": "resend-1"})
    check("J2 resend after expiry rejected (409)", st == 409, f"got {st}: {j2}")
    check("J2b expired snapshot's slot reclaimed", active_snapshots(A) == 0)
    st, j2c = http("POST", f"{A}/snapshots", {"round_id": "resend-1"})
    check("J2c resend still rejected via the terminal marker", st == 409, f"got {st}: {j2c}")
    st, j2d = http("POST", f"{A}/snapshots", {"round_id": "resend-2"})
    check("J2d freed slot usable by a new round", st == 201, f"got {st}: {j2d}")
    http("DELETE", f"{A}/snapshots/{j2d['snapshot_id']}")

    # J2e: same when the sweeper (not the create path) reclaimed the snapshot
    st, j2e = http("POST", f"{A}/snapshots", {"round_id": "resend-x"})
    backdate_snapshot(db_a, j2e["snapshot_id"])
    time.sleep(3.0)  # sweeper reclaims it and marks the round terminal
    st, j2e2 = http("POST", f"{A}/snapshots", {"round_id": "resend-x"})
    check("J2e resend after sweeper reclaim rejected (409)", st == 409, f"got {st}: {j2e2}")

    # J3: resend after an explicit release -> 409; freed slot serves new rounds
    st, j3a = http("POST", f"{A}/snapshots", {"round_id": "resend-3"})
    http("DELETE", f"{A}/snapshots/{j3a['snapshot_id']}")
    st, j3b = http("POST", f"{A}/snapshots", {"round_id": "resend-3"})
    check("J3 resend after release rejected (409)", st == 409, f"got {st}: {j3b}")
    st, j3c = http("POST", f"{A}/snapshots", {"round_id": "resend-4"})
    check("J3b freed slot usable by a new round", st == 201, f"got {st}: {j3c}")
    http("DELETE", f"{A}/snapshots/{j3c['snapshot_id']}")

    # J4: rebuild is rejected only within the configured terminal-state
    # retention window; use a dedicated site with a short retention so the
    # main sites keep their long default
    port_c2 = free_port()
    C2 = f"http://127.0.0.1:{port_c2}"
    run_server(create_site_app(SiteConfig(
        site_id="C2", database_url=f"sqlite+aiosqlite:///{TMP / 'site_c2.db'}",
        seed="C2", seed_cases=10, snapshot_ttl_seconds=60,
        finalized_retention_seconds=2.0, sweep_interval_seconds=0.5)), port_c2, "site-c2")
    wait_health(C2, "site-c2")
    st, k1 = http("POST", f"{C2}/snapshots", {"round_id": "retain-1"})
    http("DELETE", f"{C2}/snapshots/{k1['snapshot_id']}")
    st, k2 = http("POST", f"{C2}/snapshots", {"round_id": "retain-1"})
    check("J4 rebuild rejected within the retention window", st == 409, f"got {st}: {k2}")
    time.sleep(3.5)  # retention 2s + sweep interval 0.5s
    st, k3 = http("POST", f"{C2}/snapshots", {"round_id": "retain-1"})
    check("J4b rebuild allowed after the retention window", st == 201, f"got {st}: {k3}")

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s): {failures}")
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    try:
        rc = main()
    finally:
        for server, _loop, _t in servers:
            server.should_exit = True
    sys.exit(rc)
