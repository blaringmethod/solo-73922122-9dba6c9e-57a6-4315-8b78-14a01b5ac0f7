"""Hospital site node.

Stores up to 200 local cases (case_no, positive follow-up days, death/censored)
and exposes ONLY aggregate snapshot data to the coordinator: snapshot ids,
death days, and at-risk/death counts. Case-level records never leave this
service. Direct SQL corrections on `cases` are supported and never wait for
an ongoing computation (snapshots are frozen copies taken at creation time).
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field
from sqlalchemy import delete, func, select, text

from .aggregate import risk_rows
from .config import SiteConfig
from .db import init_db, make_engine, make_sessionmaker
from .models import Case, FinalizedRound, Snapshot, SnapshotHist
from .seed import seed_cases

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class CreateSnapshotIn(BaseModel):
    round_id: str = Field(min_length=1, max_length=64)


class AggregateIn(BaseModel):
    points: list[int] = Field(default_factory=list, max_length=100000)


def create_app(cfg: SiteConfig) -> FastAPI:
    logger = logging.getLogger(f"site.{cfg.site_id}")
    engine = make_engine(cfg.database_url)
    Session = make_sessionmaker(engine)
    # Serializes every snapshot lifecycle change (create, TTL reclaim, release,
    # finalize) against every snapshot read (death-days, aggregate). A 200
    # response therefore always reflects one complete, still-live snapshot: a
    # snapshot can never be reclaimed halfway through a read and leave the
    # coordinator adding up a half-deleted histogram as zeros. One site
    # process owns its database, so an in-process lock is sufficient (and
    # works identically on PostgreSQL and SQLite).
    snapshot_lock = asyncio.Lock()

    async def maybe_delay() -> None:
        """Emulate a slow hospital node on the compute endpoints (demo)."""
        if cfg.response_delay_seconds > 0:
            await asyncio.sleep(cfg.response_delay_seconds)

    async def sweeper_loop() -> None:
        """Reclaim expired snapshots so a dead coordinator never holds slots.

        Reclaiming makes the round terminal on this site: within the
        finalized-retention window the round cannot be rebuilt, so a
        coordinator retry never silently continues a round on a fresh
        snapshot. Runs under the snapshot lock so a reclaim never
        interleaves with an in-flight read of the same snapshot.
        """
        while True:
            await asyncio.sleep(cfg.sweep_interval_seconds)
            try:
                async with snapshot_lock:
                    async with Session() as s:
                        now = utcnow()
                        expired = (await s.scalars(select(Snapshot).where(Snapshot.expires_at <= now))).all()
                        for snap in expired:
                            sid, round_id = snap.id, snap.round_id
                            await _delete_snapshot(s, snap)
                            await _mark_finalized(s, round_id, now)
                            logger.info("snapshot %s (round %s) expired -> slot released, round terminal",
                                        sid, round_id)
                        cutoff = now - timedelta(seconds=cfg.finalized_retention_seconds)
                        await s.execute(delete(FinalizedRound).where(FinalizedRound.finalized_at < cutoff))
                        await s.commit()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("sweeper iteration failed")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await init_db(engine)
        async with Session() as s:
            n = await s.scalar(select(func.count()).select_from(Case))
            if not n:
                rows = seed_cases(cfg.seed, cfg.seed_cases)
                s.add_all(rows)
                await s.commit()
                logger.info("seeded %d cases", len(rows))
        sweeper = asyncio.create_task(sweeper_loop())
        try:
            yield
        finally:
            sweeper.cancel()
            await engine.dispose()

    app = FastAPI(
        title=f"KM site node {cfg.site_id}",
        version="1.0.0",
        description="Aggregate-only hospital data node for federated Kaplan-Meier computation.",
        lifespan=lifespan,
    )

    # ------------------------------------------------------------------ #
    # helpers
    # ------------------------------------------------------------------ #
    async def get_live_snapshot(s, snapshot_id: str) -> Snapshot:
        """Fetch a snapshot that is still usable; refuses expired/released ones.

        Must be called under the snapshot lock so the snapshot cannot be
        reclaimed between this check and the histogram read that follows.
        """
        snap = await s.get(Snapshot, snapshot_id)
        if snap is None or snap.expires_at <= utcnow():
            raise HTTPException(status_code=404, detail="snapshot not found, expired, or already released")
        return snap

    async def active_count(s, now: datetime) -> int:
        return int(await s.scalar(select(func.count()).select_from(Snapshot).where(Snapshot.expires_at > now)) or 0)

    def snapshot_out(snap: Snapshot, count: int) -> dict:
        return {
            "snapshot_id": snap.id,
            "round_id": snap.round_id,
            "created_at": snap.created_at.isoformat(),
            "expires_at": snap.expires_at.isoformat(),
            "active_count": count,
            "max_snapshots": cfg.max_snapshots,
        }

    # ------------------------------------------------------------------ #
    # endpoints
    # ------------------------------------------------------------------ #
    @app.post("/snapshots", status_code=201)
    async def create_snapshot(body: CreateSnapshotIn):
        """Create a frozen aggregate snapshot for a computation round.

        Idempotent per round while the snapshot is live: a retry with the
        same round_id returns the existing snapshot instead of occupying a
        second slot. Rejected (409) when the round is already finalized
        (late request), when its previous snapshot expired or was released
        (the round is terminal on this site and must not silently continue
        on a fresh snapshot), or when both snapshot slots are busy.
        """
        await maybe_delay()
        now = utcnow()
        async with snapshot_lock:
            async with Session() as s:
                if await s.get(FinalizedRound, body.round_id) is not None:
                    raise HTTPException(status_code=409, detail="round already finalized; late snapshot request rejected")

                existing = await s.scalar(select(Snapshot).where(Snapshot.round_id == body.round_id))
                if existing is not None:
                    if existing.expires_at > now:
                        return snapshot_out(existing, await active_count(s, now))
                    # The round's previous snapshot expired: reclaim the slot
                    # and mark the round terminal instead of swapping in a
                    # fresh snapshot (with a new id) mid-round.
                    sid = existing.id
                    await _delete_snapshot(s, existing)
                    await _mark_finalized(s, body.round_id, now)
                    await s.commit()
                    logger.info("snapshot %s (round %s) had expired -> reclaimed; rebuild rejected",
                                sid, body.round_id)
                    raise HTTPException(
                        status_code=409,
                        detail="snapshot for this round expired; round is terminal on this site",
                    )

                active = await active_count(s, now)
                if active >= cfg.max_snapshots:
                    raise HTTPException(status_code=409, detail="snapshot slots full on this site")

                sid = str(uuid.uuid4())
                rows = (
                    await s.execute(
                        text(
                            "SELECT days, "
                            "SUM(CASE WHEN event='death' THEN 1 ELSE 0 END) AS deaths, "
                            "SUM(CASE WHEN event='censored' THEN 1 ELSE 0 END) AS censored "
                            "FROM cases GROUP BY days"
                        )
                    )
                ).all()
                snap = Snapshot(
                    id=sid,
                    round_id=body.round_id,
                    created_at=now,
                    expires_at=now + timedelta(seconds=cfg.snapshot_ttl_seconds),
                )
                s.add(snap)
                for days, deaths, censored in rows:
                    s.add(SnapshotHist(snapshot_id=sid, day=days, deaths=deaths, censored=censored))
                await s.commit()
                logger.info("snapshot %s created for round %s (%d day buckets)", sid, body.round_id, len(rows))
                return snapshot_out(snap, active + 1)

    @app.get("/snapshots/{snapshot_id}/death-days")
    async def death_days(snapshot_id: str):
        """Distinct death days of the frozen snapshot (aggregate information).

        The liveness check and the histogram read run under the snapshot
        lock, so a 200 response always comes from one complete snapshot
        (an empty list means the snapshot genuinely has no deaths). An
        expired or released snapshot is rejected with 404.
        """
        await maybe_delay()
        async with snapshot_lock:
            async with Session() as s:
                await get_live_snapshot(s, snapshot_id)
                rows = await s.scalars(
                    select(SnapshotHist.day)
                    .where(SnapshotHist.snapshot_id == snapshot_id, SnapshotHist.deaths > 0)
                    .order_by(SnapshotHist.day)
                )
                days = list(rows)
        return {"snapshot_id": snapshot_id, "death_days": days}

    @app.post("/snapshots/{snapshot_id}/aggregate")
    async def aggregate(snapshot_id: str, body: AggregateIn):
        """At-risk and death counts of the frozen snapshot at the given days.

        Same completeness guarantee as death-days: a 200 response is always
        computed from the full frozen histogram of a live snapshot.
        """
        await maybe_delay()
        points = sorted(set(body.points))
        if any(p <= 0 for p in points):
            raise HTTPException(status_code=422, detail="points must be positive integers (days)")
        async with snapshot_lock:
            async with Session() as s:
                await get_live_snapshot(s, snapshot_id)
                hist = (
                    await s.execute(
                        select(SnapshotHist.day, SnapshotHist.deaths, SnapshotHist.censored).where(
                            SnapshotHist.snapshot_id == snapshot_id
                        )
                    )
                ).all()
        return {"snapshot_id": snapshot_id, "rows": risk_rows(hist, points)}

    @app.delete("/snapshots/{snapshot_id}", status_code=204)
    async def delete_snapshot(snapshot_id: str):
        """Release a snapshot (idempotent).

        Releasing makes the round terminal on this site: within the
        finalized-retention window the round cannot be rebuilt, while the
        freed slot is immediately usable by a new round.
        """
        async with snapshot_lock:
            async with Session() as s:
                snap = await s.get(Snapshot, snapshot_id)
                if snap is not None:
                    sid, round_id = snap.id, snap.round_id
                    await _delete_snapshot(s, snap)
                    await _mark_finalized(s, round_id, utcnow())
                    await s.commit()
                    logger.info("snapshot %s released (round %s terminal on this site)", sid, round_id)
        return Response(status_code=204)

    @app.post("/rounds/{round_id}/finalize")
    async def finalize_round(round_id: str):
        """Mark a round as terminal: delete its snapshots and reject late creates."""
        # snapshot_lock makes this atomic w.r.t. snapshot creation: either a
        # create commits first (and its snapshot is deleted here) or the
        # finalize commits first (and the create is rejected with 409).
        async with snapshot_lock:
            async with Session() as s:
                if await s.get(FinalizedRound, round_id) is None:
                    s.add(FinalizedRound(round_id=round_id, finalized_at=utcnow()))
                snaps = (await s.scalars(select(Snapshot).where(Snapshot.round_id == round_id))).all()
                for snap in snaps:
                    await _delete_snapshot(s, snap)
                await s.commit()
        if snaps:
            logger.info("round %s finalized, %d leftover snapshot(s) removed", round_id, len(snaps))
        return {"round_id": round_id, "finalized": True}

    @app.get("/snapshots")
    async def list_snapshots():
        """Operational view: active snapshot slots (ids only, no case data)."""
        async with Session() as s:
            now = utcnow()
            rows = (await s.scalars(select(Snapshot).where(Snapshot.expires_at > now).order_by(Snapshot.created_at))).all()
            return {
                "site": cfg.site_id,
                "max_snapshots": cfg.max_snapshots,
                "active_count": len(rows),
                "active": [
                    {"snapshot_id": r.id, "round_id": r.round_id, "expires_at": r.expires_at.isoformat()} for r in rows
                ],
            }

    @app.get("/health")
    async def health():
        async with Session() as s:
            await s.execute(text("SELECT 1"))
            cases = await s.scalar(select(func.count()).select_from(Case))
        return {"status": "ok", "site": cfg.site_id, "cases": cases}

    return app


async def _delete_snapshot(s, snap: Snapshot) -> None:
    await s.execute(delete(SnapshotHist).where(SnapshotHist.snapshot_id == snap.id))
    await s.delete(snap)


async def _mark_finalized(s, round_id: str, when: datetime) -> None:
    """Record a round as terminal (idempotent): snapshot creates for it are
    rejected while the marker is retained (finalized_retention_seconds)."""
    if await s.get(FinalizedRound, round_id) is None:
        s.add(FinalizedRound(round_id=round_id, finalized_at=when))


# Module-level app used by `uvicorn site_app.main:app` (Docker, CLI).
app = create_app(SiteConfig.from_env())
