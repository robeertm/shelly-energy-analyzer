from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

from shelly_analyzer.io.config import AppConfig, DeviceConfig
from shelly_analyzer.io.http import HttpConfig, ShellyHttp, download_csv, get_emdata_records, get_earliest_emdata_ts
from shelly_analyzer.io.storage import MetaState, Storage
from shelly_analyzer.services.demo import ensure_demo_csv


@dataclass(frozen=True)
class ChunkResult:
    ts: int
    end_ts: int
    ok: bool
    error: Optional[str] = None


@dataclass(frozen=True)
class SyncResult:
    device_key: str
    device_name: str
    started_at: int
    ended_at: int
    requested_range: Tuple[int, int]
    chunks: List[ChunkResult]
    updated_last_end_ts: Optional[int]


def iter_time_chunks(ts: int, end_ts: int, chunk_seconds: int):
    cur = int(ts)
    while cur < int(end_ts):
        nxt = min(int(end_ts), cur + int(chunk_seconds))
        yield cur, nxt
        cur = nxt


def determine_range(
    now_ts: int,
    meta: MetaState,
    overlap_seconds: int,
    fallback_last_days: int,
) -> Tuple[int, int]:
    if meta.last_end_ts is not None:
        start = max(0, int(meta.last_end_ts) - int(overlap_seconds))
    else:
        start = max(0, int(now_ts) - int(fallback_last_days) * 86400)
    end = int(now_ts)
    if end <= start:
        end = start + 1
    return start, end


def sync_one_device(
    cfg: AppConfig,
    storage: Storage,
    device: DeviceConfig,
    range_override: Optional[Tuple[int, int]] = None,
    fallback_last_days: int = 7,
    progress: Optional[Callable[[str, int, int, str], None]] = None,
) -> SyncResult:
    now_ts = int(time.time())
    started_at = now_ts

    # Some Shelly devices (e.g. many Switch/Plug models like SNSW-001P16EU)
    # do not support EMData CSV downloads. These devices are still fully usable
    # for Live mode, but Sync/CSV import must be treated as "not applicable".
    #
    # IMPORTANT: This should NOT be reported as an error to the user, otherwise
    # the UI gets noisy and users think Live is broken.
    #
    # NOTE: Auto-detection is best-effort. If a device is an energy meter (kind=="em")
    # but was mis-detected as no-CSV, we do a quick runtime probe against the CSV
    # endpoint and proceed if it works.
    supports_emdata = bool(getattr(device, "supports_emdata", True))
    if not supports_emdata and str(getattr(device, "kind", "")) == "em":
        try:
            http_probe = ShellyHttp(HttpConfig(timeout_seconds=min(cfg.download.timeout_seconds, 3.0), retries=1, backoff_base_seconds=0.1))
            now_probe = int(time.time())
            _ = download_csv(http_probe, device.host, device.em_id, max(0, now_probe - 300), now_probe)
            supports_emdata = True
        except Exception:
            supports_emdata = False

    if not supports_emdata:
        end_ts = int(time.time())
        return SyncResult(
            device_key=device.key,
            device_name=device.name,
            started_at=started_at,
            ended_at=end_ts,
            requested_range=(0, 0),
            chunks=[],
            updated_last_end_ts=None,
        )


    # DEMO MODE: do not perform HTTP calls. Ensure demo CSV exists and treat as synced.
    if str(getattr(device, "host", "")).startswith("demo://"):
        # Create demo CSV history on-demand (lightweight) so Plots/Export work.
        try:
            ensure_demo_csv(storage, [device], cfg.demo, days=fallback_last_days)
        except Exception:
            # Even if CSV generation fails, we still allow the app to run in Live demo mode.
            pass
        ended_at = int(time.time())
        try:
            storage.save_meta(device.key, MetaState(last_end_ts=ended_at, updated_at=ended_at))
        except Exception:
            pass
        return SyncResult(
            device_key=device.key,
            device_name=device.name,
            started_at=started_at,
            ended_at=ended_at,
            requested_range=(0, 0) if range_override is None else (int(range_override[0]), int(range_override[1])),
            chunks=[ChunkResult(ts=0, end_ts=ended_at, ok=True)],
            updated_last_end_ts=ended_at,
        )

    meta = storage.load_meta(device.key)
    if range_override is None:
        ts_start, ts_end = determine_range(
            now_ts=now_ts,
            meta=meta,
            overlap_seconds=cfg.download.overlap_seconds,
            fallback_last_days=fallback_last_days,
        )
    else:
        ts_start, ts_end = int(range_override[0]), int(range_override[1])

    http = ShellyHttp(
        HttpConfig(
            timeout_seconds=cfg.download.timeout_seconds,
            retries=cfg.download.retries,
            backoff_base_seconds=cfg.download.backoff_base_seconds,
        )
    )

    # Try to align requested start with the oldest data still stored on the device
    try:
        rec = get_emdata_records(http, device.host, device.em_id, ts=0)
        earliest_ts = get_earliest_emdata_ts(rec)
        if earliest_ts is not None and ts_start < earliest_ts:
            ts_start = earliest_ts
    except Exception:
        # If this fails, we continue with the user-requested range and let empty chunks be skipped.
        pass

    chunks: List[ChunkResult] = []
    last_success_end: Optional[int] = None

    # total chunks for progress reporting
    try:
        total_chunks = max(1, int((ts_end - ts_start + cfg.download.chunk_seconds - 1) // cfg.download.chunk_seconds))
    except Exception:
        total_chunks = 1
    done_chunks = 0

    def _progress(msg: str) -> None:
        if progress:
            try:
                # (device_key, done, total, message)
                progress(device.key, done_chunks, total_chunks, msg)
            except Exception:
                pass

    for a, b in iter_time_chunks(ts_start, ts_end, cfg.download.chunk_seconds):
        _progress(f"Lade {a}-{b} …")
        try:
            content = download_csv(http, device.host, device.em_id, a, b)
            # Skip saving empty CSV responses (header-only)
            lines = [ln for ln in content.splitlines() if ln.strip()]
            if len(lines) <= 1:
                chunks.append(ChunkResult(ts=a, end_ts=b, ok=False, error="No data returned for this interval (likely outside device history retention)."))
                done_chunks += 1
                _progress("Keine Daten (außerhalb Historie)")
                continue
            storage.save_chunk(device.key, a, b, content)
            chunks.append(ChunkResult(ts=a, end_ts=b, ok=True))
            last_success_end = b
            done_chunks += 1
            _progress("OK")
        except Exception as e:
            chunks.append(ChunkResult(ts=a, end_ts=b, ok=False, error=str(e)))
            _progress(f"Fehler: {e}")
            break

    ended_at = int(time.time())

    # Update meta ONLY to the last successfully downloaded end_ts
    if last_success_end is not None:
        storage.save_meta(device.key, MetaState(last_end_ts=last_success_end, updated_at=ended_at))

    # Optional packing
    try:
        storage.pack_csvs(
            device.key,
            threshold_count=cfg.csv_pack.threshold_count,
            max_megabytes=cfg.csv_pack.max_megabytes,
            remove_merged=cfg.csv_pack.remove_merged,
        )
    except Exception:
        pass

    return SyncResult(
        device_key=device.key,
        device_name=device.name,
        started_at=started_at,
        ended_at=ended_at,
        requested_range=(ts_start, ts_end),
        chunks=chunks,
        updated_last_end_ts=last_success_end,
    )


def sync_all(
    cfg: AppConfig,
    storage: Storage,
    range_override: Optional[Tuple[int, int]] = None,
    fallback_last_days: int = 7,
    progress: Optional[Callable[[str, int, int, str], None]] = None,
) -> List[SyncResult]:
    results: List[SyncResult] = []
    for d in cfg.devices:
        results.append(
            sync_one_device(
                cfg,
                storage,
                d,
                range_override=range_override,
                fallback_last_days=fallback_last_days,
                progress=progress,
            )
        )
    return results
