"""Multi-location support: manage separate device sets and optional DBs per site."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

_log = logging.getLogger(__name__)


@dataclass
class LocationContext:
    """Context for a single location (site)."""
    location_id: str
    name: str
    device_keys: List[str]
    db_path: Optional[Path] = None
    _db: object = field(default=None, repr=False)

    @property
    def db(self):
        """Lazy-init a separate EnergyDB if db_path is set."""
        if self._db is None and self.db_path:
            try:
                from shelly_analyzer.io.database import EnergyDB
                self._db = EnergyDB(self.db_path)
            except Exception as e:
                _log.error("Location %s DB init error: %s", self.location_id, e)
        return self._db


class LocationManager:
    """Manage multiple locations (sites) with optional separate databases.

    Single-location mode: when no locations are configured, all devices
    share the default storage and DB.  When locations are configured,
    each location has its own device set and optionally its own DB file.
    """

    def __init__(self, cfg, base_dir: Optional[Path] = None):
        self._cfg = cfg
        self._base_dir = base_dir or Path.cwd()
        self._locations: Dict[str, LocationContext] = {}
        self._active_id: str = ""

        ml = getattr(cfg, "multi_location", None)
        if ml and getattr(ml, "enabled", False):
            for loc_def in getattr(ml, "locations", []):
                db_file = getattr(loc_def, "db_file", "") or ""
                db_path = Path(db_file) if db_file else None
                if db_path and not db_path.is_absolute():
                    db_path = self._base_dir / db_path
                ctx = LocationContext(
                    location_id=loc_def.location_id,
                    name=loc_def.name,
                    device_keys=list(loc_def.device_keys),
                    db_path=db_path,
                )
                self._locations[loc_def.location_id] = ctx
            self._active_id = getattr(ml, "active_location_id", "") or ""

    @property
    def is_multi(self) -> bool:
        return bool(self._locations)

    @property
    def active_location(self) -> Optional[LocationContext]:
        if self._active_id and self._active_id in self._locations:
            return self._locations[self._active_id]
        return None

    def get_location(self, location_id: str) -> Optional[LocationContext]:
        return self._locations.get(location_id)

    def get_all_locations(self) -> List[LocationContext]:
        return list(self._locations.values())

    def set_active(self, location_id: str) -> None:
        if location_id in self._locations or location_id == "":
            self._active_id = location_id

    def device_keys_for_active(self) -> List[str]:
        """Return device keys for the active location, or all if none selected."""
        loc = self.active_location
        if loc:
            return loc.device_keys
        return [d.key for d in self._cfg.devices]

    def location_names(self) -> List[str]:
        """Return list of location names (for UI dropdowns)."""
        return [loc.name for loc in self._locations.values()]
