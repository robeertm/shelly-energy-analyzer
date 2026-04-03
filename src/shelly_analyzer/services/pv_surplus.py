from __future__ import annotations
import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Callable

_log = logging.getLogger(__name__)


class SurplusState(Enum):
    IDLE = "idle"
    PENDING_ON = "pending_on"
    ON = "on"
    PENDING_OFF = "pending_off"


@dataclass
class ConsumerState:
    device_key: str
    switch_id: int
    name: str
    priority: int
    min_power_w: float
    state: SurplusState = SurplusState.IDLE
    state_since: float = 0.0
    is_on: bool = False


@dataclass
class SurplusStatus:
    current_surplus_w: float = 0.0
    consumers: List[ConsumerState] = field(default_factory=list)
    active_count: int = 0
    total_diverted_w: float = 0.0


class PvSurplusController:
    """Controls relay consumers based on PV surplus power.

    State machine per consumer:
    IDLE -> PENDING_ON (surplus > on_threshold for debounce_seconds)
    PENDING_ON -> ON (debounce elapsed, relay switched on)
    ON -> PENDING_OFF (surplus < off_threshold for debounce_seconds)
    PENDING_OFF -> IDLE (debounce elapsed, relay switched off)
    """

    def __init__(self, cfg, switch_callback: Optional[Callable] = None):
        self.cfg = cfg
        self._switch_callback = switch_callback
        self._consumers: List[ConsumerState] = []
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._surplus_w = 0.0
        self._lock = threading.Lock()

        # Initialize consumer states from config
        for c in (cfg.consumers if hasattr(cfg, 'consumers') else []):
            self._consumers.append(ConsumerState(
                device_key=c.device_key,
                switch_id=c.switch_id,
                name=c.name or c.device_key,
                priority=c.priority,
                min_power_w=c.min_power_w,
            ))
        # Sort by priority (lower = higher priority)
        self._consumers.sort(key=lambda c: c.priority)

    def update_surplus(self, surplus_w: float) -> None:
        """Update current PV surplus value (called from live poller)."""
        with self._lock:
            self._surplus_w = surplus_w

    def get_status(self) -> SurplusStatus:
        with self._lock:
            return SurplusStatus(
                current_surplus_w=self._surplus_w,
                consumers=list(self._consumers),
                active_count=sum(1 for c in self._consumers if c.is_on),
                total_diverted_w=sum(c.min_power_w for c in self._consumers if c.is_on),
            )

    def _tick(self) -> None:
        """Run one control cycle."""
        now = time.time()
        on_thr = self.cfg.on_threshold_w
        off_thr = self.cfg.off_threshold_w
        debounce = self.cfg.debounce_seconds

        with self._lock:
            surplus = self._surplus_w

        for c in self._consumers:
            if c.state == SurplusState.IDLE:
                if surplus >= on_thr and surplus >= c.min_power_w:
                    c.state = SurplusState.PENDING_ON
                    c.state_since = now

            elif c.state == SurplusState.PENDING_ON:
                if surplus < on_thr or surplus < c.min_power_w:
                    c.state = SurplusState.IDLE
                    c.state_since = now
                elif now - c.state_since >= debounce:
                    self._switch_on(c)
                    c.state = SurplusState.ON
                    c.state_since = now
                    c.is_on = True
                    surplus -= c.min_power_w

            elif c.state == SurplusState.ON:
                if surplus < -off_thr:  # Drawing from grid
                    c.state = SurplusState.PENDING_OFF
                    c.state_since = now

            elif c.state == SurplusState.PENDING_OFF:
                if surplus >= off_thr:
                    c.state = SurplusState.ON
                    c.state_since = now
                elif now - c.state_since >= debounce:
                    self._switch_off(c)
                    c.state = SurplusState.IDLE
                    c.state_since = now
                    c.is_on = False

    def _switch_on(self, consumer: ConsumerState) -> None:
        _log.info("PV surplus: switching ON %s (switch %d)", consumer.name, consumer.switch_id)
        if self._switch_callback:
            try:
                self._switch_callback(consumer.device_key, consumer.switch_id, True)
            except Exception as e:
                _log.error("PV surplus switch ON failed: %s", e)

    def _switch_off(self, consumer: ConsumerState) -> None:
        _log.info("PV surplus: switching OFF %s (switch %d)", consumer.name, consumer.switch_id)
        if self._switch_callback:
            try:
                self._switch_callback(consumer.device_key, consumer.switch_id, False)
            except Exception as e:
                _log.error("PV surplus switch OFF failed: %s", e)

    def start(self) -> None:
        if self._running or not self.cfg.enabled:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, name="PvSurplus", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    def _run_loop(self) -> None:
        while self._running:
            try:
                self._tick()
            except Exception as e:
                _log.error("PV surplus tick error: %s", e)
            time.sleep(2.0)
