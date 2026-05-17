"""MQTT publisher for Home Assistant integration.

Publishes Shelly energy data as MQTT topics compatible with Home Assistant's
MQTT auto-discovery. Each device/metric becomes a sensor entity in HA.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Try importing paho-mqtt; gracefully degrade if not installed
try:
    import paho.mqtt.client as mqtt
    HAS_MQTT = True
except ImportError:
    mqtt = None  # type: ignore
    HAS_MQTT = False


@dataclass
class MqttConfig:
    """MQTT connection settings (mirrors io/config.py MqttConfig)."""
    enabled: bool = False
    broker: str = "127.0.0.1"
    port: int = 1883
    username: str = ""
    password: str = ""
    topic_prefix: str = "shelly_analyzer"
    ha_discovery: bool = True
    ha_discovery_prefix: str = "homeassistant"
    publish_interval_seconds: float = 10.0
    use_tls: bool = False


class MqttPublisher:
    """Background MQTT publisher that sends energy data to an MQTT broker."""

    def __init__(self, config: MqttConfig) -> None:
        self.config = config
        self._client: Any = None
        self._connected = False
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_publish: Dict[str, float] = {}
        self._discovery_sent: set = set()

    @property
    def is_available(self) -> bool:
        return HAS_MQTT

    @property
    def is_connected(self) -> bool:
        return self._connected

    def start(self) -> bool:
        """Connect to MQTT broker and start background loop."""
        if not HAS_MQTT:
            logger.warning("paho-mqtt not installed – MQTT integration disabled")
            return False

        if not self.config.enabled:
            return False

        try:
            self._client = mqtt.Client(
                client_id=f"shelly_analyzer_{int(time.time())}",
                protocol=mqtt.MQTTv311,
            )
            if self.config.username:
                self._client.username_pw_set(self.config.username, self.config.password)
            if self.config.use_tls:
                self._client.tls_set()

            self._client.on_connect = self._on_connect
            self._client.on_disconnect = self._on_disconnect

            self._client.connect_async(self.config.broker, self.config.port, keepalive=60)
            self._client.loop_start()
            logger.info("MQTT: connecting to %s:%d", self.config.broker, self.config.port)
            return True
        except Exception:
            logger.error("MQTT: connection failed", exc_info=True)
            return False

    def stop(self) -> None:
        """Disconnect and stop background loop."""
        self._stop_event.set()
        if self._client is not None:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception:
                pass
        self._connected = False

    def _on_connect(self, client, userdata, flags, rc) -> None:
        if rc == 0:
            self._connected = True
            logger.info("MQTT: connected to broker")
        else:
            logger.warning("MQTT: connection refused (rc=%d)", rc)

    def _on_disconnect(self, client, userdata, rc) -> None:
        self._connected = False
        if rc != 0:
            logger.warning("MQTT: unexpected disconnect (rc=%d)", rc)

    def publish_device_data(
        self,
        device_key: str,
        device_name: str,
        data: Dict[str, Any],
    ) -> None:
        """Publish device metrics to MQTT.

        data should contain keys like:
            power_w, voltage_v, current_a, energy_kwh,
            power_l1, power_l2, power_l3, freq_hz, cosphi, etc.
        """
        if not self._connected or self._client is None:
            return

        prefix = self.config.topic_prefix
        dev_topic = f"{prefix}/{device_key}"

        # Rate limit
        now = time.time()
        last = self._last_publish.get(device_key, 0)
        if (now - last) < self.config.publish_interval_seconds:
            return
        self._last_publish[device_key] = now

        # Send HA discovery configs (once per session)
        if self.config.ha_discovery and device_key not in self._discovery_sent:
            self._send_ha_discovery(device_key, device_name)
            self._discovery_sent.add(device_key)

        # Publish state topic
        try:
            payload = json.dumps({
                "device": device_key,
                "name": device_name,
                "timestamp": int(now),
                **{k: round(v, 3) if isinstance(v, float) else v for k, v in data.items()},
            })
            self._client.publish(f"{dev_topic}/state", payload, qos=0, retain=True)
        except Exception:
            logger.debug("MQTT publish failed for %s", device_key, exc_info=True)

    def _send_ha_discovery(self, device_key: str, device_name: str) -> None:
        """Send Home Assistant MQTT discovery messages for a device."""
        if self._client is None:
            return

        prefix = self.config.ha_discovery_prefix
        dev_topic = f"{self.config.topic_prefix}/{device_key}"
        dev_id = f"shelly_analyzer_{device_key}"

        device_info = {
            "identifiers": [dev_id],
            "name": f"Shelly Analyzer – {device_name}",
            "manufacturer": "Shelly Energy Analyzer",
            "model": "Energy Monitor",
        }

        sensors = [
            ("power_w", "Power", "W", "power", "measurement"),
            ("voltage_v", "Voltage", "V", "voltage", "measurement"),
            ("current_a", "Current", "A", "current", "measurement"),
            ("energy_kwh", "Energy Today", "kWh", "energy", "total_increasing"),
            ("freq_hz", "Frequency", "Hz", "frequency", "measurement"),
            ("cosphi", "Power Factor", None, "power_factor", "measurement"),
            ("power_l1", "Power L1", "W", "power", "measurement"),
            ("power_l2", "Power L2", "W", "power", "measurement"),
            ("power_l3", "Power L3", "W", "power", "measurement"),
            ("voltage_l1", "Voltage L1", "V", "voltage", "measurement"),
            ("voltage_l2", "Voltage L2", "V", "voltage", "measurement"),
            ("voltage_l3", "Voltage L3", "V", "voltage", "measurement"),
            ("current_l1", "Current L1", "A", "current", "measurement"),
            ("current_l2", "Current L2", "A", "current", "measurement"),
            ("current_l3", "Current L3", "A", "current", "measurement"),
            ("co2_g_per_h", "CO₂ Rate", "g/h", None, "measurement"),
            ("cost_eur_today", "Cost Today", "EUR", "monetary", "total"),
        ]

        for value_key, name, unit, dev_class, state_class in sensors:
            uid = f"{dev_id}_{value_key}"
            config_topic = f"{prefix}/sensor/{dev_id}/{value_key}/config"
            config_payload: Dict[str, Any] = {
                "name": f"{device_name} {name}",
                "unique_id": uid,
                "state_topic": f"{dev_topic}/state",
                "value_template": f"{{{{ value_json.{value_key} | default(0) }}}}",
                "device": device_info,
            }
            if unit:
                config_payload["unit_of_measurement"] = unit
            if dev_class:
                config_payload["device_class"] = dev_class
            if state_class:
                config_payload["state_class"] = state_class

            try:
                self._client.publish(
                    config_topic,
                    json.dumps(config_payload),
                    qos=1,
                    retain=True,
                )
            except Exception:
                logger.debug("HA discovery failed for %s/%s", device_key, value_key)

    def publish_summary(self, data: Dict[str, Any]) -> None:
        """Publish a summary message (e.g. daily totals)."""
        if not self._connected or self._client is None:
            return
        try:
            topic = f"{self.config.topic_prefix}/summary"
            self._client.publish(topic, json.dumps(data), qos=0, retain=True)
        except Exception:
            logger.debug("MQTT summary publish failed", exc_info=True)

    def publish_grid_data(self, data: Dict[str, Any]) -> None:
        """Publish grid-wide metrics (spot price, grid CO2 intensity) as a
        synthetic 'Netz' device for Home Assistant auto-discovery."""
        if not self._connected or self._client is None:
            return
        now = time.time()
        last = self._last_publish.get("__netz__", 0)
        if (now - last) < self.config.publish_interval_seconds:
            return
        self._last_publish["__netz__"] = now
        if self.config.ha_discovery and "__netz__" not in self._discovery_sent:
            self._send_grid_discovery()
            self._discovery_sent.add("__netz__")
        try:
            prefix = self.config.topic_prefix
            payload = json.dumps({
                "timestamp": int(now),
                **{k: round(v, 4) if isinstance(v, float) else v
                   for k, v in data.items()},
            })
            self._client.publish(f"{prefix}/netz/state", payload, qos=0, retain=True)
        except Exception:
            logger.debug("MQTT grid publish failed", exc_info=True)

    def _send_grid_discovery(self) -> None:
        """Home Assistant MQTT discovery for the synthetic Netz device."""
        if self._client is None:
            return
        prefix = self.config.ha_discovery_prefix
        dev_topic = f"{self.config.topic_prefix}/netz"
        dev_id = "shelly_analyzer_netz"
        device_info = {
            "identifiers": [dev_id],
            "name": "Shelly Analyzer \u2013 Netz",
            "manufacturer": "Shelly Energy Analyzer",
            "model": "Grid",
        }
        sensors = [
            ("spot_price_eur_kwh", "Spotpreis (inkl. Abgaben)", "EUR/kWh", None, "measurement"),
            ("spot_price_net_eur_kwh", "Spotpreis (B\u00f6rse, netto)", "EUR/kWh", None, "measurement"),
            ("co2_intensity_g_per_kwh", "Netz CO\u00b2-Intensit\u00e4t",
             "g/kWh", None, "measurement"),
        ]
        for value_key, name, unit, dev_class, state_class in sensors:
            uid = f"{dev_id}_{value_key}"
            config_topic = f"{prefix}/sensor/{dev_id}/{value_key}/config"
            config_payload: Dict[str, Any] = {
                "name": name,
                "unique_id": uid,
                "state_topic": f"{dev_topic}/state",
                "value_template": f"{{{{ value_json.{value_key} | default(0) }}}}",
                "device": device_info,
            }
            if unit:
                config_payload["unit_of_measurement"] = unit
            if dev_class:
                config_payload["device_class"] = dev_class
            if state_class:
                config_payload["state_class"] = state_class
            try:
                self._client.publish(config_topic, json.dumps(config_payload),
                                     qos=1, retain=True)
            except Exception:
                logger.debug("HA grid discovery failed for %s", value_key)
