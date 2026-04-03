from __future__ import annotations
import logging
import time
from dataclasses import dataclass, field
from typing import List, Optional

_log = logging.getLogger(__name__)


@dataclass
class Tip:
    category: str  # standby | anomaly | spot | weather | forecast | general
    priority: int  # 1=highest, 5=lowest
    title: str
    description: str
    potential_savings_eur: float = 0.0
    icon: str = "\U0001f4a1"


class RuleBasedAdvisor:
    """Generate energy-saving tips from existing analysis data."""

    def generate_tips(self, db, cfg, storage=None) -> List[Tip]:
        tips: List[Tip] = []

        try:
            tips.extend(self._standby_tips(db, cfg))
        except Exception as e:
            _log.debug("Advisor standby tips: %s", e)

        try:
            tips.extend(self._spot_price_tips(db, cfg))
        except Exception as e:
            _log.debug("Advisor spot tips: %s", e)

        try:
            tips.extend(self._consumption_tips(db, cfg))
        except Exception as e:
            _log.debug("Advisor consumption tips: %s", e)

        try:
            tips.extend(self._weather_tips(db, cfg))
        except Exception as e:
            _log.debug("Advisor weather tips: %s", e)

        try:
            tips.extend(self._general_tips(cfg))
        except Exception as e:
            _log.debug("Advisor general tips: %s", e)

        # Sort by potential savings (highest first), then priority
        tips.sort(key=lambda t: (-t.potential_savings_eur, t.priority))
        return tips

    def _standby_tips(self, db, cfg) -> List[Tip]:
        tips = []
        try:
            from shelly_analyzer.services.standby import analyze_standby
            for dev in cfg.devices:
                if getattr(dev, 'kind', 'em') != 'em':
                    continue
                result = analyze_standby(db, dev.key)
                if result and result.get("annual_cost_eur", 0) > 10:
                    cost = result["annual_cost_eur"]
                    base_w = result.get("base_load_w", 0)
                    tips.append(Tip(
                        category="standby",
                        priority=2,
                        title=f"Standby-Verbrauch: {dev.name}",
                        description=f"Grundlast von {base_w:.0f} W erkannt. "
                                    f"Einsparung bis zu {cost:.0f} \u20ac/Jahr m\u00f6glich durch "
                                    f"Abschalten ungenutzter Ger\u00e4te oder schaltbare Steckdosen.",
                        potential_savings_eur=round(cost, 2),
                        icon="\U0001f50c",
                    ))
        except Exception:
            pass
        return tips

    def _spot_price_tips(self, db, cfg) -> List[Tip]:
        tips = []
        if not getattr(cfg.spot_price, 'enabled', False):
            return tips

        try:
            now = int(time.time())
            cur_h = (now // 3600) * 3600
            zone = cfg.spot_price.bidding_zone

            # Get next 24h prices
            df = db.query_spot_prices(zone, cur_h, cur_h + 24 * 3600)
            if df is not None and not df.empty:
                prices = df["price_eur_mwh"].astype(float)
                min_price = prices.min() / 10.0  # ct/kWh
                max_price = prices.max() / 10.0
                spread = max_price - min_price

                if spread > 5:  # More than 5 ct spread
                    # Find cheapest 3h block
                    min_idx = prices.idxmin()
                    min_ts = int(df.loc[min_idx, "slot_ts"])
                    from datetime import datetime
                    cheap_time = datetime.fromtimestamp(min_ts).strftime("%H:%M")

                    tips.append(Tip(
                        category="spot",
                        priority=1,
                        title="G\u00fcnstigste Stunden nutzen",
                        description=f"Preisspreizung heute: {spread:.1f} ct/kWh. "
                                    f"G\u00fcnstigster Zeitpunkt: {cheap_time} Uhr ({min_price:.1f} ct/kWh). "
                                    f"Gro\u00dfe Verbraucher (Waschmaschine, Trockner) in g\u00fcnstige Stunden verlegen.",
                        potential_savings_eur=round(spread * 5 / 100 * 365, 2),  # Rough estimate: 5 kWh/day
                        icon="\u26a1",
                    ))
        except Exception:
            pass
        return tips

    def _consumption_tips(self, db, cfg) -> List[Tip]:
        tips = []
        try:
            now = int(time.time())
            # Compare last 30 days vs. previous 30 days
            recent_start = now - 30 * 86400
            prev_start = now - 60 * 86400

            recent_kwh = 0.0
            prev_kwh = 0.0

            for dev in cfg.devices:
                if getattr(dev, 'kind', 'em') != 'em':
                    continue
                try:
                    df_recent = db.query_hourly(dev.key, recent_start, now)
                    df_prev = db.query_hourly(dev.key, prev_start, recent_start)
                    if df_recent is not None and not df_recent.empty:
                        recent_kwh += float(df_recent["kwh"].sum())
                    if df_prev is not None and not df_prev.empty:
                        prev_kwh += float(df_prev["kwh"].sum())
                except Exception:
                    pass

            if prev_kwh > 0 and recent_kwh > 0:
                change_pct = ((recent_kwh - prev_kwh) / prev_kwh) * 100
                if change_pct > 10:
                    price = getattr(cfg.pricing, 'electricity_price_eur_per_kwh', 0.30)
                    extra_cost = (recent_kwh - prev_kwh) * price * 12  # Annualized
                    tips.append(Tip(
                        category="forecast",
                        priority=2,
                        title=f"Verbrauch gestiegen (+{change_pct:.0f}%)",
                        description=f"In den letzten 30 Tagen {recent_kwh:.0f} kWh verbraucht "
                                    f"(+{change_pct:.0f}% vs. Vormonat). "
                                    f"Pr\u00fcfen ob neue Verbraucher hinzugekommen sind.",
                        potential_savings_eur=round(max(0, extra_cost), 2),
                        icon="\U0001f4c8",
                    ))
                elif change_pct < -10:
                    tips.append(Tip(
                        category="forecast",
                        priority=4,
                        title=f"Verbrauch gesunken ({change_pct:.0f}%)",
                        description=f"Gute Nachricht! Verbrauch um {abs(change_pct):.0f}% gesunken "
                                    f"({recent_kwh:.0f} kWh vs. {prev_kwh:.0f} kWh).",
                        potential_savings_eur=0,
                        icon="\U0001f389",
                    ))
        except Exception:
            pass
        return tips

    def _weather_tips(self, db, cfg) -> List[Tip]:
        tips = []
        if not getattr(cfg.weather, 'enabled', False):
            return tips

        try:
            now = int(time.time())
            cur_h = (now // 3600) * 3600
            df = db.query_weather(cur_h - 3600, cur_h + 3600)
            if df is not None and not df.empty:
                temp = float(df.iloc[-1].get("temp_c", 20))
                if temp < 5:
                    tips.append(Tip(
                        category="weather",
                        priority=3,
                        title="K\u00e4ltewarnung: Heizkosten beachten",
                        description=f"Aktuelle Au\u00dfentemperatur: {temp:.1f}\u00b0C. "
                                    f"Heizung effizienter einstellen, Sto\u00dfl\u00fcften statt Kippl\u00fcften.",
                        icon="\U0001f321\ufe0f",
                    ))
                elif temp > 28:
                    tips.append(Tip(
                        category="weather",
                        priority=3,
                        title="Hitzewarnung: Klimaanlage optimieren",
                        description=f"Aktuelle Au\u00dfentemperatur: {temp:.1f}\u00b0C. "
                                    f"Rolll\u00e4den tags\u00fcber schlie\u00dfen, Klimaanlage auf 25\u00b0C statt 20\u00b0C.",
                        icon="\u2600\ufe0f",
                    ))
        except Exception:
            pass
        return tips

    def _general_tips(self, cfg) -> List[Tip]:
        tips = []

        # Check if spot prices are enabled
        if not getattr(cfg.spot_price, 'enabled', False):
            tips.append(Tip(
                category="general",
                priority=3,
                title="Dynamische Strompreise aktivieren",
                description="Spot-Preise sind nicht aktiviert. Mit einem dynamischen Tarif "
                            "k\u00f6nnen Sie von g\u00fcnstigen B\u00f6rsenstunden profitieren.",
                icon="\U0001f4b0",
            ))

        # Check if solar is enabled
        if not getattr(cfg.solar, 'enabled', False):
            tips.append(Tip(
                category="general",
                priority=5,
                title="Solar-Monitoring einrichten",
                description="Wenn Sie eine PV-Anlage haben, aktivieren Sie das Solar-Monitoring "
                            "f\u00fcr Eigenverbrauchs- und Amortisationsanalyse.",
                icon="\u2600\ufe0f",
            ))

        return tips


class LlmAdvisor:
    """Optional LLM-based advisor that enriches rule-based tips."""

    def __init__(self, cfg):
        self.cfg = cfg

    def enrich_tips(self, tips: List[Tip], context: str = "") -> str:
        """Send tips + context to LLM for natural language summary."""
        provider = self.cfg.llm_provider

        prompt = self._build_prompt(tips, context)

        try:
            if provider == "ollama":
                return self._call_ollama(prompt)
            elif provider == "openai":
                return self._call_openai(prompt)
            elif provider == "anthropic":
                return self._call_anthropic(prompt)
        except Exception as e:
            _log.warning("LLM advisor error: %s", e)

        return ""

    def _build_prompt(self, tips: List[Tip], context: str) -> str:
        tip_text = "\n".join(
            f"- [{t.category}] {t.title}: {t.description} (Einsparung: {t.potential_savings_eur}\u20ac/Jahr)"
            for t in tips[:10]
        )
        return (
            "Du bist ein Energieberater. Fasse die folgenden Energiespar-Tipps "
            "in einem kurzen, freundlichen Absatz zusammen. Priorisiere nach Einsparpotenzial.\n\n"
            f"Tipps:\n{tip_text}\n\n"
            f"Kontext: {context}\n\n"
            "Antwort (max 150 W\u00f6rter, auf Deutsch):"
        )

    def _call_ollama(self, prompt: str) -> str:
        import requests
        resp = requests.post(
            f"{self.cfg.ollama_url}/api/generate",
            json={"model": self.cfg.llm_model, "prompt": prompt, "stream": False},
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json().get("response", "")
        return ""

    def _call_openai(self, prompt: str) -> str:
        import requests
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {self.cfg.openai_api_key}"},
            json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": prompt}], "max_tokens": 300},
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json()["choices"][0]["message"]["content"]
        return ""

    def _call_anthropic(self, prompt: str) -> str:
        import requests
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": self.cfg.anthropic_api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 300,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json()["content"][0]["text"]
        return ""


def get_advisor_tips(db, cfg, storage=None) -> dict:
    """Main entry point: get tips + optional LLM summary."""
    advisor = RuleBasedAdvisor()
    tips = advisor.generate_tips(db, cfg, storage)

    result = {
        "tips": [
            {
                "category": t.category,
                "priority": t.priority,
                "title": t.title,
                "description": t.description,
                "potential_savings_eur": t.potential_savings_eur,
                "icon": t.icon,
            }
            for t in tips
        ],
        "llm_summary": "",
        "total_savings_potential_eur": round(sum(t.potential_savings_eur for t in tips), 2),
    }

    # Optional LLM enrichment
    if getattr(cfg.advisor, 'use_llm', False) and tips:
        try:
            llm = LlmAdvisor(cfg.advisor)
            result["llm_summary"] = llm.enrich_tips(tips)
        except Exception as e:
            _log.warning("LLM enrichment failed: %s", e)

    return result
