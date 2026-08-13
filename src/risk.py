"""Risk gate — every opportunity passes through here before execution."""
from __future__ import annotations

import logging
import time
from pathlib import Path

from .models import Opportunity

log = logging.getLogger("risk")


class RiskManager:
    def __init__(self, cfg: dict) -> None:
        self.cfg = cfg["risk"]
        self.kill_file = Path(self.cfg["kill_switch_file"])
        self.last_fill: dict[str, float] = {}   # group -> ts
        self.halted_reason: str | None = None

    def allow(self, opp: Opportunity, pnl_today: float, skew_usdt: float) -> bool:
        if self.kill_file.exists():
            self._halt("kill switch file present")
            return False
        if pnl_today <= -abs(self.cfg["max_daily_loss_usdt"]):
            self._halt(f"daily loss cap hit ({pnl_today:.2f} USDT)")
            return False
        if skew_usdt > self.cfg["max_open_skew_usdt"] * 1.5:
            log.warning("skip %s: inventory skew %.0f USDT over limit", opp.group, skew_usdt)
            return False
        last = self.last_fill.get(opp.group, 0.0)
        if time.time() - last < self.cfg["cooldown_s"]:
            return False
        self.halted_reason = None
        return True

    def record_fill(self, group: str) -> None:
        self.last_fill[group] = time.time()

    def _halt(self, reason: str) -> None:
        if self.halted_reason != reason:
            log.error("ENGINE HALTED: %s", reason)
        self.halted_reason = reason
