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
        if self.check_halt(pnl_today):
            return False
        if skew_usdt > self.cfg["max_open_skew_usdt"]:
            return False
        last = self.last_fill.get(opp.group, 0.0)
        if time.time() - last < self.cfg["cooldown_s"]:
            return False
        return True

    def check_halt(self, pnl_today: float) -> bool:
        if self.kill_file.exists():
            self._halt("kill switch file present")
            return True
        if pnl_today <= -abs(self.cfg["max_daily_loss_usdt"]):
            self._halt(f"daily loss cap hit ({pnl_today:.2f} USDT)")
            return True
        self.halted_reason = None
        return False

    def record_fill(self, group: str) -> None:
        self.last_fill[group] = time.time()

    def _halt(self, reason: str) -> None:
        if self.halted_reason != reason:
            log.error("ENGINE HALTED: %s", reason)
        self.halted_reason = reason
