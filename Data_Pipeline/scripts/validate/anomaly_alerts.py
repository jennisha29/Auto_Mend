"""
anomaly_alerts.py
──────────────────
Reads logs/schema_report.json and fires alerts when thresholds are breached.

Anomalies checked:
  - Pass rate below minimum threshold
  - PII leakage detected in output
  - Violation count above maximum
  - Zero records written

Alerts sent via Slack webhook (set SLACK_WEBHOOK_URL env var).
Falls back to WARNING log if no webhook configured.
"""

import json
import logging
import os
import re
import requests
from pathlib import Path

_ROOT = Path(__file__).parents[2]
(_ROOT / "logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(_ROOT / "logs/anomaly_alerts.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ── Thresholds ─────────────────────────────────────────────────────────────────

THRESHOLDS = {
    "min_pass_rate_pct":     80.0,   # alert if <80% of records are valid
    "max_violation_count":   50,     # alert if any single violation exceeds this
    "max_pii_leak_count":    0,      # alert on ANY PII leakage
    "min_total_records":     10,     # alert if fewer than 10 records written
}


# ── Alert dispatch ────────────────────────────────────────────────────────────

def send_alert(message: str) -> None:
    """Send to Slack if webhook is set, else log as CRITICAL."""
    log.critical("ANOMALY ALERT: %s", message)
    webhook = os.environ.get("SLACK_WEBHOOK_URL", "")
    if webhook:
        try:
            resp = requests.post(
                webhook,
                json={"text": f":rotating_light: *IaC Pipeline Anomaly*\n{message}"},
                timeout=5,
            )
            if resp.status_code != 200:
                log.error("Slack webhook returned %d", resp.status_code)
            else:
                log.info("Slack alert sent")
        except requests.RequestException as e:
            log.error("Failed to send Slack alert: %s", e)
    else:
        log.warning("SLACK_WEBHOOK_URL not set — alert logged only")


# ── Anomaly checks (pure functions) ──────────────────────────────────────────

def check_pass_rate(report: dict, threshold: float) -> tuple[bool, str]:
    rate = report.get("pass_rate_pct", 0)
    if rate < threshold:
        return True, (f"Pass rate {rate:.1f}% is below minimum "
                      f"{threshold:.1f}%")
    return False, ""


def check_pii_leakage(report: dict, max_allowed: int) -> tuple[bool, str]:
    violations = report.get("violation_counts", {})
    pii_keys   = [k for k in violations if k.startswith("pii_leaked")]
    total_pii  = sum(violations[k] for k in pii_keys)
    if total_pii > max_allowed:
        return True, f"PII leakage detected in {total_pii} records: {pii_keys}"
    return False, ""


def check_violation_count(report: dict, max_count: int) -> tuple[bool, str]:
    violations = report.get("violation_counts", {})
    over = {k: v for k, v in violations.items() if v > max_count}
    if over:
        return True, f"Violation counts exceed threshold {max_count}: {over}"
    return False, ""


def check_minimum_records(report: dict, minimum: int) -> tuple[bool, str]:
    total = report.get("total", 0)
    if total < minimum:
        return True, f"Only {total} records found — expected at least {minimum}"
    return False, ""


# ── Main ──────────────────────────────────────────────────────────────────────

def run_anomaly_check() -> dict:
    report_path = _ROOT / "logs/schema_report.json"
    out_path    = _ROOT / "logs/anomaly_report.json"

    assert report_path.exists(), \
        f"Schema report not found at {report_path} — run schema_stats.py first"

    report  = json.loads(report_path.read_text())
    anomalies: list[str] = []

    checks = [
        check_pass_rate(report,        THRESHOLDS["min_pass_rate_pct"]),
        check_pii_leakage(report,      THRESHOLDS["max_pii_leak_count"]),
        check_violation_count(report,  THRESHOLDS["max_violation_count"]),
        check_minimum_records(report,  THRESHOLDS["min_total_records"]),
    ]

    for triggered, message in checks:
        if triggered:
            anomalies.append(message)
            send_alert(message)

    result = {
        "anomalies_found": len(anomalies),
        "anomalies":       anomalies,
        "thresholds_used": THRESHOLDS,
        "status":          "FAIL" if anomalies else "PASS",
    }

    out_path.write_text(json.dumps(result, indent=2))

    if anomalies:
        log.warning("Anomaly check FAILED — %d anomalies found", len(anomalies))
    else:
        log.info("Anomaly check PASSED — no issues detected")

    return result


if __name__ == "__main__":
    run_anomaly_check()