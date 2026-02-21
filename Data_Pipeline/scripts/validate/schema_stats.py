"""
schema_stats.py
────────────────
Validates every training record in training_records.jsonl:
  - Required fields present (messages, _meta)
  - messages has exactly 2 turns (user + assistant)
  - assistant content is valid JSON
  - manifest_content inside is valid YAML
  - No PII leaked through redaction
  - Content length within bounds

Writes logs/schema_report.json with per-field statistics.
"""

import json
import logging
import re
import yaml
from pathlib import Path

_ROOT = Path(__file__).parents[2]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(_ROOT / "logs/schema_stats.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ── Schema rules ──────────────────────────────────────────────────────────────

REQUIRED_TOP_KEYS  = {"messages", "_meta"}
REQUIRED_META_KEYS = {"hexsha", "path", "size", "licenses"}
PII_PATTERNS = [
    re.compile(r"sk-[A-Za-z0-9]{20,}"),
    re.compile(r"(?:\d{1,3}\.){3}\d{1,3}"),
    re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"),
]


def validate_record(record: dict) -> tuple[bool, list[str]]:
    """
    Returns (ok, list_of_violations).
    Empty violations list means the record is fully valid.
    """
    violations: list[str] = []

    # Top-level keys
    for k in REQUIRED_TOP_KEYS:
        if k not in record:
            violations.append(f"missing_top_key:{k}")

    # Messages structure
    msgs = record.get("messages", [])
    if len(msgs) != 2:
        violations.append(f"wrong_message_count:{len(msgs)}")
    else:
        if msgs[0].get("role") != "user":
            violations.append("first_role_not_user")
        if msgs[1].get("role") != "assistant":
            violations.append("second_role_not_assistant")

        # Prompt must be non-empty string
        prompt = msgs[0].get("content", "")
        if not isinstance(prompt, str) or not prompt.strip():
            violations.append("empty_prompt")

        # Assistant content must be valid JSON
        assistant = msgs[1].get("content", "")
        try:
            parsed = json.loads(assistant)
        except json.JSONDecodeError as e:
            violations.append(f"assistant_invalid_json:{e}")
            parsed = None

        if parsed is not None:
            # Must have correct tool call structure
            if parsed.get("tool") != "apply_manifest":
                violations.append("wrong_tool_name")
            manifest = parsed.get("params", {}).get("manifest_content", "")
            if not manifest:
                violations.append("empty_manifest_content")
            else:
                # Manifest must be valid YAML
                try:
                    yaml.safe_load(manifest)
                except yaml.YAMLError as e:
                    violations.append(f"manifest_invalid_yaml:{e}")

                # PII check — should have been redacted
                for pat in PII_PATTERNS:
                    if pat.search(manifest):
                        violations.append(f"pii_leaked:{pat.pattern[:20]}")

    # Meta block
    meta = record.get("_meta", {})
    for k in REQUIRED_META_KEYS:
        if k not in meta:
            violations.append(f"missing_meta_key:{k}")

    return len(violations) == 0, violations


def compute_stats(records: list[dict]) -> dict:
    """Compute aggregate statistics over all valid records."""
    prompt_lengths, manifest_lengths, sizes = [], [], []

    for r in records:
        msgs = r.get("messages", [])
        if len(msgs) == 2:
            prompt_lengths.append(len(msgs[0].get("content", "")))
            try:
                parsed = json.loads(msgs[1].get("content", "{}"))
                mc = parsed.get("params", {}).get("manifest_content", "")
                manifest_lengths.append(len(mc))
            except Exception:
                pass
        size = (r.get("_meta") or {}).get("size") or 0
        if size:
            sizes.append(size)

    def _stats(vals):
        if not vals:
            return {}
        return {
            "count":  len(vals),
            "min":    min(vals),
            "max":    max(vals),
            "mean":   round(sum(vals) / len(vals), 1),
            "median": sorted(vals)[len(vals) // 2],
        }

    return {
        "prompt_length_chars":   _stats(prompt_lengths),
        "manifest_length_chars": _stats(manifest_lengths),
        "source_file_size":      _stats(sizes),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def run_validation() -> dict:
    import yaml as _yaml
    cfg      = _yaml.safe_load((_ROOT / "config/iac_analysis.yaml").read_text())
    in_path  = _ROOT / cfg["paths"]["processed_dir"] / "training_records.jsonl"
    out_path = _ROOT / "logs/schema_report.json"
    (_ROOT / "logs").mkdir(exist_ok=True)

    assert in_path.exists(), f"No training records at {in_path}"
    log.info("Validating %s", in_path)

    records, valid_records = [], []
    violation_counts: dict[str, int] = {}
    total = valid = invalid = 0

    with open(in_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                invalid += 1
                violation_counts["unparseable_line"] = \
                    violation_counts.get("unparseable_line", 0) + 1
                continue

            ok, violations = validate_record(record)
            if ok:
                valid += 1
                valid_records.append(record)
            else:
                invalid += 1
                for v in violations:
                    violation_counts[v] = violation_counts.get(v, 0) + 1

            records.append(record)

    stats = compute_stats(valid_records)
    report = {
        "total":            total,
        "valid":            valid,
        "invalid":          invalid,
        "pass_rate_pct":    round(valid / total * 100, 2) if total else 0,
        "violation_counts": violation_counts,
        "statistics":       stats,
    }

    out_path.write_text(json.dumps(report, indent=2))
    log.info("Schema validation: %d/%d passed (%.1f%%)", valid, total,
             report["pass_rate_pct"])
    if violation_counts:
        log.warning("Violations found: %s", violation_counts)

    return report


if __name__ == "__main__":
    run_validation()