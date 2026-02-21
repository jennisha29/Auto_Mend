"""
bias_detection.py
──────────────────
Slices the processed training records across meaningful dimensions and
reports representation imbalance:

  Slices:
    - iac_type        (kserve / seldon / k8s_workload / k8s_config / other)
    - license         (mit / apache-2.0 / etc.)
    - size_bucket     (<1KB / 1-10KB / 10-100KB / >100KB)
    - prompt_type     (deploy / gpu / inference / service / fallback / etc.)

  Metrics per slice:
    - count + percentage of total
    - mean manifest length
    - imbalance flag if slice < MIN_SLICE_PCT of total

  Outputs logs/bias_report.json
"""

import json
import logging
import re
import yaml
from collections import defaultdict
from pathlib import Path

_ROOT = Path(__file__).parents[2]
(_ROOT / "logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(_ROOT / "logs/bias_detection.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

MIN_SLICE_PCT = 5.0     # slices below this % of total are flagged as underrepresented


# ── Slice classifiers (pure functions) ───────────────────────────────────────

def classify_iac_type(manifest: str) -> str:
    c = manifest[:2000]
    if re.search(r"InferenceService|kserve",                        c): return "kserve"
    if re.search(r"seldon|SeldonDeployment",                        c): return "seldon"
    if re.search(r"kind:\s*(Deployment|StatefulSet|DaemonSet|Job)", c): return "k8s_workload"
    if re.search(r"kind:\s*(Service|Ingress|ConfigMap|Secret)",     c): return "k8s_config"
    if re.search(r"apiVersion",                                      c): return "k8s_other"
    return "other"


def classify_size_bucket(size: int) -> str:
    if size < 1_000:   return "<1KB"
    if size < 10_000:  return "1-10KB"
    if size < 100_000: return "10-100KB"
    return ">100KB"


def classify_prompt_type(prompt: str) -> str:
    p = prompt.lower()
    if "deploy"    in p: return "deploy"
    if "gpu"       in p: return "gpu"
    if "inference" in p: return "inference"
    if "service"   in p: return "service"
    if "train"     in p: return "train"
    if "pipeline"  in p: return "pipeline"
    if "ingress"   in p: return "ingress"
    if "secret"    in p: return "secret"
    if "config"    in p: return "config"
    return "fallback"


def classify_license(licenses: list) -> str:
    if not licenses:
        return "unknown"
    # normalise and return the first known license
    known = {"mit", "apache-2.0", "bsd-2-clause", "bsd-3-clause",
             "isc", "cc0-1.0", "unlicense", "wtfpl", "0bsd"}
    for lic in licenses:
        l = str(lic).lower().strip()
        if l in known:
            return l
    return "other"


# ── Slice analysis ────────────────────────────────────────────────────────────

def build_slices(records: list[dict]) -> dict:
    """
    Returns dict of dimension → {slice_value → {count, manifest_lengths}}.
    """
    slices: dict[str, dict] = {
        "iac_type":    defaultdict(lambda: {"count": 0, "manifest_lengths": []}),
        "license":     defaultdict(lambda: {"count": 0, "manifest_lengths": []}),
        "size_bucket": defaultdict(lambda: {"count": 0, "manifest_lengths": []}),
        "prompt_type": defaultdict(lambda: {"count": 0, "manifest_lengths": []}),
    }

    for record in records:
        msgs = record.get("messages", [])
        meta = record.get("_meta", {})
        if len(msgs) != 2:
            continue

        prompt = msgs[0].get("content", "")
        try:
            parsed   = json.loads(msgs[1].get("content", "{}"))
            manifest = parsed.get("params", {}).get("manifest_content", "")
        except Exception:
            continue

        size     = int(meta.get("size") or 0)
        licenses = meta.get("licenses") or []
        mlen     = len(manifest)

        for dim, val in [
            ("iac_type",    classify_iac_type(manifest)),
            ("license",     classify_license(licenses)),
            ("size_bucket", classify_size_bucket(size)),
            ("prompt_type", classify_prompt_type(prompt)),
        ]:
            slices[dim][val]["count"]             += 1
            slices[dim][val]["manifest_lengths"].append(mlen)

    return slices


def summarise_slices(slices: dict, total: int) -> dict:
    """Convert raw slice data into a reportable summary with imbalance flags."""
    summary = {}
    for dim, groups in slices.items():
        summary[dim] = {}
        for val, data in groups.items():
            count = data["count"]
            pct   = round(count / total * 100, 2) if total else 0
            mlens = data["manifest_lengths"]
            summary[dim][val] = {
                "count":               count,
                "pct_of_total":        pct,
                "mean_manifest_chars": round(sum(mlens)/len(mlens), 1) if mlens else 0,
                "underrepresented":    pct < MIN_SLICE_PCT,
            }
    return summary


def detect_imbalances(summary: dict) -> list[str]:
    """Return human-readable imbalance messages for the report."""
    messages = []
    for dim, groups in summary.items():
        flagged = [v for v, s in groups.items() if s["underrepresented"]]
        if flagged:
            messages.append(
                f"[{dim}] Underrepresented slices (<{MIN_SLICE_PCT}%): {flagged}"
            )
    return messages


# ── Main ──────────────────────────────────────────────────────────────────────

def run_bias_detection() -> dict:
    cfg      = yaml.safe_load((_ROOT / "config/iac_analysis.yaml").read_text())
    in_path  = _ROOT / cfg["paths"]["processed_dir"] / "training_records.jsonl"
    out_path = _ROOT / "logs/bias_report.json"

    assert in_path.exists(), f"No training records at {in_path}"
    log.info("Loading records from %s", in_path)

    records = []
    with open(in_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    total = len(records)
    log.info("Analysing %d records across 4 slice dimensions", total)

    slices     = build_slices(records)
    summary    = summarise_slices(slices, total)
    imbalances = detect_imbalances(summary)

    report = {
        "total_records":   total,
        "min_slice_pct":   MIN_SLICE_PCT,
        "imbalances_found": len(imbalances),
        "imbalances":      imbalances,
        "slices":          summary,
    }

    out_path.write_text(json.dumps(report, indent=2))

    if imbalances:
        log.warning("Bias detection found %d imbalances:", len(imbalances))
        for msg in imbalances:
            log.warning("  %s", msg)
    else:
        log.info("No imbalances detected")

    return report


if __name__ == "__main__":
    run_bias_detection()