"""
payload_pipeline.py
────────────────────
Orchestrator. Reads raw parquet chunks produced by stack_iac_sample.py,
transforms each row via payload_preprocess.py, validates the output,
and writes training records to data/processed/training_records.jsonl.

Memory stays flat: one chunk loaded at a time.
All decisions (thresholds, paths, patterns) live in config/iac_analysis.yaml.

Usage:
    python scripts/preprocess/payload_pipeline.py
"""

import json
import sys
import time
import yaml
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq
from tqdm import tqdm

# Make repo root importable regardless of working directory
_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(_ROOT))

from scripts.preprocess.payload_preprocess import (
    build_redactors,
    build_prompt_rules,
    process_row,
)

# ── Config ────────────────────────────────────────────────────────────────────
CFG  = yaml.safe_load((_ROOT / "config/iac_analysis.yaml").read_text())
RAW  = _ROOT / CFG["paths"]["raw_dir"]
PROC = _ROOT / CFG["paths"]["processed_dir"]
LOGS = _ROOT / CFG["paths"]["logs_dir"]
PROC.mkdir(parents=True, exist_ok=True)
LOGS.mkdir(parents=True, exist_ok=True)

# Build compiled objects once — reused for every row
REDACTORS    = build_redactors(CFG)
PROMPT_RULES = build_prompt_rules(CFG)


# ── Validation ────────────────────────────────────────────────────────────────
def validate(record: dict) -> tuple[bool, str]:
    """
    Round-trip check on the assistant content:
      1. Must be valid JSON.
      2. manifest_content inside must be valid YAML.
    This catches any escaping bugs before writing to disk.
    """
    try:
        parsed   = json.loads(record["messages"][1]["content"])
        manifest = parsed["params"]["manifest_content"]
        yaml.safe_load(manifest)
        return True, "ok"
    except json.JSONDecodeError:
        return False, "invalid_json"
    except yaml.YAMLError:
        return False, "invalid_yaml_after_wrap"
    except KeyError as e:
        return False, f"missing_key:{e}"


# ── Chunk processor ───────────────────────────────────────────────────────────
def process_chunk(chunk_path: Path, out_fh, stats: Counter) -> None:
    for row in pq.read_table(chunk_path).to_pylist():
        stats["total"] += 1

        record, status = process_row(row, CFG, REDACTORS, PROMPT_RULES)
        if status != "ok":
            stats[f"drop_{status}"] += 1
            continue

        # process_row already does post-redaction YAML check + wrap;
        # validate() here is the final JSON+YAML round-trip safety net.
        ok, v_reason = validate(record)
        if not ok:
            stats[f"drop_{v_reason}"] += 1
            continue

        out_fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        stats["written"] += 1


# ── Main ──────────────────────────────────────────────────────────────────────
def run() -> None:
    chunks = sorted(RAW.glob("chunk_*.parquet"))
    assert chunks, f"No parquet chunks in {RAW} — run download script first."

    out_path = PROC / "training_records.jsonl"
    stats    = Counter()
    t0       = time.time()

    print(f"Processing {len(chunks)} chunks  →  {out_path}\n")

    with open(out_path, "w", encoding="utf-8") as fh:
        for chunk_path in tqdm(chunks, desc="Chunks"):
            process_chunk(chunk_path, fh, stats)

    elapsed = time.time() - t0
    n, w    = stats["total"], stats["written"]

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*55}")
    print(f"  PIPELINE COMPLETE  ({elapsed:.1f}s)")
    print(f"{'='*55}")
    print(f"  Rows read   : {n}")
    print(f"  Written     : {w}  ({w/n*100:.1f}% yield)")
    print(f"\n  Drop breakdown:")
    for k, v in sorted(stats.items()):
        if k.startswith("drop_"):
            label = k[5:]
            print(f"    {label:<30} {v:>6}  ({v/n*100:.1f}%)")
    print(f"\n  Output  → {out_path}")

    # Persist stats for later inspection / tuning
    log_path = LOGS / CFG["paths"]["pipeline_log"].split("/")[-1]
    log_path.write_text(json.dumps(dict(stats), indent=2))
    print(f"  Log     → {log_path}")


if __name__ == "__main__":
    run()