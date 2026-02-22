"""
Orchestrator. Reads raw parquet chunks produced by stack_iac_sample.py,
transforms each row via payload_preprocess.py, validates the output,
and writes training records to data/processed/training_records.jsonl.

Memory stays flat: one chunk loaded at a time.
All decisions (thresholds, paths, patterns) live in config/iac_analysis.yaml.

"""
import json
import logging
import sys
import time
import yaml
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq
from tqdm import tqdm

_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(_ROOT))

from scripts.preprocess.payload_preprocess import (
    build_redactors,
    build_prompt_rules,
    process_row,
)

# config
CFG  = yaml.safe_load((_ROOT / "config/iac_analysis.yaml").read_text())
RAW  = _ROOT / CFG["paths"]["raw_dir"]
PROC = _ROOT / CFG["paths"]["processed_dir"]
LOGS = _ROOT / CFG["paths"]["logs_dir"]
PROC.mkdir(parents=True, exist_ok=True)
LOGS.mkdir(parents=True, exist_ok=True)

# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOGS / "preprocess.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# builds compiled objects once
REDACTORS    = build_redactors(CFG)
PROMPT_RULES = build_prompt_rules(CFG)

# validation
# verifies that the assistant content is valid JSON
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

def process_chunk(chunk_path: Path, out_fh, stats: Counter) -> None:
    """Read one parquet chunk and write valid training records to the output file."""
    for row in pq.read_table(chunk_path).to_pylist():
        stats["total"] += 1

        record, status = process_row(row, CFG, REDACTORS, PROMPT_RULES)
        if status != "ok":
            stats[f"drop_{status}"] += 1
            continue

        ok, v_reason = validate(record)
        if not ok:
            stats[f"drop_{v_reason}"] += 1
            continue

        out_fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        stats["written"] += 1

def run() -> None:
    # finds all parquet files written by the download script
    chunks = sorted(RAW.glob("chunk_*.parquet"))
    assert chunks, f"No parquet chunks in {RAW} — run download script first."

    out_path = PROC / "training_records.jsonl"
    # tracks totals like how many read, written and dropped
    stats    = Counter()
    t0       = time.time()

    log.info("Processing %d chunks → %s", len(chunks), out_path)

    with open(out_path, "w", encoding="utf-8") as fh:
        for chunk_path in tqdm(chunks, desc="Chunks"):
            process_chunk(chunk_path, fh, stats)

    elapsed = time.time() - t0
    n, w    = stats["total"], stats["written"]
    pct     = lambda x: f"{x/n*100:.1f}%" if n else "n/a"

    # logs a summary showing how many rows passed and why others were dropped
    log.info("Pipeline complete in %.1fs", elapsed)
    log.info("Rows read: %d | Written: %d (%s yield)", n, w, pct(w))
    for k, v in sorted(stats.items()):
        if k.startswith("drop_"):
            log.info("  drop %-30s %6d  (%s)", k[5:], v, pct(v))
    log.info("Output → %s", out_path)

    log_path = LOGS / CFG["paths"]["pipeline_log"].split("/")[-1]
    log_path.write_text(json.dumps(dict(stats), indent=2))
    log.info("Stats → %s", log_path)


if __name__ == "__main__":
    run()