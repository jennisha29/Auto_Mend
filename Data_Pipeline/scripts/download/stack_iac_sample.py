"""
stack_iac_sample.py
────────────────────
Stream rows from bigcode/the-stack-dedup (YAML sub-corpus) and write
them to snappy-compressed parquet chunks under data/raw/.

This is the ONLY script that touches the network.
Everything downstream reads local parquet — fully repeatable offline.

Usage:
    python scripts/download/stack_iac_sample.py
    huggingface-cli login  (required once — must accept dataset ToS)
"""

import yaml
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datasets import load_dataset
from tqdm import tqdm

# ── Config ────────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).parents[2]
CFG   = yaml.safe_load((_ROOT / "config/iac_analysis.yaml").read_text())

DS  = CFG["dataset"]
S   = CFG["sampling"]
F   = CFG["fields"]
RAW = _ROOT / CFG["paths"]["raw_dir"]
RAW.mkdir(parents=True, exist_ok=True)

# Only pull columns we actually use — avoids downloading the heavy metadata
PULL = list(F.values())   # e.g. content, size, ext, hexsha, max_stars_repo_path …


# ── Helpers ───────────────────────────────────────────────────────────────────
def _write_chunk(rows: list[dict], idx: int) -> None:
    path = RAW / f"chunk_{idx:04d}.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path, compression="snappy")


# ── Main ──────────────────────────────────────────────────────────────────────
def download() -> None:
    ds = load_dataset(
        DS["repo"],
        data_dir=f"data/{DS['lang']}",
        split=DS["split"],
        streaming=DS["streaming"],
    )

    chunk: list[dict] = []
    chunk_idx = 0
    seen: set[str] = set()          # hexsha dedup within this run

    for row in tqdm(ds, total=S["sample_size"], desc="Streaming"):
        if len(seen) >= S["sample_size"]:
            break

        sha = row.get(F["hexsha"], "")
        if sha in seen:             # skip exact duplicates
            continue
        seen.add(sha)

        chunk.append({col: row.get(col) for col in PULL})

        if len(chunk) >= S["chunk_size"]:
            _write_chunk(chunk, chunk_idx)
            chunk_idx += 1
            chunk = []

    if chunk:                       # flush final partial chunk
        _write_chunk(chunk, chunk_idx)
        chunk_idx += 1

    print(f"\n✅  {len(seen)} rows → {chunk_idx} parquet chunks in {RAW}")


if __name__ == "__main__":
    download()