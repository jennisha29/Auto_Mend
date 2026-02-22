"""
Stream rows from bigcode/the-stack-dedup (YAML sub-corpus) and write
them to compressed parquet chunks under data/raw/.

This is the ONLY script that touches the network.
Everything downstream reads local parquet, fully repeatable offline.

"""

import logging
import yaml
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datasets import load_dataset
from tqdm import tqdm

# config
_ROOT = Path(__file__).parents[2]
CFG   = yaml.safe_load((_ROOT / "config/iac_analysis.yaml").read_text())

DS  = CFG["dataset"]
S   = CFG["sampling"]
F   = CFG["fields"]
# folder to save parquet files
RAW = _ROOT / CFG["paths"]["raw_dir"]
RAW.mkdir(parents=True, exist_ok=True)

# logging
(_ROOT / CFG["paths"]["logs_dir"]).mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(_ROOT / CFG["paths"]["logs_dir"] / "download.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# only pulls required columns 
PULL = list(F.values())   # e.g. content, size, ext, hexsha, max_stars_repo_path

# helpers
def _write_chunk(rows: list[dict], idx: int) -> None:
    path = RAW / f"chunk_{idx:04d}.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path, compression="snappy")

def download() -> None:
    # connects to HuggingFace and starts streaming the YAML sub-corpus
    ds = load_dataset(
        DS["repo"],
        data_dir=f"data/{DS['lang']}",
        split=DS["split"],
        streaming=DS["streaming"],
    )

    chunk: list[dict] = []
    chunk_idx = 0
    # tracks file hashes to skip exact duplicates
    seen: set[str] = set()

    for row in tqdm(ds, total=S["sample_size"], desc="Streaming"):
        if len(seen) >= S["sample_size"]:
            break

        sha = row.get(F["hexsha"], "")
        if sha in seen:
            continue
        seen.add(sha)

        chunk.append({col: row.get(col) for col in PULL})

        if len(chunk) >= S["chunk_size"]:
            _write_chunk(chunk, chunk_idx)
            chunk_idx += 1
            chunk = []

    if chunk:
        _write_chunk(chunk, chunk_idx)
        chunk_idx += 1

    log.info("Download complete — %d rows → %d parquet chunks in %s", len(seen), chunk_idx, RAW)


if __name__ == "__main__":
    download()