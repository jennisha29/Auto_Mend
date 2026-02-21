"""
stack_iac_analysis.py
──────────────────────
Reads raw parquet chunks (from stack_iac_sample.py) and produces a
JSON report covering: field nulls, size distribution, IaC type breakdown,
keyword frequencies, PII hit rate, and JSON-escape difficulty.

No network. No state. Fully repeatable.

Usage:
    python scripts/analyze/stack_iac_analysis.py
"""

import re
import json
import yaml
import collections
from pathlib import Path

import pyarrow.parquet as pq
from tqdm import tqdm

# ── Config ────────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).parents[2]
CFG   = yaml.safe_load((_ROOT / "config/iac_analysis.yaml").read_text())

F    = CFG["fields"]
KWS  = CFG["keywords"]
RAW  = _ROOT / CFG["paths"]["raw_dir"]
LOGS = _ROOT / CFG["paths"]["logs_dir"]
LOGS.mkdir(parents=True, exist_ok=True)

# Compile PII patterns once
_PII = {k: re.compile(v) for k, v in CFG["redaction"]["patterns"].items()}


# ── Pure classifiers ──────────────────────────────────────────────────────────
def iac_type(content: str) -> str:
    c = content[:2000]
    if re.search(r"InferenceService|kserve",                          c): return "kserve"
    if re.search(r"seldon|SeldonDeployment",                          c): return "seldon"
    if re.search(r"kind:\s*(Deployment|StatefulSet|DaemonSet|Job)",   c): return "k8s_workload"
    if re.search(r"kind:\s*(Service|Ingress|ConfigMap|Secret)",       c): return "k8s_config"
    if re.search(r"apiVersion",                                        c): return "k8s_other"
    if re.search(r'resource\s+"|terraform\s*{',                       c): return "terraform"
    return "other"


def escape_difficulty(content: str) -> str:
    score = content.count("\n") + content.count('"') * 2 + content.count("\\") * 3
    return "hard" if score > 5000 else "medium" if score > 1000 else "easy"


def has_pii(content: str) -> bool:
    return any(p.search(content) for p in _PII.values())


def keyword_hits(content: str) -> dict[str, dict[str, int]]:
    return {
        cat: {w: len(re.findall(re.escape(w), content, re.I)) for w in words}
        for cat, words in KWS.items()
    }


def size_bucket(size: int) -> str:
    if size < 1_000:    return "<1KB"
    if size < 10_000:   return "1-10KB"
    if size < 100_000:  return "10-100KB"
    return ">100KB"


# ── Main ──────────────────────────────────────────────────────────────────────
def analyze() -> None:
    chunks = sorted(RAW.glob("chunk_*.parquet"))
    assert chunks, f"No parquet chunks in {RAW} — run download script first."

    # Accumulators
    total     = 0
    pii_count = 0
    types     = collections.Counter()
    escape    = collections.Counter()
    sizes     = collections.Counter()
    nulls     = collections.Counter()
    keywords  = {cat: collections.Counter() for cat in KWS}
    licenses  = collections.Counter()

    for path in tqdm(chunks, desc="Analyzing"):
        for row in pq.read_table(path).to_pylist():
            total     += 1
            content    = row.get(F["content"]) or ""
            size       = row.get(F["size"]) or 0

            # Field null audit
            for col, hf_name in F.items():
                if row.get(hf_name) is None:
                    nulls[col] += 1

            types[iac_type(content)]          += 1
            escape[escape_difficulty(content)] += 1
            sizes[size_bucket(size)]           += 1
            if has_pii(content):
                pii_count += 1

            for cat, counts in keyword_hits(content).items():
                for w, n in counts.items():
                    if n:
                        keywords[cat][w] += n

            for lic in (row.get(F["repo_licenses"]) or []):
                licenses[lic.lower()] += 1

    # ── Build report ──────────────────────────────────────────────────────────
    report = {
        "total_rows":        total,
        "pii_files":         {"count": pii_count, "pct": round(pii_count / total * 100, 2)},
        "iac_type_dist":     dict(types.most_common()),
        "escape_difficulty": dict(escape.most_common()),
        "size_distribution": dict(sizes.most_common()),
        "field_null_pct":    {k: round(v / total * 100, 2) for k, v in nulls.items()},
        "top_keywords":      {cat: dict(ctr.most_common(15)) for cat, ctr in keywords.items()},
        "top_licenses":      dict(licenses.most_common(15)),
    }

    out = _ROOT / CFG["paths"]["analysis_out"]
    out.write_text(json.dumps(report, indent=2))

    # ── Console summary ───────────────────────────────────────────────────────
    print(f"\n{'='*55}")
    print(f"  ANALYSIS  ({total} rows, {len(chunks)} chunks)")
    print(f"{'='*55}")
    print("\nIaC Types:")
    for k, v in types.most_common():
        print(f"  {k:<20} {v:>6}  ({v/total*100:.1f}%)")
    print("\nEscape Difficulty:")
    for k, v in escape.most_common():
        print(f"  {k:<10} {v:>6}  ({v/total*100:.1f}%)")
    print(f"\nPII Files: {pii_count} ({pii_count/total*100:.1f}%)")
    print(f"\n✅  Report → {out}")


if __name__ == "__main__":
    analyze()