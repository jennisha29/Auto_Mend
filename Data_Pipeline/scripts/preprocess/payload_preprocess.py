"""
payload_preprocess.py
──────────────────────
Pure-function core transform. No I/O, no state, no config loading.
Takes one raw dict → returns (training_record | None, status_str).

Pipeline order per row:
  1. Filter        — size, ext, alphanum, apiVersion, kind, metadata,
                     YAML validity, license, ML infra keyword
  2. Redact PII    — substitution before any wrapping
  3. Re-validate   — yaml.safe_load after redaction (redaction can break YAML)
  4. Synthesize    — natural-language user prompt from filepath
  5. Wrap + Escape — json.dumps handles ALL escaping
"""

import os
import re
import json
import yaml
from typing import Optional


# ── Compiled artifacts (built once by pipeline, passed in) ────────────────────

def build_redactors(cfg: dict) -> list[tuple[re.Pattern, str]]:
    pats  = cfg["redaction"]["patterns"]
    repls = cfg["redaction"]["replacements"]
    return [
        (re.compile(pats["ipv4"]),                repls["ipv4"]),
        (re.compile(pats["api_key"]),              repls["api_key"]),
        (re.compile(pats["email"]),                repls["email"]),
        (re.compile(pats["creds"], re.IGNORECASE), repls["creds"]),
    ]


def build_prompt_rules(cfg: dict) -> list[dict]:
    return cfg["prompt_rules"]


# ── Internal helpers ──────────────────────────────────────────────────────────

def _collect_licenses(row: dict) -> list[str]:
    """
    Collect license strings from all three Stack license fields.
    Returns lowercased values — comparison to permissive_licenses allowlist
    normalises both sides so matching is correct.
    Note: _meta["licenses"] in output will therefore be lowercase.
    """
    out: list[str] = []
    for key in ("max_stars_repo_licenses", "max_issues_repo_licenses",
                "max_forks_repo_licenses"):
        v = row.get(key) or []
        if isinstance(v, list):
            out.extend(x for x in v if isinstance(x, str) and x.strip())
    return [x.strip().lower() for x in out]


def _best_path(row: dict) -> str:
    """Pick the first non-empty repo path (stars → issues → forks)."""
    for key in ("max_stars_repo_path", "max_issues_repo_path",
                "max_forks_repo_path"):
        v = row.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _k8s_ok(content: str, f: dict) -> bool:
    """
    Returns True only if content satisfies ALL enabled K8s detection gates.
    If all three flags are off, returns True (no K8s requirement → pass through).

    Single source of truth used by both:
      - passes_filter (explicit per-gate error codes)
      - ML infra strict mode (has_k8s) — keeps all three gates coherent.
    """
    if f.get("require_api_version") and "apiVersion" not in content: return False
    if f.get("require_kind")        and "kind:"       not in content: return False
    if f.get("require_metadata")    and "metadata:"   not in content: return False
    return True


def _has_ml_keyword(content: str, cfg: dict) -> bool:
    """True if content contains any keyword from the configured ML infra groups."""
    cl = content.lower()
    for group in (cfg["filters"].get("ml_infra_groups") or []):
        for kw in (cfg["keywords"].get(group) or []):
            if kw.lower() in cl:
                return True
    return False


# ── Step 1 — Filter ───────────────────────────────────────────────────────────

def passes_filter(row: dict, cfg: dict) -> tuple[bool, str]:
    """
    Returns (ok, reason). Checks ordered cheapest → most expensive.
    Any False short-circuits immediately.

    K8s detection controlled by config flags:
      require_api_version → checks "apiVersion" in content → "not_k8s"
      require_kind        → checks "kind:" in content      → "missing_kind"
      require_metadata    → checks "metadata:" in content  → "missing_metadata"
    All three default to off if absent from config.

    ML infra strict mode: has_k8s = _k8s_ok() which mirrors ALL enabled K8s
    gates so all three flags stay coherent in both the explicit gate section
    and the ML infra check.

    License note: _collect_licenses() lowercases all values. Comparison to
    permissive_licenses allowlist normalises both sides so matching is correct.
    _meta["licenses"] in output will be lowercase.
    """
    f        = cfg["filters"]
    content  = row.get("content") or ""
    size     = int(row.get("size") or 0)
    af       = float(row.get("alphanum_fraction") or 0.0)
    ext      = (row.get("ext") or "").lstrip(".").lower()
    licenses = _collect_licenses(row)

    if not content:                                         return False, "empty_content"
    if size < f["min_size_bytes"]:                          return False, "too_small"
    if size > f["max_size_bytes"]:                          return False, "too_large"
    if af < f["min_alphanum_frac"]:                         return False, "low_alphanum"

    allowed = [e.lstrip(".").lower() for e in (f.get("allowed_exts") or [])]
    if allowed and ext and ext not in allowed:              return False, "bad_extension"

    # Explicit K8s gates — each returns its own error code for clear diagnostics
    if f.get("require_api_version") and \
       "apiVersion" not in content:                         return False, "not_k8s"
    if f.get("require_kind") and \
       "kind:" not in content:                              return False, "missing_kind"
    if f.get("require_metadata") and \
       "metadata:" not in content:                          return False, "missing_metadata"

    # YAML validity checked BEFORE ML keyword — catches parse errors early
    # and avoids running keyword search on structurally broken content.
    if f.get("valid_yaml_only"):
        try:
            yaml.safe_load(content)
        except yaml.YAMLError:
            return False, "invalid_yaml"

    allowlist = [l.lower() for l in (f.get("permissive_licenses") or [])]
    if allowlist:
        if f.get("require_license") and not licenses:       return False, "missing_license"
        if licenses and not any(l in allowlist for l in licenses):
                                                            return False, "bad_license"

    if f.get("require_ml_infra"):
        mode    = (f.get("ml_infra_mode") or "strict").lower()
        has_kw  = _has_ml_keyword(content, cfg)
        # _k8s_ok mirrors ALL three enabled K8s gates — consistent with the
        # explicit gate section above. If all flags are off → has_k8s = True.
        has_k8s = _k8s_ok(content, f)
        if mode == "strict":
            if not (has_k8s and has_kw):                    return False, "not_ml_infra"
        else:
            if not (has_k8s or has_kw):                     return False, "not_ml_infra"

    return True, "ok"


# ── Step 2 — PII Redaction ────────────────────────────────────────────────────

def redact(content: str, redactors: list[tuple[re.Pattern, str]]) -> str:
    for pattern, repl in redactors:
        content = pattern.sub(repl, content)
    return content


# ── Step 3 — Prompt Synthesis ─────────────────────────────────────────────────

def synthesize_prompt(filepath: str, rules: list[dict]) -> str:
    name = os.path.basename(filepath or "manifest.yaml")
    slug = re.sub(r"\.(yaml|yml)$", "", name, flags=re.I)
    slug = re.sub(r"[-_]+", " ", slug).strip()
    for rule in rules:
        if rule["match"] == "*" or rule["match"] in slug.lower():
            return rule["template"].format(slug=slug)
    return f"Apply the following Kubernetes manifest for {slug}"


# ── Step 4 — Wrap & Escape ────────────────────────────────────────────────────

def wrap(content: str) -> str:
    """
    json.dumps handles ALL escaping — newlines, quotes, backslashes.
    Never use f-strings or .replace() chains here.
    """
    return json.dumps(
        {"tool": "apply_manifest", "params": {"manifest_content": content}},
        ensure_ascii=False,
    )


# ── Main entry-point ──────────────────────────────────────────────────────────

def process_row(
    row:          dict,
    cfg:          dict,
    redactors:    list[tuple[re.Pattern, str]],
    prompt_rules: list[dict],
) -> tuple[Optional[dict], str]:
    """
    Returns (training_record, "ok") on success.
    Returns (None, reason_str)       on rejection.
    """
    ok, reason = passes_filter(row, cfg)
    if not ok:
        return None, reason

    content = redact(row["content"], redactors)

    # Re-validate after redaction — regex substitution can break YAML structure.
    try:
        yaml.safe_load(content)
    except yaml.YAMLError:
        return None, "invalid_yaml_post_redaction"

    filepath = _best_path(row)
    prompt   = synthesize_prompt(filepath, prompt_rules)
    wrapped  = wrap(content)

    return {
        "messages": [
            {"role": "user",      "content": prompt},
            {"role": "assistant", "content": wrapped},
        ],
        "_meta": {
            "hexsha":   row.get("hexsha"),
            "path":     filepath,
            "size":     row.get("size"),
            "licenses": _collect_licenses(row),
        },
    }, "ok"