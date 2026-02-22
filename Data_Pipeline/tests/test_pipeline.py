"""

Full test suite — every function in every script.

  - payload_preprocess.py  (passes_filter, _k8s_ok, redact, synthesize_prompt,
                             wrap, process_row)
  - stack_iac_analysis.py  (iac_type, escape_difficulty, has_pii,
                             keyword_hits, size_bucket)
  - schema_stats.py        (validate_record, compute_stats)
  - anomaly_alerts.py      (check_pass_rate, check_pii_leakage,
                             check_violation_count, check_minimum_records)
  - bias_detection.py      (classify_iac_type, classify_size_bucket,
                             classify_prompt_type, classify_license,
                             build_slices, summarise_slices, detect_imbalances)
"""

import json
import re
import sys
import copy
import yaml
import pytest
from pathlib import Path

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))

from scripts.preprocess.payload_preprocess import (
    build_redactors, build_prompt_rules,
    passes_filter, redact, synthesize_prompt, wrap, process_row,
    _k8s_ok,
)
from scripts.analyze.stack_iac_analysis import (
    iac_type, escape_difficulty, has_pii, keyword_hits, size_bucket,
)
from scripts.validate.schema_stats import (
    validate_record, compute_stats,
)
from scripts.validate.anomaly_alerts import (
    check_pass_rate, check_pii_leakage,
    check_violation_count, check_minimum_records,
)
from scripts.validate.bias_detection import (
    classify_iac_type, classify_size_bucket,
    classify_prompt_type, classify_license,
    build_slices, summarise_slices, detect_imbalances,
)

# a kubernetes deployment manifest with a GPU resource limit
VALID_YAML = """\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gpu-model-server
  namespace: ml-serving
spec:
  replicas: 2
  selector:
    matchLabels:
      app: gpu-model-server
  template:
    metadata:
      labels:
        app: gpu-model-server
    spec:
      containers:
        - name: model-server
          image: pytorch/torchserve:latest
          resources:
            limits:
              nvidia.com/gpu: "1"
              memory: "8Gi"
            requests:
              cpu: "2"
"""

# KServe InferenceService manifest
KSERVE_YAML = """\
apiVersion: serving.kserve.io/v1beta1
kind: InferenceService
metadata:
  name: sklearn-iris
  namespace: kserve-test
spec:
  predictor:
    sklearn:
      storageUri: "gs://kfserving-examples/models/sklearn/1.0/model"
"""

# A broken YAML file
# testing if the invalid YAML is correctly rejected.
INVALID_YAML = (
    "apiVersion: apps/v1\n"
    "kind: Deployment\n"
    "metadata:\n"
    "  name: bad\n"
    "  tags: [unclosed, bracket\n"
    "spec:\n"
    "  replicas: 2\n"
)

# gitHub Actions file as not Kubernetes, should be rejected by the K8s gate
NON_K8S_YAML = """\
name: my-ci-pipeline
on: [push]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
"""

# kubernetes file but with no ML keywords, should be rejected by the ML gate
K8S_NO_ML_YAML = """\
apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
  namespace: default
data:
  key: value
  another: setting
  third: entry
"""

@pytest.fixture
def cfg():
    """Load the real config file"""
    return yaml.safe_load((ROOT / "config/iac_analysis.yaml").read_text())

@pytest.fixture
def redactors(cfg):
    """Build compiled PII regex patterns from config."""
    return build_redactors(cfg)

@pytest.fixture
def prompt_rules(cfg):
    """Load the filename-to-prompt rules from config."""
    return build_prompt_rules(cfg)

def _row(content=VALID_YAML, size=None, af=0.6, licenses=None,
         hexsha="abc123", path="gpu-deployment.yaml", ext="yaml"):
    """A minimal raw row dict with the same shape as a row from The Stack."""
    return {
        "content":                  content,
        "size":                     size if size is not None else len(content),
        "alphanum_fraction":        af,
        "ext":                      ext,
        "max_stars_repo_licenses":  licenses if licenses is not None else ["MIT"],
        "max_issues_repo_licenses": [],
        "max_forks_repo_licenses":  [],
        "hexsha":                   hexsha,
        "max_stars_repo_path":      path,
        "max_issues_repo_path":     "",
        "max_forks_repo_path":      "",
    }


def _make_record(prompt="Deploy gpu config",
                 manifest=VALID_YAML,
                 hexsha="abc", path="gpu.yaml", size=500,
                 licenses=None):
    """A valid training record with the same shape as a record in training_records.jsonl."""
    return {
        "messages": [
            {"role": "user",      "content": prompt},
            {"role": "assistant", "content": json.dumps({
                "tool": "apply_manifest",
                "params": {"manifest_content": manifest},
            })},
        ],
        "_meta": {
            "hexsha":   hexsha,
            "path":     path,
            "size":     size,
            "licenses": licenses or ["mit"],
        },
    }


def _safe_ml_keyword(cfg: dict) -> str:
    """
    Return an ML keyword that is safe to embed as a YAML key in test content.
    avoid keywords like 'nvidia.com/gpu' (contains dots and slashes)
    as they break yaml.safe_load when used as unquoted keys.
    """
    groups = cfg["filters"].get("ml_infra_groups", [])
    for group in groups:
        for kw in (cfg["keywords"].get(group) or []):
            if re.match(r'^[a-zA-Z][a-zA-Z0-9_-]*$', kw):
                return kw
    raise AssertionError(
        "No plain-identifier ML keyword found in config — "
        "check ml_infra_groups and keywords sections"
    )

class TestPassesFilter:

    def test_valid_row_passes(self, cfg):
        ok, reason = passes_filter(_row(), cfg)
        assert ok and reason == "ok"

    def test_empty_content(self, cfg):
        ok, r = passes_filter(_row(content=""), cfg)
        assert not ok and r == "empty_content"

    def test_too_small(self, cfg):
        ok, r = passes_filter(_row(size=10), cfg)
        assert not ok and r == "too_small"

    def test_too_large(self, cfg):
        ok, r = passes_filter(_row(size=999_999), cfg)
        assert not ok and r == "too_large"

    def test_low_alphanum(self, cfg):
        # file is mostly binary and whitespace not useful in training data
        ok, r = passes_filter(_row(af=0.10), cfg)
        assert not ok and r == "low_alphanum"

    def test_bad_extension(self, cfg):
        # only want .yaml/.yml files
        ok, r = passes_filter(_row(ext="json"), cfg)
        assert not ok and r == "bad_extension"

    def test_non_k8s(self, cfg):
         # GitHub Actions file with no apiVersion, rejected as not Kubernetes.
        min_size = cfg["filters"]["min_size_bytes"]
        ok, r = passes_filter(_row(content=NON_K8S_YAML, size=min_size + 1), cfg)
        assert not ok and r == "not_k8s"

    def test_invalid_yaml(self, cfg):
        # unclosed bracket so yaml.safe_load fails is rejected
        min_size = cfg["filters"]["min_size_bytes"]
        ok, r = passes_filter(_row(content=INVALID_YAML, size=min_size + 1), cfg)
        assert not ok and r == "invalid_yaml"

    def test_missing_license(self, cfg):
        ok, r = passes_filter(_row(licenses=[]), cfg)
        assert not ok and r == "missing_license"

    def test_bad_license(self, cfg):
        # GPL is not in the permissive license allowlist
        ok, r = passes_filter(_row(licenses=["GPL-3.0"]), cfg)
        assert not ok and r == "bad_license"

    def test_no_ml_keyword(self, cfg):
        # valid K8s ConfigMap but no ML keywords so rejected
        min_size = cfg["filters"]["min_size_bytes"]
        ok, r = passes_filter(_row(content=K8S_NO_ML_YAML,
                                   size=min_size + 1), cfg)
        assert not ok and r == "not_ml_infra"

    def test_multiple_licenses_one_permissive(self, cfg):
        # if any license is permissive, the file passes
        ok, _ = passes_filter(_row(licenses=["GPL-3.0", "MIT"]), cfg)
        assert ok

    def test_all_permissive_licenses(self, cfg):
         # every license in the allowlist should pass
        for lic in cfg["filters"]["permissive_licenses"]:
            ok, reason = passes_filter(_row(licenses=[lic]), cfg)
            assert ok, f"{lic!r} should pass but got: {reason}"

    def test_kserve_passes(self, cfg):
        ok, r = passes_filter(_row(content=KSERVE_YAML,
                                   size=len(KSERVE_YAML),
                                   path="kserve.yaml"), cfg)
        assert ok and r == "ok"

    def test_ml_infra_strict_requires_both_k8s_and_keyword(self, cfg):
        # must be both a K8s manifest and contain an ML keyword
        min_size = cfg["filters"]["min_size_bytes"]
        ok, r = passes_filter(_row(content=K8S_NO_ML_YAML,
                                   size=min_size + 1), cfg)
        assert not ok and r == "not_ml_infra"

    def test_require_api_version_false_relaxes_has_k8s(self, cfg):
        cfg2 = copy.deepcopy(cfg)
        cfg2["filters"]["require_api_version"] = False
        kw      = _safe_ml_keyword(cfg2)
        content = (
            f"kind: Deployment\n"
            f"metadata:\n"
            f"  name: test\n"
            f"spec:\n"
            f"  {kw}: true\n"
        ) * 10
        ok, reason = passes_filter(_row(content=content, size=len(content)), cfg2)
        assert ok, f"Expected ok but got: {reason}"

    def test_require_kind_rejects_missing_kind(self, cfg):
        cfg2     = copy.deepcopy(cfg)
        cfg2["filters"]["require_kind"] = True
        content  = "apiVersion: apps/v1\nspec:\n  nvidia.com/gpu: '1'\n" * 5
        min_size = cfg2["filters"]["min_size_bytes"]
        ok, r    = passes_filter(
            _row(content=content, size=max(len(content), min_size + 1)), cfg2
        )
        assert not ok and r == "missing_kind"

    def test_require_metadata_rejects_missing_metadata(self, cfg):
        cfg2     = copy.deepcopy(cfg)
        cfg2["filters"]["require_metadata"] = True
        content  = "apiVersion: apps/v1\nkind: Deployment\nspec:\n  nvidia.com/gpu: '1'\n" * 5
        min_size = cfg2["filters"]["min_size_bytes"]
        ok, r    = passes_filter(
            _row(content=content, size=max(len(content), min_size + 1)), cfg2
        )
        assert not ok and r == "missing_metadata"


# ═════════════════════════════════════════════════════════════════════════════
#  unit tests for the Kubernetes detection
# ═════════════════════════════════════════════════════════════════════════════

class TestK8sOk:
    """
    _k8s_ok() checks whether a file looks like a Kubernetes manifest.
    """
    def _f(self, api=False, kind=False, meta=False) -> dict:
        return {
            "require_api_version": api,
            "require_kind":        kind,
            "require_metadata":    meta,
        }

    def test_all_flags_off_always_true(self):
        assert _k8s_ok("anything", self._f()) is True

    def test_api_version_required_present(self):
        assert _k8s_ok("apiVersion: v1", self._f(api=True)) is True

    def test_api_version_required_missing(self):
        assert _k8s_ok("kind: Pod", self._f(api=True)) is False

    def test_kind_required_present(self):
        assert _k8s_ok("kind: Deployment", self._f(kind=True)) is True

    def test_kind_required_missing(self):
        assert _k8s_ok("apiVersion: v1", self._f(kind=True)) is False

    def test_metadata_required_present(self):
        assert _k8s_ok("metadata: {}", self._f(meta=True)) is True

    def test_metadata_required_missing(self):
        assert _k8s_ok("apiVersion: v1", self._f(meta=True)) is False

    def test_all_flags_on_full_k8s_passes(self):
        content = "apiVersion: v1\nkind: Pod\nmetadata:\n  name: x"
        assert _k8s_ok(content, self._f(api=True, kind=True, meta=True)) is True

    def test_all_flags_on_partial_fails(self):
        # Has apiVersion and kind but no metadata
        assert _k8s_ok("apiVersion: v1\nkind: Pod\n",
                        self._f(api=True, kind=True, meta=True)) is False

    def test_ml_infra_strict_uses_all_gates(self, cfg):
        cfg2 = copy.deepcopy(cfg)
        cfg2["filters"]["require_kind"]     = True
        cfg2["filters"]["require_metadata"] = True
        content = (
            "apiVersion: apps/v1\n"
            "kind: Deployment\n"
            "spec:\n"
            "  nvidia.com/gpu: '1'\n"
        ) * 5
        min_size = cfg2["filters"]["min_size_bytes"]
        ok, r    = passes_filter(
            _row(content=content, size=max(len(content), min_size + 1)), cfg2
        )
        assert not ok and r == "missing_metadata"


# ═════════════════════════════════════════════════════════════════════════════
# 3. redact PII removal
# ═════════════════════════════════════════════════════════════════════════════

class TestRedact:

    def test_ipv4(self, redactors):
        out = redact("server: 192.168.1.100", redactors)
        assert "192.168.1.100" not in out and "IPV4_REDACTED" in out

    def test_api_key(self, redactors):
        out = redact("key: sk-abcdefghijklmnopqrstuvwxyz", redactors)
        assert "sk-REDACTED" in out

    def test_email(self, redactors):
        out = redact("contact: user@example.com", redactors)
        assert "user@example.com" not in out

    def test_password(self, redactors):
        out = redact("password: supersecret123", redactors)
        assert "supersecret123" not in out

    def test_token(self, redactors):
        out = redact("token: mytoken999", redactors)
        assert "mytoken999" not in out

    def test_clean_unchanged(self, redactors):
        clean = "replicas: 3\nimage: pytorch/serve:latest"
        assert redact(clean, redactors) == clean

    def test_multiple_ips(self, redactors):
        out = redact("a: 10.0.0.1\nb: 172.16.0.2", redactors)
        assert "10.0.0.1"  not in out
        assert "172.16.0.2" not in out
        assert out.count("IPV4_REDACTED") == 2

    def test_yaml_structure_preserved(self, redactors):
        # after redaction, the YAML must parse correctly
        dirty = VALID_YAML + "\n  # admin@corp.com\n  # 10.0.0.5\n"
        yaml.safe_load(redact(dirty, redactors))

# ═════════════════════════════════════════════════════════════════════════════
# synthesize_prompt
# ═════════════════════════════════════════════════════════════════════════════

class TestSynthesizePrompt:

    def test_deploy(self, prompt_rules):
        assert "Deploy" in synthesize_prompt("prod-deployment.yaml", prompt_rules)

    def test_service(self, prompt_rules):
        assert "service" in synthesize_prompt("model-service.yaml", prompt_rules).lower()

    def test_gpu(self, prompt_rules):
        assert "gpu" in synthesize_prompt("gpu-workload.yaml", prompt_rules).lower()

    def test_inference(self, prompt_rules):
        assert "inference" in synthesize_prompt("inference-server.yaml", prompt_rules).lower()

    def test_train(self, prompt_rules):
        assert "train" in synthesize_prompt("train-job.yaml", prompt_rules).lower()

    def test_pipeline(self, prompt_rules):
        assert "pipeline" in synthesize_prompt("ml-pipeline.yaml", prompt_rules).lower()

    def test_fallback(self, prompt_rules):
        p = synthesize_prompt("random-thing.yaml", prompt_rules)
        assert isinstance(p, str) and len(p) > 0

    def test_empty_path(self, prompt_rules):
        assert len(synthesize_prompt("", prompt_rules)) > 0

    def test_extension_stripped(self, prompt_rules):
        # the .yaml extension should not appear in the prompt
        assert ".yaml" not in synthesize_prompt("my-deploy.yaml", prompt_rules)

    def test_underscores_to_spaces(self, prompt_rules):
        assert "_" not in synthesize_prompt("my_deploy.yaml", prompt_rules)


# ═════════════════════════════════════════════════════════════════════════════
# 5. wrap
# ═════════════════════════════════════════════════════════════════════════════

class TestWrap:
    """
    wrap() packages a YAML string into the apply_manifest tool-call format.
    The key guarantee: the YAML must survive a round-trip through JSON
    and still be valid YAML.
    """

    def test_valid_json(self):
        assert json.loads(wrap(VALID_YAML))["tool"] == "apply_manifest"

    def test_content_preserved(self):
        assert json.loads(wrap(VALID_YAML))["params"]["manifest_content"] == VALID_YAML

    def test_newlines_escaped(self):
        assert "\\n" in wrap("line1\nline2")

    def test_quotes_escaped(self):
        assert '\\"' in wrap('key: "value"')

    def test_backslashes_escaped(self):
        assert "\\\\" in wrap("path: C:\\Users")

    def test_round_trip_valid_yaml(self):
        yaml.safe_load(json.loads(wrap(VALID_YAML))["params"]["manifest_content"])

    def test_kserve_round_trip(self):
        yaml.safe_load(json.loads(wrap(KSERVE_YAML))["params"]["manifest_content"])

    def test_unicode_preserved(self):
        content = "# 모델\napiVersion: v1\nkind: Pod"
        assert "모델" in json.loads(wrap(content))["params"]["manifest_content"]


# ═════════════════════════════════════════════════════════════════════════════
# 6. processed Row
# ═════════════════════════════════════════════════════════════════════════════

class TestProcessRow:
    """
    process_row() runs all 5 steps on one raw row:
    filter, redact, re-validate, synthesize prompt and wrap
    """

    def test_valid(self, cfg, redactors, prompt_rules):
        record, status = process_row(_row(), cfg, redactors, prompt_rules)
        assert status == "ok" and record is not None

    def test_messages_structure(self, cfg, redactors, prompt_rules):
        record, _ = process_row(_row(), cfg, redactors, prompt_rules)
        assert record["messages"][0]["role"] == "user"
        assert record["messages"][1]["role"] == "assistant"

    def test_assistant_valid_json(self, cfg, redactors, prompt_rules):
        record, _ = process_row(_row(), cfg, redactors, prompt_rules)
        json.loads(record["messages"][1]["content"])

    def test_manifest_valid_yaml(self, cfg, redactors, prompt_rules):
        record, _ = process_row(_row(), cfg, redactors, prompt_rules)
        parsed = json.loads(record["messages"][1]["content"])
        yaml.safe_load(parsed["params"]["manifest_content"])

    def test_meta_present(self, cfg, redactors, prompt_rules):
        record, _ = process_row(_row(), cfg, redactors, prompt_rules)
        assert "_meta" in record and "hexsha" in record["_meta"]

    def test_rejected_returns_none(self, cfg, redactors, prompt_rules):
        # empty content should be rejected
        record, status = process_row(_row(content=""), cfg, redactors, prompt_rules)
        assert record is None and status != "ok"

    def test_pii_stripped(self, cfg, redactors, prompt_rules):
        # email in a comment should be removed before the record is written
        dirty = VALID_YAML + "\n# ops@company.com\n"
        record, _ = process_row(_row(content=dirty, size=len(dirty)),
                                cfg, redactors, prompt_rules)
        assert "ops@company.com" not in record["messages"][1]["content"]

    def test_kserve_accepted(self, cfg, redactors, prompt_rules):
        _, status = process_row(_row(content=KSERVE_YAML, size=len(KSERVE_YAML),
                                     path="kserve.yaml"), cfg, redactors, prompt_rules)
        assert status == "ok"

    def test_all_rejection_reasons_are_strings(self, cfg, redactors, prompt_rules):
        min_size = cfg["filters"]["min_size_bytes"]
        for row in [
            _row(content=""),
            _row(size=1),
            _row(af=0.01),
            _row(content=NON_K8S_YAML, size=min_size + 1),
            _row(licenses=["GPL-3.0"]),
            _row(content=K8S_NO_ML_YAML, size=min_size + 1),
        ]:
            record, reason = process_row(row, cfg, redactors, prompt_rules)
            assert record is None and isinstance(reason, str)


# ═════════════════════════════════════════════════════════════════════════════
# 7. stack_iac_analysis classifiers
# ═════════════════════════════════════════════════════════════════════════════

class TestIacType:
    """iac_type() identifies what kind of infrastructure file this is."""

    def test_kserve(self):    assert iac_type(KSERVE_YAML) == "kserve"
    def test_seldon(self):    assert iac_type("apiVersion: v1\nkind: SeldonDeployment") == "seldon"
    def test_deployment(self):assert iac_type(VALID_YAML) == "k8s_workload"
    def test_service(self):
        assert iac_type("apiVersion: v1\nkind: Service\nmetadata:\n  name: s") == "k8s_config"
    def test_k8s_other(self):
        assert iac_type("apiVersion: v1\nkind: Namespace\nmetadata:\n  name: n") == "k8s_other"
    def test_terraform(self):
        assert iac_type('resource "aws_instance" "x" {\n  ami="a"\n}') == "terraform"
    def test_other(self):     assert iac_type(NON_K8S_YAML) == "other"


class TestEscapeDifficulty:
    """escape_difficulty() estimates how hard a file would be to JSON-escape."""

    def test_easy(self):
        assert escape_difficulty("replicas: 3") == "easy"

    def test_medium(self):
        assert escape_difficulty(('key: "v"\n') * 260) in ("medium", "hard")

    def test_hard(self):
        assert escape_difficulty(('"x"\n\\p\\\n') * 500) == "hard"

class TestHasPii:
    """has_pii() returns True if the file contains any PII."""
    def test_ip(self):             assert has_pii("host: 10.0.0.1")
    def test_api_key(self):        assert has_pii("key: sk-abcdefghijklmnopqrstuvwxyz")
    def test_email(self):          assert has_pii("a: user@example.com")
    def test_password(self):       assert has_pii("password: secret")
    def test_clean(self):          assert not has_pii("replicas: 3")
    def test_false_positive(self): assert not has_pii("apiVersion: apps/v1")


class TestKeywordHits:
    """keyword_hits() counts how many times each ML keyword appears."""

    def test_gpu_counted(self):
        assert keyword_hits(VALID_YAML)["ml_gpu"]["nvidia.com/gpu"] >= 1

    def test_kserve_counted(self):
        assert keyword_hits(KSERVE_YAML)["kserve_seldon"]["inferenceservice"] >= 1

    def test_absent_is_zero(self):
        assert keyword_hits(VALID_YAML)["kserve_seldon"]["kserve"] == 0


class TestSizeBucket:
    """size_bucket() puts a file into a size category."""

    def test_tiny(self):   assert size_bucket(500)     == "<1KB"
    def test_small(self):  assert size_bucket(5_000)   == "1-10KB"
    def test_medium(self): assert size_bucket(50_000)  == "10-100KB"
    def test_large(self):  assert size_bucket(200_000) == ">100KB"


# ═════════════════════════════════════════════════════════════════════════════
# 8. schema_stats
# ═════════════════════════════════════════════════════════════════════════════

class TestValidateRecord:
    """validate_record() checks one training record against all schema rules."""

    def test_valid_record_passes(self):
        ok, violations = validate_record(_make_record())
        assert ok and violations == []

    def test_missing_messages_key(self):
        r = _make_record(); del r["messages"]
        ok, v = validate_record(r)
        assert not ok and any("missing_top_key" in x for x in v)

    def test_missing_meta_key(self):
        r = _make_record(); del r["_meta"]
        ok, v = validate_record(r)
        assert not ok and any("missing_top_key" in x for x in v)

    def test_wrong_message_count(self):
        r = _make_record()
        r["messages"] = [r["messages"][0]]
        ok, v = validate_record(r)
        assert not ok and any("wrong_message_count" in x for x in v)

    def test_wrong_role_order(self):
        # first message must be from the user
        r = _make_record()
        r["messages"][0]["role"] = "assistant"
        ok, v = validate_record(r)
        assert not ok

    def test_empty_prompt(self):
        r = _make_record(prompt="")
        ok, v = validate_record(r)
        assert not ok and any("empty_prompt" in x for x in v)

    def test_invalid_json_in_assistant(self):
        # assistant content must be valid JSON
        r = _make_record()
        r["messages"][1]["content"] = "not json {"
        ok, v = validate_record(r)
        assert not ok and any("invalid_json" in x for x in v)

    def test_wrong_tool_name(self):
        # apply_manifest tool must be used
        r = _make_record()
        content = json.loads(r["messages"][1]["content"])
        content["tool"] = "wrong_tool"
        r["messages"][1]["content"] = json.dumps(content)
        ok, v = validate_record(r)
        assert not ok and any("wrong_tool_name" in x for x in v)

    def test_invalid_yaml_manifest(self):
        # the manifest inside must be valid YAML
        r = _make_record(manifest=": broken: [")
        ok, v = validate_record(r)
        assert not ok and any("invalid_yaml" in x for x in v)

    def test_pii_leaked_flagged(self):
        r = _make_record(manifest=VALID_YAML + "\n# admin@corp.com\n")
        ok, v = validate_record(r)
        assert not ok and any("pii_leaked" in x for x in v)

    def test_missing_meta_subkey(self):
        r = _make_record()
        del r["_meta"]["hexsha"]
        ok, v = validate_record(r)
        assert not ok and any("missing_meta_key:hexsha" in x for x in v)

    def test_kserve_record_passes(self):
        ok, v = validate_record(_make_record(manifest=KSERVE_YAML))
        assert ok and v == []


class TestComputeStats:
    """compute_stats() calculates summary statistics over a list of valid records."""

    def test_stats_keys_present(self):
        stats = compute_stats([_make_record() for _ in range(3)])
        assert "prompt_length_chars" in stats
        assert "manifest_length_chars" in stats
        assert "source_file_size" in stats

    def test_empty_records(self):
        assert compute_stats([])["prompt_length_chars"] == {}

    def test_mean_is_correct(self):
        r1 = _make_record(prompt="AB",   size=100)
        r2 = _make_record(prompt="ABCD", size=300)
        stats = compute_stats([r1, r2])
        assert stats["prompt_length_chars"]["mean"] == 3.0
        assert stats["source_file_size"]["mean"] == 200.0


# ═════════════════════════════════════════════════════════════════════════════
# 9. anomaly_alerts — threshold check functions
# ═════════════════════════════════════════════════════════════════════════════

class TestAnomalyChecks:
    """
    Each check function takes a report dict and a threshold.
    Returns (triggered=True, message) if the threshold is breached.
    Returns (triggered=False, "") if everything is fine.
    """

    def _report(self, pass_rate=95.0, violations=None, total=100):
        return {
            "pass_rate_pct":    pass_rate,
            "violation_counts": violations or {},
            "total":            total,
        }

    def test_pass_rate_ok(self):
        triggered, _ = check_pass_rate(self._report(pass_rate=95.0), 80.0)
        assert not triggered

    def test_pass_rate_fails(self):
        triggered, msg = check_pass_rate(self._report(pass_rate=50.0), 80.0)
        assert triggered and "50.0%" in msg

    def test_pass_rate_exact_boundary(self):
        triggered, _ = check_pass_rate(self._report(pass_rate=80.0), 80.0)
        assert not triggered

    def test_no_pii(self):
        triggered, _ = check_pii_leakage(self._report(), max_allowed=0)
        assert not triggered

    def test_pii_detected(self):
        r = self._report(violations={"pii_leaked:sk-": 3})
        triggered, msg = check_pii_leakage(r, max_allowed=0)
        assert triggered and "3" in msg

    def test_pii_within_allowed(self):
        r = self._report(violations={"pii_leaked:sk-": 2})
        triggered, _ = check_pii_leakage(r, max_allowed=5)
        assert not triggered

    def test_no_violations(self):
        triggered, _ = check_violation_count(self._report(), max_count=50)
        assert not triggered

    def test_violation_exceeds_threshold(self):
        r = self._report(violations={"invalid_yaml": 60})
        triggered, msg = check_violation_count(r, max_count=50)
        assert triggered and "invalid_yaml" in msg

    def test_violation_at_threshold(self):
        r = self._report(violations={"invalid_yaml": 50})
        triggered, _ = check_violation_count(r, max_count=50)
        assert not triggered

    def test_enough_records(self):
        triggered, _ = check_minimum_records(self._report(total=100), minimum=10)
        assert not triggered

    def test_too_few_records(self):
        triggered, msg = check_minimum_records(self._report(total=3), minimum=10)
        assert triggered and "3" in msg

    def test_zero_records(self):
        triggered, _ = check_minimum_records(self._report(total=0), minimum=1)
        assert triggered


# ═════════════════════════════════════════════════════════════════════════════
# 10. bias_detection
# ═════════════════════════════════════════════════════════════════════════════

class TestSliceClassifiers:
    """Each classifier puts a record into one of several named categories."""

    def test_kserve(self):    assert classify_iac_type(KSERVE_YAML) == "kserve"
    def test_deployment(self):assert classify_iac_type(VALID_YAML)  == "k8s_workload"
    def test_service(self):
        assert classify_iac_type("apiVersion: v1\nkind: Service") == "k8s_config"
    def test_other(self):
        assert classify_iac_type("name: thing\nvalue: 1") == "other"

    def test_small(self):  assert classify_size_bucket(500)     == "<1KB"
    def test_medium(self): assert classify_size_bucket(5_000)   == "1-10KB"
    def test_large(self):  assert classify_size_bucket(50_000)  == "10-100KB"
    def test_huge(self):   assert classify_size_bucket(200_000) == ">100KB"

    def test_deploy_prompt(self):    assert classify_prompt_type("Deploy the config")  == "deploy"
    def test_gpu_prompt(self):       assert classify_prompt_type("Provision GPU")      == "gpu"
    def test_inference_prompt(self): assert classify_prompt_type("Set up inference")   == "inference"
    def test_service_prompt(self):   assert classify_prompt_type("Apply service")      == "service"
    def test_fallback_prompt(self):  assert classify_prompt_type("Apply the manifest") == "fallback"

    def test_mit(self):          assert classify_license(["MIT"])        == "mit"
    def test_apache(self):       assert classify_license(["Apache-2.0"]) == "apache-2.0"
    def test_unknown(self):      assert classify_license([])             == "unknown"
    def test_gpl_is_other(self): assert classify_license(["GPL-3.0"])    == "other"
    def test_case_insensitive(self): assert classify_license(["MIT"])    == "mit"


class TestBuildSlices:
    """build_slices() groups records into buckets across all 4 dimensions."""

    def test_slices_have_four_dimensions(self):
        slices = build_slices([_make_record() for _ in range(5)])
        assert set(slices.keys()) == {"iac_type", "license", "size_bucket", "prompt_type"}

    def test_count_matches_records(self):
        slices = build_slices([_make_record() for _ in range(10)])
        assert sum(v["count"] for v in slices["iac_type"].values()) == 10

    def test_empty_records(self):
        slices = build_slices([])
        for dim in ("iac_type", "license", "size_bucket", "prompt_type"):
            assert sum(v["count"] for v in slices[dim].values()) == 0

    def test_kserve_slice_counted(self):
        records = [_make_record(manifest=KSERVE_YAML, path="kserve.yaml")
                   for _ in range(3)]
        assert build_slices(records)["iac_type"]["kserve"]["count"] == 3


class TestSummariseSlices:
    """summarise_slices() converts counts into percentages and adds imbalance flags."""

    def test_pct_sums_to_100(self):
        records = [_make_record() for _ in range(10)]
        summary = summarise_slices(build_slices(records), 10)
        for dim in summary:
            total_pct = sum(v["pct_of_total"] for v in summary[dim].values())
            assert abs(total_pct - 100.0) < 0.1

    def test_underrepresented_flag(self):
        # deploy=1/10=10% — above MIN_SLICE_PCT=5% → not flagged
        records = (
            [_make_record(prompt="Deploy config")] +
            [_make_record(prompt="Provision GPU workload") for _ in range(9)]
        )
        summary = summarise_slices(build_slices(records), 10)
        assert not summary["prompt_type"]["deploy"]["underrepresented"]


class TestDetectImbalances:
    """detect_imbalances() returns plain english messages for any flagged categories."""

    def test_no_imbalances_when_single_slice(self):
        records = [_make_record() for _ in range(10)]
        summary = summarise_slices(build_slices(records), 10)
        assert isinstance(detect_imbalances(summary), list)

    def test_imbalance_message_contains_dimension(self):
        records = (
            [_make_record(prompt="Deploy config")] +
            [_make_record(prompt="Provision GPU workload") for _ in range(99)]
        )
        summary = summarise_slices(build_slices(records), 100)
        msgs    = detect_imbalances(summary)
        assert any("prompt_type" in m for m in msgs)