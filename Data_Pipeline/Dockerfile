# Dockerfile
# ─────────────────────────────────────────────────────────────────────────────
# Extends the official Airflow 2.9.2 image with all pipeline dependencies.
#
# Build:   docker-compose build
# Run:     docker-compose up
#
# The base image as published on Docker Hub ships with versions that are
# incompatible with Airflow 2.9.2's own codebase:
#   celery==5.4.0                          — changed worker_main() to crash on
#                                            None hostname (NoneType.split())
#   apache-airflow-providers-celery==3.7.2 — imports ARG_AUTOSCALE from
#                                            airflow.cli.cli_config, only present
#                                            from Airflow 2.9.3+
#
# Pinned to the compatible pairing for Airflow 2.9.2:
#   celery==5.3.6             — worker_main() defaults hostname when not provided
#   providers-celery==3.6.0   — does not require ARG_AUTOSCALE
#   click==8.1.7              — defensive pin; click upgrades can silently break
#                               Airflow's CLI argument parsing

FROM apache/airflow:2.9.2

RUN python -m pip install --no-cache-dir \
    "celery==5.3.6" \
    "apache-airflow-providers-celery==3.6.0" \
    "click==8.1.7" \
    "datasets==2.20.0" \
    "huggingface_hub==0.23.4" \
    "pyarrow==16.1.0" \
    "pyyaml==6.0.2" \
    "tqdm==4.66.4" \
    "requests==2.32.3" \
    "pandas==2.2.2" \
    "numpy==1.26.4"