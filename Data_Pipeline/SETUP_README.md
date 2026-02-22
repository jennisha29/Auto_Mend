# AutoMend — Data Pipeline

## 1. Clone & Setup Environment

```bash
git clone <your-repo-url>
cd Auto_Mend/Data_Pipeline
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install dvc
```

## 2. Configure Environment

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

On Windows:

```bash
echo AIRFLOW_UID=50000 > .env
```

## 3. Get HuggingFace Token

1. Sign up at [huggingface.co](https://huggingface.co)
2. Accept dataset terms at [huggingface.co/datasets/bigcode/the-stack-dedup](https://huggingface.co/datasets/bigcode/the-stack-dedup)
3. Generate a read token at [huggingface.co/settings/tokens](https://huggingface.co/settings/tokens)

## 4. Build & Start Airflow

```bash
docker compose build
docker compose up -d
docker compose ps
```

Wait until all services show `(healthy)` before proceeding.

## 5. Access UI

Open: **http://localhost:8081**

Credentials are set in `docker-compose.yaml`
(defaults: `airflow2` / `airflow2`)

## 6. Set HuggingFace Token

```bash
docker compose exec airflow-scheduler airflow variables set HF_TOKEN "hf_your_token_here"
```

## 7. Trigger Pipeline

Click ▶ next to `iac_payload_pipeline` in the UI.

Or via CLI:

```bash
docker compose exec airflow-scheduler airflow dags trigger iac_payload_pipeline
```

## 8. Run Tests

```bash
source .venv/bin/activate
pytest tests/test_pipeline.py -v
```

139 tests, no network required, fully offline.

## 9. Run Offline with DVC

Once `data/raw/` is populated, all stages run without network:

```bash
dvc repro
```

---

## Common Commands

```bash
# View live logs
docker compose logs -f

# Stop Airflow
docker compose down

# Full reset (removes database)
docker compose down -v

# Rebuild image
docker compose build --no-cache

# Restart worker only
docker compose up -d --force-recreate --no-deps airflow-worker

# Run tests
pytest tests/test_pipeline.py -v

# Check task states (quote the run ID)
docker compose exec airflow-scheduler airflow dags list-runs -d iac_payload_pipeline
docker compose exec airflow-scheduler airflow tasks states-for-dag-run \
  iac_payload_pipeline "manual__2026-02-21T16:13:39+00:00"

# DVC
dvc repro        # re-run outdated stages
dvc dag          # view pipeline graph
dvc metrics show # current output metrics
```
