# Developer Metrics Ingestion Platform

An automated, idempotent pipeline that ingests developer activity events from
the public [GitHub Events API](https://docs.github.com/en/rest/activity/events)
into Snowflake, orchestrated by Apache Airflow.

> **Status:** Bronze (ingestion) layer complete and running hourly. Silver/Gold
> transformation layers (dbt) are in progress — see [Roadmap](#roadmap).

## Architecture

```
GitHub Events API
      │  hourly poll
      ▼
Airflow (Astro CLI, Dockerized)
      │  Python extractor, RSA key-pair auth
      ▼
Snowflake — BRONZE.bronze_github_events (VARIANT, MERGE-deduped)
      │  [planned] dbt
      ▼
Snowflake — SILVER (typed, relational)  →  GOLD (aggregates)
```

## Stack

- **Orchestration:** Apache Airflow via [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview)
- **Extraction:** Python, `requests`, `tenacity` (retry/backoff)
- **Warehouse:** Snowflake, RSA key-pair (JWT) authentication
- **Transformation (planned):** dbt
- **Infra:** Docker

## Key design decisions

- **Idempotent loads.** Rows are staged then merged into the Bronze table on
  `event_id`, so Airflow's hourly polling — which overlaps with GitHub's
  rolling `/events` window — never produces duplicate rows.
- **Key-pair auth, not passwords.** The pipeline authenticates to Snowflake
  with an RSA key pair rather than a stored password, standard practice for
  unattended service accounts.
- **Least-privilege Snowflake role.** The pipeline's role is scoped to only
  the warehouse/database/schema it needs.
- **Resilient extraction.** Exponential-backoff retries on transient GitHub
  API failures, plus GitHub rate-limit monitoring and logging.
- **Fail loud, not silent.** Load failures roll back the transaction and
  raise, so Airflow retries and alerts rather than the pipeline silently
  dropping data.

## Repo layout

```
dags/                      Airflow DAG(s)
include/scripts/           Extraction/load logic
include/secrets/           RSA key pair (git-ignored, not committed)
developer_metrics_dbt/     dbt project (Silver/Gold layer, in progress)
sql/snowflake_setup.sql    One-shot Snowflake bootstrap (role/user/grants)
Dockerfile                 Astro Runtime image
requirements.txt           Python dependencies
packages.txt               OS-level build dependencies
.env.example                Config template (copy to .env, fill in your own values)
```

## Setup

1. Install the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) and Docker.
2. Copy `.env.example` to `.env` and fill in your GitHub token and Snowflake connection details. Leave `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` empty unless you encrypt the private key; the private key path is already filled in and matches step 3 below.
3. Generate an RSA key pair. Keep the private key local and never commit it:
   ```bash
   mkdir -p include/secrets
   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM \
     -out include/secrets/snowflake_rsa_key.p8 -nocrypt
   openssl rsa -in include/secrets/snowflake_rsa_key.p8 \
     -pubout -out include/secrets/snowflake_rsa_key.pub
   chmod 644 include/secrets/snowflake_rsa_key.p8
   ```
   `chmod 644` (not the usual `600`) is required here: Airflow's container
   runs as a non-root user whose UID doesn't match your host user, so a
   stricter `600` makes the file unreadable inside the container. `644`
   means the key is world-readable on your local machine, an acceptable
   trade-off for local development on a single-user machine. For a shared
   or production host, match the container's UID instead of loosening the
   mode (`chown <container-uid> include/secrets/snowflake_rsa_key.p8` with
   mode `600`, or mount the key via a proper secrets manager).
4. As a Snowflake administrator, run [`sql/snowflake_setup.sql`](sql/snowflake_setup.sql)
   to create the service role, user, and grants. It needs the public key body
   from `include/secrets/snowflake_rsa_key.pub`, stripped of its `BEGIN`/`END`
   lines:
   ```bash
   grep -v -- "-----" include/secrets/snowflake_rsa_key.pub | tr -d '\n'
   ```
   Paste that output into the script where indicated, along with your actual
   warehouse name (`SHOW WAREHOUSES;` if you're not sure what's available).
5. Run `astro dev start`.
6. Open the Airflow UI (default local login: `admin` / `admin`), unpause `github_events_to_snowflake_bronze`, and trigger a run.

## Roadmap

- [x] Bronze: hourly ingestion into Snowflake with idempotent MERGE
- [x] RSA key-pair authentication
- [x] Retry/backoff + rate-limit handling
- [ ] Silver: dbt model parsing raw JSON into typed columns
- [ ] Gold: dbt aggregate tables (daily activity per repo, active developers)
- [ ] Wire dbt into Airflow (Cosmos or `BashOperator`)
- [ ] dbt tests + source freshness checks in CI
- [ ] Dashboard / BI layer on top of Gold
