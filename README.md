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
developer_metrics_dbt/     dbt project (Silver/Gold layer, in progress)
Dockerfile                 Astro Runtime image
requirements.txt           Python dependencies
packages.txt               OS-level build dependencies
.env.example                Config template (copy to .env, fill in your own values)
```

## Setup

1. Install the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) and Docker.
2. Copy `.env.example` to `.env` and fill in your Snowflake and GitHub credentials.
3. Generate an RSA key pair from the repository root. Keep the private key local and never commit it:
   ```bash
   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM \
     -out include/secrets/snowflake_rsa_key.p8 -nocrypt
   openssl rsa -in include/secrets/snowflake_rsa_key.p8 \
     -pubout -out secrets/snowflake_rsa_key.pub
   chmod 600 include/secrets/snowflake_rsa_key.p8
   ```
4. As a Snowflake administrator, register the public key for the service user. Copy the contents of `secrets/snowflake_rsa_key.pub` without the `BEGIN` and `END` lines:
   ```sql
   ALTER USER <SNOWFLAKE_USER> ADD KEY PAIR developer_metrics_pipeline
     PUBLIC_KEY = '<public-key-body>';
   ```
   The private key must match the registered public key. The command above creates an unencrypted key, so leave `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` empty. If you use an encrypted private key, set its passphrase in `.env`.
5. Run `astro dev start`.
6. Open the Airflow UI, unpause `github_events_to_snowflake_bronze`, and trigger a run.

## Roadmap

- [x] Bronze: hourly ingestion into Snowflake with idempotent MERGE
- [x] RSA key-pair authentication
- [x] Retry/backoff + rate-limit handling
- [ ] Silver: dbt model parsing raw JSON into typed columns
- [ ] Gold: dbt aggregate tables (daily activity per repo, active developers)
- [ ] Wire dbt into Airflow (Cosmos or `BashOperator`)
- [ ] dbt tests + source freshness checks in CI
- [ ] Dashboard / BI layer on top of Gold
