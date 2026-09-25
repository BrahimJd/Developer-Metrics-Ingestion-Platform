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

1. `astro dev init` prerequisites: [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli), Docker.
2. Copy `.env.example` to `.env` and fill in your own Snowflake/GitHub credentials.
3. Generate an RSA key pair for Snowflake key-pair auth and register the
   public key on your Snowflake service user (see `docs/snowflake-setup.md` —
   *TODO: write this up*).
4. `astro dev start`
5. Open the Airflow UI, unpause `github_events_to_snowflake_bronze`, trigger a run.

## Roadmap

- [x] Bronze: hourly ingestion into Snowflake with idempotent MERGE
- [x] RSA key-pair authentication
- [x] Retry/backoff + rate-limit handling
- [ ] Silver: dbt model parsing raw JSON into typed columns
- [ ] Gold: dbt aggregate tables (daily activity per repo, active developers)
- [ ] Wire dbt into Airflow (Cosmos or `BashOperator`)
- [ ] dbt tests + source freshness checks in CI
- [ ] Dashboard / BI layer on top of Gold

## License

*(add one, e.g. MIT, if you want this repo to be reusable by others)*