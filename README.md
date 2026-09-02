# CSV → Cleaned Dataset Pipeline (n8n + Python + Supabase)

An automated ETL pipeline built in **n8n** that watches a Gmail inbox for incoming CSV attachments, cleans the data using **Python**, loads cleaned records into **Supabase (Postgres)** and **Google Sheets**, and emails a confirmation with the cleaned CSV — or an alert when no rows survive cleaning.

## Architecture

The pipeline flows from Gmail → CSV parsing → Python cleaning/validation → valid-row branching → Supabase/Postgres + Google Sheets + cleaned CSV confirmation, with an alert path for empty results.

### Workflow diagram

[![CSV cleaning ETL pipeline architecture](https://raw.githubusercontent.com/SANSKAR500/csv-cleaning-etl-pipeline-n8n/main/assets/pipeline-animated.svg)](https://github.com/SANSKAR500/csv-cleaning-etl-pipeline-n8n/blob/main/assets/pipeline-animated.svg)

> **GitHub note:** GitHub README pages do not execute local HTML/JavaScript. The diagram above is displayed directly from the repository's raw SVG asset. The interactive HTML remains available as a separate file that you can download or open locally.

### Interactive version

**[Open / download the interactive pipeline HTML](https://github.com/SANSKAR500/csv-cleaning-etl-pipeline-n8n/blob/main/assets/interactive-pipeline.html)**

The interactive page supports clickable workflow nodes and explanatory details. To use the interactions, open the downloaded HTML file in a browser; GitHub's file viewer will show the source code rather than execute it.

### Text version

```text
Gmail Trigger (watches for emails with a .csv attachment)
        ↓
Parse CSV to JSON (reads attachment into structured rows)
        ↓
Clean & Validate Data (Python — type-cast, reject bad rows, dedupe)
        ↓
Has Valid Rows?
        ├── TRUE  → Load to Postgres (Supabase) — upsert on order_id
        ├── TRUE  → Load to Google Sheets — append cleaned rows
        ├── TRUE  → Prepare CSV Columns → Convert to CSV → Confirmation email
        └── FALSE → Alert email (0 valid rows after cleaning)
```

## Why this design

- **Parallel Postgres + Google Sheets branches** let either destination fail independently.
- **Explicit valid-row branching** prevents a silent no-op when all rows are rejected.
- **Rejected rows retain reasons** such as `missing_id`, `invalid_date`, or `invalid_amount` for troubleshooting.
- **Idempotent database loading** uses `order_id` as the upsert key.

## What it does

1. Gmail Trigger polls for messages matching `has:attachment filename:csv`.
2. The CSV attachment is parsed into JSON rows using the header row as field names.
3. A native Python Code node type-casts and validates the data, rejects bad rows, and deduplicates valid records by `order_id`, keeping the most recent `delivery_date`.
4. Valid rows are loaded into Supabase Postgres and Google Sheets in parallel, then converted into a clean CSV and emailed as confirmation.
5. If no rows survive cleaning, an alert email reports the rejected rows and their reasons.

## Cleaning logic

| Source issue | Handling |
|---|---|
| Duplicate `order_id` | Keep the record with the latest `delivery_date` |
| Missing `order_id` / `customer_name` | Reject and tag as `missing_id` |
| Missing / invalid `delivery_date` | Reject and tag as `invalid_date`; four common formats are attempted |
| Currency symbols in `amount` | Strip symbols and parse as `float` |
| Missing / non-numeric `amount` | Reject and tag as `invalid_amount` |
| Mixed-case `status` | Normalize to lowercase |

## Database: Supabase (Postgres)

```sql
CREATE TABLE IF NOT EXISTS deliveries_clean (
  order_id TEXT PRIMARY KEY,
  customer_name TEXT,
  delivery_date DATE,
  amount NUMERIC,
  status TEXT,
  ingested_at TIMESTAMP
);
```

`order_id` is the primary key, making the load idempotent: rerunning the same file updates the existing record instead of creating a duplicate.

> For public-facing deployments, enable Row Level Security (RLS) and use appropriate server-side access controls.

## Tech stack

| Layer | Tool |
|---|---|
| Orchestration | n8n |
| Trigger | Gmail |
| Transformation | Python (n8n Code node) |
| Database | Supabase / Postgres |
| Secondary sink | Google Sheets |
| Notifications | Gmail |

## Repository contents

```text
/assets/
  pipeline-animated.svg
  interactive-pipeline.html

README.md
```

## Setup

1. Import the corresponding n8n workflow into your n8n instance.
2. Create the Supabase project and run the SQL schema above.
3. Connect Gmail, Supabase/Postgres, and Google Sheets credentials in n8n.
4. Configure the target Google Sheet tab as `Cleaned Data`.
5. Update the recipient email used by the confirmation and alert branches.
6. Send a test email containing a CSV attachment and inspect the n8n execution, Supabase table, Google Sheet, and confirmation message.
7. Activate the workflow after the test succeeds.

## Customization

The pipeline uses the generic fields `order_id`, `customer_name`, `delivery_date`, `amount`, and `status`, but the validation rules, deduplication key, output destinations, and trigger can be adapted to other datasets.

Possible extensions include a rejected-row quarantine table, Slack notifications, and scheduled processing summaries.

## License

MIT — feel free to fork, adapt, and reuse.
