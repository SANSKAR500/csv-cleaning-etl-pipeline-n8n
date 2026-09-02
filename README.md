# CSV → Cleaned Dataset Pipeline (n8n + Python + Supabase)

An automated ETL pipeline built in **n8n** that watches a Gmail inbox for incoming CSV attachments, cleans the data using a **Python** script (deduplication, type-casting, null/invalid-row handling), loads the cleaned records into a **Supabase (Postgres)** database and a **Google Sheet**, and emails back a confirmation with the cleaned file attached — or an alert if nothing survived cleaning.

Built as a hands-on demonstration of ETL fundamentals: extraction, validation, transformation, loading, and failure handling — the same core skills used in real data analyst / data engineering workflows.

---

## Architecture

```
Gmail Trigger (watches for emails with a .csv attachment)
        ↓
Parse CSV to JSON  (reads the attachment into structured rows)
        ↓
Clean & Validate Data  (Python — type-cast, reject bad rows, dedupe)
        ↓
Has Valid Rows?
        ├── TRUE ──▶ Load to Postgres (Supabase)  — upsert on order_id
        ├── TRUE ──▶ Load to Google Sheets  — append cleaned rows
        ├── TRUE ──▶ Prepare CSV Columns ──▶ Convert Cleaned Data to CSV ──▶ Send confirmation email (with cleaned CSV attached)
        └── FALSE ─▶ Send alert email ("0 valid rows after cleaning")
```

**Why this design:**
- Splitting **Load to Postgres** and **Load to Google Sheets** into parallel branches means either destination can fail independently without blocking the other.
- The **"Has Valid Rows?"** branch prevents the pipeline from silently loading nothing — you get an explicit alert instead of a quiet no-op.
- Rejected rows aren't just dropped — each one is tagged with *why* it was rejected (`missing_id`, `invalid_date`, or `invalid_amount`), so a human can go back and fix the source file if needed.

---

## What it does, step by step

1. **Trigger:** Gmail Trigger polls the inbox every minute, filtered to `has:attachment filename:csv` so it only fires on relevant emails, and downloads the attachment automatically.
2. **Parse:** The CSV attachment is parsed into JSON rows (one object per row, using the header row as field names).
3. **Clean & validate (Python):** Each row is type-cast, checked for missing IDs / unparseable dates / invalid amounts, and either kept or rejected with a reason. Valid rows are deduplicated by `order_id`, keeping whichever record has the most recent `delivery_date`.
4. **Branch on outcome:**
   - If at least one row survives cleaning → it's loaded into **Supabase Postgres** (upsert) and **Google Sheets** (append) in parallel, and also converted back into a clean `.csv` file that's emailed back as a confirmation.
   - If zero rows survive → an alert email is sent instead, listing the rejected rows and reasons.

---

## The cleaning logic (Python)

This runs inside an n8n **Code node** set to native Python. It has no network access (by design, for sandboxing), so all cleaning logic is self-contained — no external calls.

```python
import json
from datetime import datetime

# Input: all incoming items
rows = [item.json for item in _input.all()]

def parse_date(value):
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(value), fmt)
        except ValueError:
            continue
    return None

# --- 1. Type casting ---
casted = []
for row in rows:
    order_id = str(row.get('order_id') or '').strip()
    customer_name = str(row.get('customer_name') or '').strip()
    delivery_date = parse_date(row.get('delivery_date'))

    amount_raw = row.get('amount')
    amount = None
    if amount_raw not in (None, ''):
        cleaned_amount = ''.join(c for c in str(amount_raw) if c.isdigit() or c in '.-')
        try:
            amount = float(cleaned_amount) if cleaned_amount not in ('', '-', '.') else None
        except ValueError:
            amount = None

    status = str(row.get('status') or '').strip().lower()

    casted.append({
        'order_id': order_id,
        'customer_name': customer_name,
        'delivery_date': delivery_date,
        'amount': amount,
        'status': status,
    })

# --- 2. Split valid vs invalid rows ---
valid = []
rejected = []
for row in casted:
    valid_id = row['order_id'] != ''
    valid_date = row['delivery_date'] is not None
    valid_amount = row['amount'] is not None
    if valid_id and valid_date and valid_amount:
        valid.append(row)
    else:
        reason = 'missing_id' if not valid_id else ('invalid_date' if not valid_date else 'invalid_amount')
        rejected_row = dict(row)
        rejected_row['delivery_date'] = row['delivery_date'].strftime('%Y-%m-%d') if row['delivery_date'] else None
        rejected_row['reason'] = reason
        rejected.append(rejected_row)

# --- 3. Deduplicate valid rows (by order_id, keep latest delivery_date) ---
dedup_map = {}
for row in valid:
    existing = dedup_map.get(row['order_id'])
    if not existing or row['delivery_date'] > existing['delivery_date']:
        dedup_map[row['order_id']] = row

deduped = list(dedup_map.values())

# --- 4. Add pipeline metadata ---
with_meta = []
for row in deduped:
    new_row = dict(row)
    new_row['delivery_date'] = row['delivery_date'].strftime('%Y-%m-%d')
    new_row['ingested_at'] = datetime.utcnow().isoformat()
    new_row['_rejected_count'] = len(rejected)
    with_meta.append(new_row)

if with_meta:
    with_meta[0]['_rejected_rows'] = json.dumps(rejected)
    return [{'json': row} for row in with_meta]
else:
    return [{'json': {'_rejected_count': len(rejected), '_rejected_rows': json.dumps(rejected), '_empty_result': True}}]
```

**What it handles:**

| Issue in source CSV | How it's handled |
|---|---|
| Duplicate `order_id` rows | Deduplicated, keeping the most recent `delivery_date` |
| Missing `order_id` or `customer_name` | Row rejected, tagged `missing_id` |
| Unparseable / missing `delivery_date` | Row rejected, tagged `invalid_date` (tries 4 common date formats first) |
| Currency symbols in `amount` (e.g. `$99.99`) | Stripped and parsed as a float |
| Missing or non-numeric `amount` | Row rejected, tagged `invalid_amount` |
| Inconsistent casing in `status` (`Delivered`, `DELIVERED`) | Normalized to lowercase |

---

## Database: Supabase (Postgres)

[Supabase](https://supabase.com) is used as the managed Postgres backend — free tier, hosted, and gives a SQL editor + table browser out of the box, so no local database setup is required.

**Table schema** (run this once in the Supabase SQL Editor to set up storage for cleaned records):

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

`order_id` is the primary key, which is what makes the **upsert** operation in n8n idempotent — re-running the pipeline on the same file (or receiving the same order twice) updates the existing row instead of creating a duplicate.

**How it's connected:** n8n's built-in Postgres node is pointed at the Supabase project's connection credentials (host, database, user, password — found under Supabase → Project Settings → Database). The `Load to Postgres` node in the workflow runs an `UPSERT` with `order_id` as the matching column.

> **Note on security:** this table currently has Row Level Security (RLS) disabled for simplicity, since it's only written to by the n8n backend using project credentials, not exposed to a public frontend. If you fork this for a project with a public-facing client, enable RLS and add a policy scoped to a service-role key.

---

## Tech stack

| Layer | Tool |
|---|---|
| Orchestration | [n8n](https://n8n.io) (workflow automation) |
| Trigger | Gmail (polling via Gmail API) |
| Transformation | Python (n8n Code node, native sandbox) |
| Database | Supabase (managed Postgres) |
| Secondary sink | Google Sheets |
| Notifications | Gmail |

---

## Repo contents

```
/workflow/
  csv-cleaning-pipeline.json     ← importable n8n workflow
/schema/
  deliveries_clean_schema.sql    ← Supabase/Postgres table schema
/sample-data/
  messy_sample.csv               ← test file with duplicates, nulls, bad dates, currency symbols
README.md
```

---

## Setup — how to run this yourself

1. **Import the workflow** into your n8n instance: Workflows → Import from File → `csv-cleaning-pipeline.json`.
2. **Create the Supabase project** (or use an existing one) and run `deliveries_clean_schema.sql` in the SQL Editor to create the `deliveries_clean` table.
3. **Connect credentials** in n8n:
   - `Load to Postgres` → your Supabase Postgres credentials (host, db, user, password from Supabase → Project Settings → Database)
   - `Load to Google Sheets` → a Google account with a target spreadsheet (tab named `Cleaned Data`, matching the node)
   - `Gmail Trigger` / `Send a message` / `Send a message1` → a Gmail account (ideally a dedicated inbox, not your personal one)
4. **Update the recipient email** on both Gmail send nodes to your own address.
5. **Test it:** send yourself an email with `messy_sample.csv` attached. Within a minute, check:
   - n8n → **Executions** tab for the run log
   - Supabase → **Table Editor** → `deliveries_clean` for the loaded rows
   - Your Google Sheet's `Cleaned Data` tab
   - Your inbox for the confirmation email with the cleaned CSV attached
6. **Activate the workflow** (top-right toggle in n8n) once a test run succeeds.

---

## Customizing this for your own data

This pipeline was built around a generic `order_id / customer_name / delivery_date / amount / status` schema (modeled on delivery/logistics data), but it's meant to be adapted:

- **Different columns?** Update the field names in the `Clean & Validate Data` Python code, the `deliveries_clean_schema.sql` table definition, and the column mappings in `Load to Postgres` / `Load to Google Sheets` / `Prepare CSV Columns`.
- **Different validation rules?** The `valid_id` / `valid_date` / `valid_amount` checks in step 2 of the Python code are independent — add, remove, or loosen any of them.
- **Different dedup key?** Change `order_id` throughout (the Postgres `matchingColumns`, the Python `dedup_map` key, and the table's `PRIMARY KEY`).
- **Different trigger?** Swap the Gmail Trigger for a **Local File Trigger** (watch a folder) or a **Schedule Trigger** + HTTP/API pull, depending on how your data actually arrives.
- **Skip Google Sheets or Postgres entirely?** Each load branch is independent — just delete the node and its connection if you only need one destination.

---

## Possible extensions

- Route rejected rows into their own "quarantine" table or sheet instead of just reporting the count, so nothing is silently lost.
- Add a Slack notification branch alongside (or instead of) email.
- Add a scheduled daily summary of how many rows were processed/rejected over the past week.

---

## License

MIT — feel free to fork, adapt, and reuse.
