# EDA — Yuanta Brokerage Data

Exploratory Data Analysis of the raw input files provided in the assessment.
Notebook: [`eda.ipynb`](eda.ipynb)

---

## Files Analysed

| File | Rows | Description |
|------|------|-------------|
| `yuanta-data-engineer-assessment-test/input/clients.csv` | 15 | Client master / KYC data |
| `yuanta-data-engineer-assessment-test/input/instruments.csv` | 20 | Instrument reference data |
| `yuanta-data-engineer-assessment-test/input/trades_2026-03-09.csv` | 48 | Daily trade events |

---

## How to Run

```bash
# Install dependencies (if not already available)
pip install pandas numpy jupyter

# Open the notebook
jupyter notebook eda.ipynb
# or
jupyter lab eda.ipynb
```

Run all cells top-to-bottom (`Kernel → Restart & Run All`).

---

## Anomalies Found

### `clients.csv`

| # | Client | Issue |
|---|--------|-------|
| 1 | C005 (Echo Fund) | Missing `country` — load as NULL |
| 2 | C003, C008, C013 | `kyc_status = PENDING` — trades flagged |
| 3 | C004, C010 | `kyc_status = REJECTED` — trades flagged |

### `instruments.csv`

No structural anomalies. All 20 records are clean.

### `trades_2026-03-09.csv`

| # | Trade(s) | Anomaly Type | Detail |
|---|----------|--------------|--------|
| 1 | T0002 (×2) | **Exact duplicate** | Same data, different `trade_time` — drop earlier row |
| 2 | T0034 (×2) | **Late update / amendment** | Same ID, different `trade_time` & `price` — keep latest |
| 3 | T0003 | **Invalid FK — client** | `client_id = C999` not in clients |
| 4 | T0025 | **Invalid FK — instrument** | `instrument_id = I999` not in instruments |
| 5 | T0009 | **Whitespace + lowercase** | `client_id = " C001 "`, `side = " buy "` |
| 6 | T0006 | **Invalid side** | `side = "HOLD"` — only BUY/SELL valid |
| 7 | T0007 | **Negative price** | `price = -10` |
| 8 | T0005, T0026 | **Zero quantity** | `quantity = 0` |
| 9 | T0030 | **Missing price** | Cannot compute trade notional |
| 10 | T0008 | **Missing fees** | NULL fees on CANCELLED trade |
| 11 | T0004, T0032 | **Price as comma-string** | `"1,950"`, `"3,100.00"` — needs numeric parse |
| 12 | T0012 | **Quantity as comma-string** | `"1,000"` — needs numeric parse |

---

## Cleaning Rules Derived

| Issue | Rule |
|-------|------|
| String whitespace in `client_id`, `side` | `TRIM()` + `UPPER()` before any comparison |
| Comma-separated numerics | `str.replace(",", "")` then `cast(FLOAT)` / `cast(INT)` |
| Exact duplicate `trade_id` | Deduplicate: keep row with the latest `trade_time` |
| Amended `trade_id` (T0034) | Same dedup logic — latest `trade_time` wins |
| Invalid FK (`C999`, `I999`) | Quarantine to `trades_rejected` table with reason |
| Invalid `side` (`HOLD`) | Quarantine |
| Negative `price` | Quarantine |
| Zero `quantity` | Quarantine |
| Missing `price` | Quarantine |
| Missing `fees` on CANCELLED | Default to `0.0` |
| Non-APPROVED KYC clients | Load into main table with `kyc_status` column; flag in reporting |
| Missing `country` (C005) | Load as NULL; enrich later |

---

## Trade Row Counts After Cleaning (estimated)

| Category | Count |
|----------|-------|
| Raw rows | 48 |
| Drop — exact duplicate | −1 |
| Drop — amended (earlier version) | −1 |
| Quarantine — invalid FK / bad values | −6 (T0003, T0006, T0007, T0025, T0026, T0030) |
| **Expected clean rows** | **~40** |
