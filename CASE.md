# Junior Data Engineer – Take‑Home Exercise (Brokerage Data)

**Recommended timebox:** 3-4 hours
**Submission:** a GitHub repository link

## Background
You’re joining a team supporting a brokerage firm’s internal reporting and analytics. The team receives daily CSV drops containing trade events and reference/master data. Source files are not always consistent and may include duplicates, missing fields, formatting issues, and other common data problems.

## What You’re Given
This repository contains example input files under:

- `data/input/clients.csv`
- `data/input/instruments.csv`
- `data/input/trades_2026-03-09.csv`

You may assume future trade files follow a similar pattern and arrive periodically.

## Expected Outcome
By running the project (via Docker), a reviewer should be able to see **clean, queryable brokerage datasets** in a database, suitable for analytics and reporting.

## Constraints
- The solution must run locally on a reviewer’s machine.
- It should be possible to run the solution both **on-demand** and **periodically** without manual intervention.
- The system should behave sensibly under common failures (for example, dependency not ready, temporary connection failure).

## Data Characteristics to Consider
The provided data intentionally includes patterns commonly found in real brokerage feeds, such as:
- inconsistent casing and whitespace
- numeric values formatted as strings (including separators like commas)
- missing values
- duplicates and late updates
- invalid references between trade events and reference data

## Deliverables
Your GitHub repository should include:
1. Source code in Github repository
2. A `README.md` that explains:
   - how to start everything
   - how to trigger a run (if applicable)
   - how to confirm results (example SQL queries are fine)
   - any design decisions / trade-offs

## Evaluation (What We Look For)
We’ll review:
- correctness and completeness of the final datasets
- robustness (repeatable runs, safe re-runs, failure handling)
- data reliability practices (cleansing, validation, basic governance)
- clarity of repository structure and documentation
- containerized reproducibility (a reviewer can run it quickly)

## Notes
- Choose any language and tooling you’re comfortable with.
- Keep the solution practical and focused; explain assumptions in the README.