# Automated-data-cleaning-pipeline

An automated pipeline for processing and visualizing training satisfaction
survey data. Supports two survey types — **MODULE** and **TRAQOM** — with
end-to-end data cleaning, score conversion, MySQL ingestion, and BI dashboard
generation.

---

## 📁 Project Structure

```
survey visualization/
├── Main function/
│   ├── main.py                      # Core pipeline: data processing & DB write
│   ├── query.sql                    # DB maintenance queries (dedup, clean, patch)
│   └── survey_config_template.xlsx  # Config file (question mapping, score scales)
├── raw_data/                        # Drop raw survey Excel files (.xlsx) here
└── Visualization Result/
    ├── BI-Dashboard.png             # Power BI dashboard screenshot
    └── Survey Comments.png          # Open-ended comments report screenshot
```

---

## ⚙️ Requirements

- Python 3.8+
- MySQL database (local or remote)
- Python dependencies:

```bash
pip install pandas sqlalchemy pymysql openpyxl
```

---

## 🗄️ Database Setup

Update the connection string in `main.py` to match your database:

```python
engine = create_engine("mysql+pymysql://<user>:<password>@<host>/<database>")
```

Default config:
```
mysql+pymysql://root:root@localhost/survey_db
```

The following tables must exist in your database before running:

| Table | Description |
|-------|-------------|
| `course_metadata` | Course info (name, trainer, dates, run ID) |
| `survey_response` | One record per survey submission |
| `survey_score` | Per-question scores in long format |
| `survey_comment` | Open-ended text responses |

---

## 🚀 Usage

**1. Add raw data**

Place raw survey Excel files (`.xlsx`) into the `raw_data/` folder.
Each file must contain:
- `Raw Review` sheet → MODULE survey data
- `TRAQOM Raw Data` sheet → TRAQOM survey data

**2. Check the config**

Review `survey_config_template.xlsx`, which contains three sheets:
- `question_master` — master list of question codes
- `column_mapping` — maps raw column names to question codes per survey type
- `scale_mapping` — converts text answer options to numeric scores

**3. Run the pipeline**

```bash
python main.py
```

The script processes all `.xlsx` files in `raw_data/` and:
- Matches raw columns to question codes
- Converts text answers to numeric scores
- Fills MODULE metadata from TRAQOM where missing
- Writes to `course_metadata`, `survey_response`, `survey_score`, and `survey_comment`
- Outputs `survey_wide_table.xlsx` and `survey_long_table.xlsx` locally

---

## 🔄 Pipeline Overview

```
raw_data/*.xlsx
      │
      ▼
 load_config()          ← survey_config_template.xlsx
      │
      ▼
 load_raw_data()        ← MODULE + TRAQOM sheets
      │
      ▼
 match_columns()        ← map raw headers → question codes
      │
      ▼
 convert_scores()       ← text answers → numeric scores
      │
      ▼
 build_wide_table()     ← add course metadata columns
      │
      ▼
 fill_module_metadata() ← fill MODULE nulls from TRAQOM
      │
      ▼
 build_long_table()     ← melt wide → long; extract comments
      │
      ▼
 MySQL DB write         ← course_metadata / survey_response
                           survey_score / survey_comment
```

---

## 🛠️ Database Maintenance

`query.sql` contains utility queries for manual DB cleanup, including:
- Remove records with missing trainer or date info
- Detect and delete duplicate `course_run` entries
- Patch specific records (e.g. fix trainer name or course dates)

Run these in MySQL Workbench or any SQL client as needed.

---

## 📝 Notes

- Comment columns (`M16`, `M17`, `M18`, `T11`, `T12`, `T13`) are extracted
  separately into `survey_comment` and excluded from numeric scoring.
- The pipeline appends to existing Excel output files rather than overwriting.
- `response_id` is retrieved from the DB after insert and matched back to the
  wide table by insertion order.
