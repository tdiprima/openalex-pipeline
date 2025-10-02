# 📚 OpenAlex Pipeline

## 📝 Overview

Python tool to:

* 🔎 Query OpenAlex API for **ExampleOrg** researchers
* 📄 Download + scan PDFs for affiliations
* 🤖 (Optional) Summarize w/ AI
* 📊 Export structured results (JSONL)

## ⚡ Setup

```bash
python3.9 -m venv .venv  
source .venv/bin/activate  
pip install -r requirements.txt
```

### Core Deps

* 🌐 `requests` → API calls
* ⚡ `orjson` → fast JSON export
* 📄 `pdfplumber`, `PyPDF2` → text extraction
* 🖼️ `pdf2image`, `pytesseract`, `pillow` → OCR (optional)
* 🤖 `transformers`, `torch` → AI summaries (optional)

```sh
# MacOS
brew install tesseract

# Rocky Linux
sudo dnf install -y tesseract tesseract-langpack-eng
```

## ▶️ Run It

```bash
source .venv/bin/activate  

# Default (3 authors, 2 pubs)
python main.py --email you@email  

# Custom run
python main.py --email you@email --authors 5 --pubs 3


# Skip PDF downloads (metadata only - much faster for large datasets)
python main.py --email your@email.com --authors 40000 --pubs 10 --no-pdf
```

## ⚙️ Env Vars

* `EMAIL` → OpenAlex polite pool (faster)
* `ENABLE_OCR=true` → OCR mode
* `ENABLE_SUMMARIZATION=true` → AI summaries
* `OUTPUT_DIR` → output folder (default: `./output`)
* `NUM_AUTHORS`, `NUM_PUBS`, `VERBOSE` → tuning
* `ENABLE_PDF_DOWNLOAD`: Set to "false" to skip PDF downloads

## 📂 Output

After each run → timestamped dir:

* 📜 `authors.jsonl` → 1 author per line
* 📜 `publications.jsonl` → 1 pub per line
* 📊 `run_stats.json` → stats + metadata
* 📄 `paper_*.pdf` → downloaded PDFs (if enabled)

Quick queries:

```bash
jq 'select(.cited_by_count > 1000)' authors.jsonl   # top authors
jq 'select(.processing.ExampleOrg_validation.found == true)' publications.jsonl
```

## 🏗️ Core Files

* `pipeline.py` → orchestrator
* `api_client.py` → OpenAlex calls
* `pdf_processor.py` → text + OCR + summaries
* `content_validator.py` → checks ExampleOrg mentions
* `data_exporter_streaming.py` → JSONL export

## 🔄 Data Flow

1. 📡 Get authors (by citations)
2. 📚 Fetch pubs (latest first)
3. 📥 Download PDFs
4. 📄 Extract text (plumber → PyPDF2 → OCR)
5. 🕵 Validate SB mentions
6. 🤖 Summarize (optional)
7. 📊 Export JSONL + stats

---

To get truly "all" authors:

```sh
# Very large dataset (be careful!)
python main.py --email your@email.com --authors 50000 --pubs 500 --optimized --parallel
```

Safe recommendations:

```sh
# Test run
python main.py --email your@email.com --authors 100 --pubs 10 --optimized --parallel

# Medium scale
python main.py --email your@email.com --authors 1000 --pubs 25 --optimized --parallel

# Large scale (might take hours)
python main.py --email your@email.com --authors 10000 --pubs 50 --optimized --parallel
```

The OpenAlex API doesn't have a true "get all authors" option - you specify a max number. For ExampleOrg, there are likely 5,000-15,000 total authors in their database.

<br>
