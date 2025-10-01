# 📚 OpenAlex Pipeline

## 📝 What's this?

Python tool to:

* 🔎 Query OpenAlex for **ExampleOrg** researchers
* 📄 Download + scan PDFs for affiliation mentions
* 🤖 (Optional) Summarize w/ AI

## ⚡ Setup

```bash
# See setup.sh
python3.9 -m venv .venv  
source .venv/bin/activate  
pip install -r requirements.txt
```

Core deps:

* 🌐 `requests` (API calls)
* 📄 `pdfplumber`, `PyPDF2` (PDF text)
* 🔍 `pdf2image`, `pytesseract` (OCR, optional)
* 🤖 `transformers`, `torch` (AI summary, optional)

## ▶️ Run it

```bash
source .venv/bin/activate  

# Defaults: 3 authors, 2 pubs
python main.py --email you@email  

# Custom params
python main.py --email you@email --authors 5 --pubs 3
```

## ⚙️ Env Vars

* `EMAIL` → OpenAlex polite pool
* `ENABLE_OCR=true` → OCR mode
* `ENABLE_SUMMARIZATION=true` → AI summaries
* `NUM_AUTHORS`, `NUM_PUBS` → defaults
* `OUTPUT_DIR` → download path (default: `./output`)

### Enabling/Disabling AI Summarization

```bash
# Enable summarization (requires transformers and torch installed)
export ENABLE_SUMMARIZATION=true
python main.py --email your@email.com

# Disable summarization (default)
export ENABLE_SUMMARIZATION=false
# or simply omit the environment variable
python main.py --email your@email.com
```

## 🏗️ Core pieces

* **pipeline.py** → main flow
* **api_client.py** → OpenAlex calls
* **pdf_processor.py** → extract + OCR + summarize
* **content_validator.py** → checks for "ExampleOrg" etc
* **config.py** → env + defaults

## 🔄 Data flow

1. 📡 Grab ExampleOrg authors (by citations)
2. 📚 Get pubs (latest first)
3. 📥 Download OA PDFs
4. 📄 Extract text (plumber → PyPDF2 → OCR)
5. 🕵 Validate mentions
6. 🤖 Summarize (if enabled)
7. 📊 Output results

**JSON Export**: `orjson` for efficient JSON serialization (fallback to standard `json`)

<br>
