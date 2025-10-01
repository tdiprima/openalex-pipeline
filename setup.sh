#!/bin/bash

# Check your Python version
python --version

# Create virtual environment with specific Python version
python3.9 -m venv .venv  # or python3.10

# Activate it
source .venv/bin/activate

# Install core requirements first
pip install requests PyPDF2 pdfplumber

# Then install torch (if needed) - use the right command for your system
# CPU only (smaller, easier):
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu

# Or with CUDA 11.8 (for GPU):
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

# Then install transformers if using summarization
pip install transformers

# Finally, OCR libraries if needed
pip install pdf2image pytesseract pillow
