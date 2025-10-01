"""
PDF processing module for downloading and extracting text from PDFs.
"""

from typing import Optional

import requests

# PDF processing libraries
try:
    import pdfplumber
    import PyPDF2

    PDF_LIBS_AVAILABLE = True
except ImportError:
    PDF_LIBS_AVAILABLE = False
    print("Install PDF libraries: pip install PyPDF2 pdfplumber")

# OCR libraries (optional)
try:
    import pytesseract
    from pdf2image import convert_from_path

    OCR_AVAILABLE = True
except ImportError:
    OCR_AVAILABLE = False
    print("OCR not available. Install: pip install pdf2image pytesseract pillow")

# Language model libraries (optional)
try:
    from transformers import pipeline

    LLM_AVAILABLE = True
except ImportError:
    LLM_AVAILABLE = False
    print("Transformers not available. Install: pip install transformers torch")


class PDFProcessor:
    """Handles PDF downloading and text extraction"""

    def __init__(self):
        self.summarizer = None
        if LLM_AVAILABLE:
            print("Loading summarization model (this may take a moment)...")
            self.summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

    def download_pdf(self, url: str, output_path: str) -> bool:
        """Download PDF from URL"""
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()

            with open(output_path, "wb") as f:
                f.writelines(response.iter_content(chunk_size=8192))

            return True
        except Exception as e:
            print(f"  Error downloading PDF: {e}")
            return False

    def extract_text_pypdf2(self, pdf_path: str) -> str:
        """Extract text using PyPDF2"""
        if not PDF_LIBS_AVAILABLE:
            return ""

        text = ""
        try:
            with open(pdf_path, "rb") as f:
                reader = PyPDF2.PdfReader(f)
                for page in reader.pages:
                    text += page.extract_text() + "\n"
        except Exception as e:
            print(f"  PyPDF2 extraction error: {e}")

        return text.strip()

    def extract_text_pdfplumber(self, pdf_path: str) -> str:
        """Extract text using pdfplumber (more accurate)"""
        if not PDF_LIBS_AVAILABLE:
            return ""

        text = ""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
        except Exception as e:
            print(f"  pdfplumber extraction error: {e}")

        return text.strip()

    def extract_text_ocr(self, pdf_path: str) -> str:
        """Extract text using OCR (for image-based PDFs)"""
        if not OCR_AVAILABLE:
            return ""

        text = ""
        try:
            images = convert_from_path(pdf_path)
            for i, image in enumerate(images):
                page_text = pytesseract.image_to_string(image)
                text += f"--- Page {i+1} ---\n{page_text}\n"
        except Exception as e:
            print(f"  OCR extraction error: {e}")

        return text.strip()

    def extract_text(self, pdf_path: str, use_ocr: bool = False) -> str:
        """
        Extract text from PDF using multiple methods.

        Args:
            pdf_path: Path to PDF file
            use_ocr: Whether to use OCR for image-based PDFs
        """
        # Try pdfplumber first (most accurate)
        text = self.extract_text_pdfplumber(pdf_path)

        # Fallback to PyPDF2 if pdfplumber fails
        if len(text) < 100:
            text = self.extract_text_pypdf2(pdf_path)

        # Use OCR if text extraction yielded little content
        if use_ocr and len(text) < 100:
            text = self.extract_text_ocr(pdf_path)

        return text

    def summarize_text(self, text: str, max_length: int = 150) -> Optional[str]:
        """Summarize text using language model"""
        if not self.summarizer or len(text) < 200:
            return None

        try:
            # Truncate to avoid token limits
            text_chunk = text[:1024]
            summary = self.summarizer(
                text_chunk, max_length=max_length, min_length=30, do_sample=False
            )
            return summary[0]["summary_text"]
        except Exception as e:
            print(f"  Summarization error: {e}")
            return None
