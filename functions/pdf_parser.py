"""
pdf_parser.py
=============

Asynchronous helpers for:

1. Downloading a PDF while bypassing common publisher blocks.
2. Extracting embedded text with pdfplumber.
3. Falling back to page-level OCR (Tesseract) when the PDF is scanned.

External deps:
    aiohttp, pdfplumber, pdf2image, pytesseract
Environment vars (optional):
    WILEY_TDM_KEY   – API key for Wiley text- and data-mining endpoint.
"""

from __future__ import annotations

import io
import os
from typing import Optional

import aiohttp
import pdfplumber
import pytesseract
from pdf2image import convert_from_bytes

# ──────────────────────────────────────────────────────────────────────────────
# 1. Default HTTP headers (safe to send to any site)
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/pdf,application/octet-stream;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


# ──────────────────────────────────────────────────────────────────────────────
# 2. Downloader
# ──────────────────────────────────────────────────────────────────────────────
async def download_pdf(
    url: str,
    *,
    session: Optional[aiohttp.ClientSession] = None,
) -> bytes:
    """
    Download *url* and return the raw PDF bytes.

    A desktop-browser UA, broad Accept header, and (for a few hosts) a synthetic
    **Referer** header are added automatically to bypass CDN blocks.

    Parameters
    ----------
    url : str
        Direct (or quasi-direct) link to a PDF.
    session : aiohttp.ClientSession, optional
        Re-use an existing session for efficiency; one will be created and
        closed automatically if not supplied.

    Returns
    -------
    bytes
        Complete PDF file in memory.

    Raises
    ------
    RuntimeError
        If the server does not return a PDF or returns a non-200 status.
    """
    headers = DEFAULT_HEADERS.copy()

    # ── Host-specific tweaks ────────────────────────────────────────────────
    if "academic.oup.com" in url and "/article-pdf/" in url:
        # Oxford Academic requires a referer that points to the article HTML.
        headers["Referer"] = url.split("/article-pdf/")[0]

    elif "tandfonline.com" in url and "/doi/pdf/" in url:
        # Taylor & Francis blocks direct /pdf/ unless referer matches /full/.
        doi = url.split("/doi/pdf/")[1]
        headers["Referer"] = f"https://tandfonline.com/doi/full/{doi}"

    elif "api.wiley.com" in url:
        # Wiley TDM endpoint needs an API key + explicit Accept header.
        api_key = os.getenv("WILEY_TDM_KEY")
        if api_key:
            headers["X-Api-Key"] = api_key
            headers["Accept"] = "application/pdf"

    # ── Session management ─────────────────────────────────────────────────
    owns_session = False
    if session is None:
        session = aiohttp.ClientSession()
        owns_session = True

    try:
        async with session.get(url, headers=headers, allow_redirects=True) as resp:
            # Read first 4 bytes to verify %PDF-magic before downloading rest
            first4 = await resp.content.read(4)
            rest   = await resp.content.read()

            if resp.status == 200 and first4 == b"%PDF":
                return first4 + rest

            ct = resp.headers.get("content-type", "")
            raise RuntimeError(
                f"Not a PDF: status={resp.status} content-type={ct!r}"
            )
    finally:
        if owns_session:
            await session.close()


# ──────────────────────────────────────────────────────────────────────────────
# 3. Extraction helpers
# ──────────────────────────────────────────────────────────────────────────────
def extract_text_with_ocr(pdf_bytes: bytes) -> Optional[str]:
    """
    Render each page to an image and OCR it with Tesseract.

    Slow but guarantees some text even for fully scanned / image-based PDFs.
    """
    images = convert_from_bytes(pdf_bytes)
    chunks: list[str] = [
        pytesseract.image_to_string(img) for img in images if img is not None
    ]
    text = "\n".join(t.strip() for t in chunks if t.strip())
    return text or None


def extract_pdf(pdf_bytes: bytes) -> Optional[str]:
    """
    Extract text from *pdf_bytes* using pdfplumber; fallback to OCR as needed.
    """
    all_text = []

    try:

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page_num, page in enumerate(pdf.pages, start = 1):
                words = page.extract_words()
                if not words:
                    continue

                x_coords = [w['x0'] for w in words] + [w['x1'] for w in words]
                middle_x = (min(x_coords) + max(x_coords))/2

                left_column = [w for w in words if w['x0'] < middle_x]
                right_column = [w for w in words if w['x0'] >= middle_x]

                left_column = sorted(left_column, key =lambda w: w['top'])
                right_column = sorted(right_column, key = lambda w: w['top'])

                left_text = "  ".join(w['text'] for w in left_column)
                right_text = "  ".join(w['text'] for w in right_column)

                all_text.append(left_text)
                all_text.append(right_text)

                print(f"Page {page_num} processed")

        full_text = "\n\n".join(all_text)
        
        print(f"Extraction complete.")
        return full_text


    except Exception as exc:  # noqa: BLE001
        print(f"[pdf_parser] pdfplumber failed: {exc}")


    print("[pdf_parser] Falling back to OCR …")
    return extract_text_with_ocr(pdf_bytes)


# ──────────────────────────────────────────────────────────────────────────────
# 4. Top-level convenience wrapper
# ──────────────────────────────────────────────────────────────────────────────
async def extract_text_from_pdf_url(pdf_url: str) -> Optional[str]:
    """
    Download *pdf_url* and return extracted text (or *None* on failure).

    Combines :pyfunc:`download_pdf` and :pyfunc:`extract_pdf`.
    """
    try:
        raw = await download_pdf(pdf_url)
        return extract_pdf(raw)
    except Exception as exc:  # noqa: BLE001
        print(f"[pdf_parser] Error extracting text: {exc}")
        return None
