
import pytesseract
import pdfplumber
from pdf2image import convert_from_bytes
import io
import asyncio
import aiohttp


def extract_text_with_ocr(pdf_bytes):
    images = convert_from_bytes(pdf_bytes)
    full_text = []
    for i, img in enumerate(images):
        text = pytesseract.image_to_string(img)
        if text:
            full_text.append(text)
    return "\n".join(full_text).strip() if full_text else None


def extract_pdf(pdf_data): 
    extracted_text = []

    try:
        with pdfplumber.open(io.BytesIO(pdf_data)) as pdf:
                for pdf_page in pdf.pages:
                    page_text = pdf_page.extract_text()
                    if page_text:
                        extracted_text.append(page_text)
    except Exception as e:
        print(f"pdfplumber failed: {e}")

    if not extracted_text:
        print("Falling back to OCR...")
        ocr_text = extract_text_with_ocr(pdf_data)
        return ocr_text
    
    
async def download_pdf(url: str) -> bytes:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200 and response.content_type == 'application/pdf':
                return await response.read()
            else:
                raise Exception(f"Failed to download PDF: {response.status} {response.content_type}")

async def extract_text_from_pdf_url(pdf_url):
    try:
        pdf_data = await download_pdf(pdf_url)
        text = extract_pdf(pdf_data)
        return text
    except Exception as e:
        print(f"Error extracting PDF text from URL: {e}")
        return None