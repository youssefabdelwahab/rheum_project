import sys
import os
from dotenv import load_dotenv
import asyncio
import requests

cwd = os.getcwd()
parent_folder = os.path.abspath(os.path.join(cwd, ".."))
if parent_folder not in sys.path:
    sys.path.append(parent_folder)


from modules.pdf_resolver_v2 import PDFResolver
load_dotenv()

async def extract_text_with_pdf_resolver(doi: str, paper_id:str, selector_timeout:int) -> str: 
    """
        Resolve *doi* → PDF → text using the new async PDFResolver.

    • Tries all resolver logic (Springer, OUP, Wiley, Playwright fallback…)
    • Downloads the PDF URL it finds
    • Extracts text with your existing `pdf_parser.extract_text_from_pdf_url`
    • Returns *None* if nothing could be extracted
    """
    async with PDFResolver(selector_timeout= selector_timeout) as resolver: 
        try:
            pdf = await resolver.get_pdf(doi=doi, paper_id=paper_id)
            return pdf
        except resolver.MissingIdentifier as exc:
            return {"Missing Doi Error": exc.error_message}
        except resolver.CantDownload as exc: 
            #print 
            return {"url":exc.landing}



