import sys
import os
from dotenv import load_dotenv
from typing import Optional , Union, Tuple, Dict


cwd = os.getcwd()
parent_folder = os.path.abspath(os.path.join(cwd, ".."))
if parent_folder not in sys.path:
    sys.path.append(parent_folder)


from modules.pdf_resolver_v2 import PDFResolver
load_dotenv()

async def extract_text_with_pdf_resolver(
    doi: Optional[str],
    paper_id: str,
    cross_ref_paper_link: Optional[str],
    selector_timeout: int,
) -> Union[Tuple[bytes, str], Dict[str, str], None]:
    """
    Resolve DOI / landing → (pdf_bytes, pdf_url) using the async PDFResolver.

    Returns
    -------
    - (pdf_bytes, pdf_url) on success
    - {"Missing Doi Error": "..."} on MissingIdentifier
    - {"url": "..."} on CantDownload
    - None in unexpected failure (if you later choose to)
    """
    async with PDFResolver(selector_timeout=selector_timeout) as resolver:
        try:
            pdf_bytes, pdf_url = await resolver.get_pdf(
                doi=doi,
                paper_id=paper_id,
                cross_ref_paper_link=cross_ref_paper_link,
            )
            return pdf_bytes, pdf_url

        except resolver.MissingIdentifier as exc:
            return {"Missing Doi Error": str(exc)}

        except resolver.CantDownload as exc:
            return {"url": exc.landing}



