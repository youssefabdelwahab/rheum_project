"""
pdf_resolver.py
================

Asynchronous DOI / landing‑page → PDF resolver supporting many major
publishers (Springer, OUP, Wiley, F1000Research, Hindawi, etc.).

The class exposes an **async context‑manager** interface so HTTP resources
are cleaned up automatically:

```python
async with PDFResolver() as resolver:
    pdf_url = await resolver.get_pdf(doi, landing_url, paper_id)
```

If no PDF can be located it raises :class:`CantDownload`; if neither a DOI
nor a landing URL is supplied it raises :class:`MissingIdentifier`.
"""






import re 
from bs4 import BeautifulSoup
from typing import Optional, List
import urllib.parse
import requests
from dotenv import load_dotenv
from playwright.async_api import async_playwright , Playwright , Browser , TimeoutError as PWTimeoutError, Page
import asyncio
import os
from typing import Optional
from urllib.parse import urljoin
import httpx
from httpx import Timeout, AsyncClient
import random
from playwright_stealth import stealth_async
import logging
import random
from playwright.async_api import BrowserContext 
from pathlib import Path
from datetime import datetime

load_dotenv()

env_path = "/work/robust_ai_lab/shared/env_vars/rheum_project/env_vars.sh"  # export SCRIPT_ENV_FILE=/full/path/to/env_vars.sh
if not env_path:
    raise RuntimeError("SCRIPT_ENV_FILE is not set")

env_path = str(Path(env_path).expanduser())
ok = load_dotenv(dotenv_path=env_path, override=False)
if not ok:
    raise FileNotFoundError(f"Could not load env file at {env_path}")
print("Loaded Env File")

repo_root = os.getenv("ROOT_DIR")
shared_folder = os.path.join(repo_root, "shared")
pmcid_tool_name = os.getenv("PMCID_TOOL")
pmcid_email = os.getenv("PMCID_EMAIL")
print("Loaded Environment Variables Successfully")


date_str = datetime.now().strftime("%Y-%m-%d") 
log_folder = os.path.join(repo_root, shared_folder, "logs/paper_extraction")
os.makedirs(log_folder, exist_ok=True)
log_file_path = os.path.join(log_folder, f"extraction_{date_str}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()  
    ]
)
logging.getLogger("pdfminer").setLevel(logging.ERROR)




def drop_www(host:str) -> str: 
    return host[4:] if host.startswith("www.") else host

login_selectors = {
            "user":   'input[name="user"], input[name="username"], #username',
            "pass":   'input[name="pass"], input[name="password"], #password',
            "submit": 'input[type="submit"], button[type="submit"]'

        }
JOURNAL_PDF_SELECTORS = {
        "bmj.com": 'a[title="Download this article as a PDF"]',
        "thelancet.com": 'a.download-pdf-link',
        "nejm.org": 'a[href*="pdf"]',
        "sciencedirect.com": 'a.pdf-download',
        "jamanetwork.com": 'a[href*="/pdf"]',
        "nature.com": 'a[data-track-action="download pdf"]',
        "springer.com": 'a[href$=".pdf"]',
        "oup.com": 'a[href$=".pdf"]',
        "frontiersin.org": 'a[href*="/pdf"]',
        "tandfonline.com": 'a[href*="/pdf"]',
        "sagepub.com": 'a.article-tools-download',
        "karger.com": 'a[href$=".pdf"]',
        # Add more as needed...
    }
class PDFResolver:
    playwright: Playwright | None = None
    browser : Browser| None = None
    lock: asyncio.Lock = asyncio.Lock()
    def __init__(self, *, selector_timeout: int  , client: Optional[AsyncClient] = None):
        self._client = client
        self.headers = {
    "User-Agent": random.choice([
        # realistic desktop browsers
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36",

        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.4 Safari/605.1.15",

        "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) "
        "Gecko/20100101 Firefox/125.0",
    ]),
    "Accept":
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "application/pdf;q=0.8,*/*;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
}
        self.timeout = selector_timeout
        self.user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/125.0",
]
        # self.ezproxy_user = os.getenv("uni_username")
        # self.ezproxy_pass = os.getenv("uni_password")
        # self.ezproxy_state_path = os.getenv("ezproxy_state_path")
        self.ezbase = "http://ezproxy.lib.ucalgary.ca/login?url="
        self.selector_timeout = selector_timeout
        self._SPRINGER_HOST = "link.springer.com"
        self._OUP_HOST_RE = re.compile(r"^https?://academic\.oup\.com/")
        self._F1000_HOST_RE = re.compile(r"^https?://(?:f1000research|wellcomeopenresearch|gatesopenresearch)\.org/")
        self._HINDAWI_DOWNLOAD_RE = re.compile(r"^https?://downloads\.hindawi\.com/")
        self._HINDAWI_LANDING_RE = re.compile(r"^https?://(?:www\.)?hindawi\.com/")
        self._ANCHOR_HINT_RE = re.compile(r"""(?x)
        (
            pdf
            | full[\s_-]*text
            | full[\s_-]*article
            | view[\s_-]*article
            | view[\s_-]*pdf
            | read[\s_-]*(this[\s_-]*)?article
            | article[\s_-]*as[\s_-]*pdf
            | open[\s_-]*access
            | download
            | dl[\s_-]*(pdf|article)?
            | link[\s_-]*to[\s_-]*pdf
            | show[\s_-]*pdf
            | get[\s_-]*pdf
            | pdf[\s_-]*download
            | primary[\s_-]*document
            | main[\s_-]*article
            | publication[\s_-]*file
            | document[\s_-]*view
            | article[\s_-]*file
            | content[\s_-]*pdf
            | view[\s_-]*full[\s_-]*text
            | citation[\s_-]*pdf[\s_-]*url
        )
        """,
        re.I
    )
        self.wiley_token = os.getenv("wiley_api_token")
    
    class PDFResolverError(Exception): 
        """Base class for PDF resolver errors."""
        pass 
        
    class MissingIdentifier(PDFResolverError):
      def __init__(self, message: str = "Neither DOI nor landing URL was supplied."):
          super().__init__(message)
    
    class CantDownload(PDFResolverError):
        """
        Raised when the resolver failed to find a downloadable PDF.
        Carries the DOI and the final landing URL we ended up on.
        """
        def __init__(self, doi:str | None, landing:str):
            self.doi = doi
            self.landing = landing
            super().__init__(f"Could not download PDF for DOI {doi} from {landing}")

    async def __aenter__(self):
        self._client = httpx.AsyncClient(headers=self.headers, follow_redirects=True, timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._client:
            await self._client.aclose()
            self._client = None


    def _client_required(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                headers=self.headers, 
                follow_redirects=True, 
                timeout=Timeout(15.0, read=30.0), 
                http2=True)
        return self._client
    
    async def context_required(self) -> BrowserContext: 
        async with PDFResolver.lock:
            if PDFResolver.browser is None:
                PDFResolver.playwright = await async_playwright().start()
                PDFResolver.browser = await PDFResolver.playwright.chromium.launch(
                    headless=True,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--disable-web-security",
                        "--no-sandbox",
                        "--disable-gpu",
                    ],
                )

        context = await PDFResolver.browser.new_context(
            user_agent=random.choice(self.user_agents),
            locale="en-US",
            timezone_id="America/New_York",
        )
        await context.add_init_script(
            """
            Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
            Object.defineProperty(navigator,'languages',{get:()=>['en-US','en']});
            Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3]});
            """
        )
        return context
    
    def _springer_candidates(self, landing: str, doi: str) -> List[str]:
        base = f"https://{self._SPRINGER_HOST}"
        candidates = [f"{base}/content/pdf/{doi}.pdf"]
        if landing.endswith(("fulltext.html", "fulltext.htm")):
            candidates.append(landing.rsplit("/", 1)[0] + ".pdf")
        elif landing.endswith(".html"):
            candidates.append(landing[:-5] + ".pdf")
        elif "/article/" in landing and not landing.endswith(".pdf"):
            candidates.append(landing + ".pdf")
        return candidates

    async def _f1000_pdf(self, landing: str, doi: str) -> Optional[str]:
        client = self._client_required()
        article_id = doi.split("/")[-1]
        host = urllib.parse.urlparse(landing).hostname
        #unique f1000 method
        api_url = f"https://api.{host}/article/{article_id}"
        try:
            r = await client.get(api_url)
            r.raise_for_status()
            f100_url = r.json()["data"]["pdf_url"]
        except Exception:
            return None 
        #general method last chance
        try: 
            url_response = await self.try_pdf_url(f100_url)
        except Exception as e: 
            if url_response: 
                return url_response 
        return None

    async def _oup_pdf(self, landing: str) -> Optional[str]:
        client = await self._client_required()
        html = (await client.get(landing)).text
        oup_meta = self._extract_meta_pdf(html)
        #unique oup methods
        try: 
            meta_response = await self.try_pdf_http(oup_meta)
        except Exception as e: 
            if meta_response: 
                return meta_response 
        oup_anchor = self._extract_anchor_pdf_score(html, landing)
        try: 
            anchor_response = await self.try_pdf_url(oup_anchor)
        except Exception as e: 
            if anchor_response: 
                return anchor_response
        #general method last chance
        try: 
            oup_url_response = await self.try_pdf_url(landing)
        except Exception as e:
            if oup_url_response: 
                return oup_url_response
        return None

    async def _wiley_pdf(self , landing:str , doi:str) -> Optional[str]:
        client = self._client_required()
        
        #1 TDM Api
        api_url = f"https://api.wiley.com/tdm/v1/articles?{doi}/pdf"
        try: 
            url_response = await self.try_pdf_http(api_url)
        except Exception as e: 
            if url_response: 
                return url_response 
        
        #pdfdirect 
        
        pdf_url = f"https://onlinelibrary.wiley.com/doi/pdfdirect/{doi}"
        r2 = await client.get(pdf_url)
        try:
            url_response = await self.try_pdf_url(r2)
        except Exception as e: 
            if url_response: 
                return url_response
        #htmlparse 
        html = (await client.get(landing)).text
        soup = BeautifulSoup(html, "html.parser")
        btn = soup.find("a", class_="pdf-download-link", href=True)
        if btn:
            resolved_btn_link =  urllib.parse.urljoin(landing, btn["href"])
            try: 
                btn_response = await self.try_pdf_url(resolved_btn_link)
            except Exception as e: 
                if btn_response: 
                    return btn_response 
        # general method last chance
        try: 
            wiley_url_response = await self.try_pdf_url(landing)
        except Exception as e:
            if wiley_url_response: 
                return wiley_url_response
        return None
    
    async def _tand_pdf(self , landing:str , doi:str) -> Optional[str]:
        client = self._client_required()
        
        #direct pdf endpoint 
        
        pdf_endpoint = f"https://www.tandfonline.com/doi/pdf/{doi}"
        resolved_tand_link  = await client.get(pdf_endpoint)
        try: 
            url_response = await self.try_pdf_url(resolved_tand_link)
        except Exception as e: 
            if url_response: 
                return url_response 
    
        #meta tag
        html = (await client.get(landing)).text
        if (meta:= self._extract_meta_pdf(html)):
            resolved_tand_meta = urllib.parse.urljoin(landing, meta)
            try: 
                meta_response = await self.try_pdf_http(resolved_tand_meta)
            except Exception as e: 
                if meta_response: 
                    return meta_response 
        #anchor tag
        if (anchor := self._extract_anchor_pdf_score(html, landing)):
            resolved_tand_anchor = anchor 
            try: 
                anchor_response = await self.try_pdf_url(resolved_tand_anchor)
            except Exception as e: 
                if anchor_response: 
                    return anchor_response 
        # general method last chance
        try: 
            tand_url_response = await self.try_pdf_url(landing)
        except Exception as e:
            if tand_url_response: 
                return tand_url_response
        return None
            
        
    async def _sage_pdf(self, landing: str, doi: str) -> Optional[str]:
        client = self._client_required()
        
        #pdf endpoint
        
        pdf_url = f"https://journals.sagepub.com/doi/pdf/{doi}"
        r = await client.get(pdf_url)
        try: 
            url_response = await self.try_pdf_url(r)
        except Exception as e: 
            if url_response: 
                return r 

        #html parse
        html = (await client.get(landing)).text
        if (meta:= self._extract_meta_pdf(html)):
            resolved_sage_meta = urllib.parse.urljoin(landing, meta)
            try:
                meta_response = await self.try_pdf_http(resolved_sage_meta)
            except Exception as e: 
                if meta_response: 
                    return resolved_sage_meta 
        #anchor tag
        if (anchor := self._extract_anchor_pdf_score(html, landing)):
            resolved_sage_anchor = anchor
            try: 
                anchor_response = await self.try_pdf_url(resolved_sage_anchor)
            except Exception as e: 
                #log 
                if anchor_response: 
                    return anchor_response 
        # general method last chance
        try: 
            sage_url_response = await self.try_pdf_url(landing)
        except Exception as e:
            if sage_url_response: 
                return sage_url_response
        return None
        
        
    async def _karger_pdf(self, landing: str, doi: str) -> Optional[str]:
        client = self._client_required()
        
        #must get article id 
        
        html = (await client.get(landing)).text
        soup = BeautifulSoup(html, "html.parser")
        tag = soup.find("meta", attrs={"name": "dc.identifier"})
        if not tag or not tag.get("data-article-id"):
            try: 
                tag_response = await self.try_pdf_http(tag)
            except Exception as e: 
                if tag_response: 
                    return tag_response
    
        article_id = tag["data-article-id"]
        resolved_karger_link = f"https://www.karger.com/Article/Pdf/{article_id}"
        try: 
            karger_response = await self.try_pdf_url(resolved_karger_link)
        except Exception as e: 
            #log 
            if karger_response: 
                return resolved_karger_link 
        # general method last chance
        try: 
            karger_url_response = await self.try_pdf_url(landing)
        except Exception as e:
            if karger_url_response: 
                return karger_url_response
        return None
    
    
    async def try_pdf_url(self,
        url: str,
        context: BrowserContext | None = None
    ) -> str | None:
        """
        Download *url* via Playwright’s request API and extract text if it is a PDF.

        Parameters
        ----------
        context : BrowserContext
            The Playwright context that owns the current page.
        url : str
            Candidate PDF link (absolute URL).

        Returns
        -------
        str | None
            Extracted text if successful, otherwise *None*.
        """
        if context is None: 
            context =  await self.context_required()
        try:
            resp = await context.request.get(url, timeout=self.selector_timeout)
        except Exception as exc:
            print(f"[resolver]   GET failed for {url!s}: {exc}")
            return None

        # Quick validation: 200 OK + URL / MIME hint contains 'pdf'
        if resp.status != 200:
            return None

        # Prefer content-type header when present; fallback to URL check
        ctype = resp.headers.get("content-type", "").lower()
        if "pdf" not in ctype and not url.lower().endswith(".pdf"):
            return None

        # Confirm first bytes contain %PDF-magic
        raw = await resp.body()
        if not raw:
            return None

        if not raw.startswith(b"%PDF"):
            return None
        print('Successfully Found PDF URL:', url)
        return raw
        # loop = asyncio.get_running_loop()
        # text = await loop.run_in_executor(None, functools.partial(extract_pdf, raw))
        # if text:
        #     return text or None
    
    
    async def try_pdf_http(self,url:str):
        """
        Download *url* with httpx, verify it is a PDF (%PDF magic),
        then return extracted text (or None on failure).
        """
        client = self._client_required()
        try:
            resp = await client.get(url, timeout=15)
        except Exception as exc:
            print(f"[resolver] httpx GET failed: {exc}")
            return None

        if resp.status_code != 200:
            return None

        ctype = resp.headers.get("content-type", "").lower()
        if "pdf" not in ctype and not url.lower().endswith(".pdf"):
            return None

        raw = resp.content
        if not raw:
            return None

        if not raw.startswith(b"%PDF"):
            return None
        print('Successfully downloaded PDF from URL:', url)
        return raw

       
    
    def _extract_anchor_pdf_score(self, html: str, base_url: str) -> Optional[str]:
        soup = BeautifulSoup(html, "html.parser")
        join = lambda h: urllib.parse.urljoin(base_url, h)
        best = None

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            lower = href.lower()
            full_url = join(href)

            text = a.get_text(" ").lower()
            cls = " ".join(a.get("class", [])).lower()
            ident = a.get("id", "").lower()
            title = a.get("title", "").lower()
            aria = a.get("aria-label", "").lower()

            score = 0
            if lower.endswith(".pdf"):
                score += 3
            if "/article-pdf/" in lower or "/advance-article-pdf/" in lower:
                score += 2
            if "pdf" in text:
                score += 2
            if re.search(self._ANCHOR_HINT_RE, cls) or re.search(self._ANCHOR_HINT_RE, ident):
                score += 1
            if "full text" in text or "read article" in text:
                score += 1
            if "pdf" in title or "pdf" in aria:
                score += 1

            if score >= 3:  # only consider fairly confident ones
                return full_url
            elif not best and score > 0:
                best = full_url
        return best
    
    async def find_via_selector(self,domain: str, page: Page) -> str | None:
        selector = JOURNAL_PDF_SELECTORS.get(domain)
        if not selector:
            return None                # nothing to try

        try:
            await page.wait_for_selector(selector, timeout=self.selector_timeout)
        # take only the first match to avoid strict-mode failure
            href   = await page.locator(selector).first.get_attribute("href")
            if not href:
                return None

            resolved = urljoin(page.url, href)
            selector_bytes = await self.try_pdf_url(resolved)
            return selector_bytes                 # None when not a PDF / could not extract
        except PWTimeoutError:
            return None
        
    async def find_pdf_button(self,page: Page) -> str | None:
        try:
            href_btn= await page.get_by_role("link", name="PDF").get_attribute("href")
        except TimeoutError: 
            return None
        if not href_btn:
            return None                     # no PDF button found
        try:
            resolved_href = urljoin(page.url, href_btn)
            href_bytes = await self.try_pdf_url(resolved_href)
            if href_bytes: 
                return href_bytes
        except TimeoutError:
            return None
    
    async def find_via_anchor(self,page: Page) -> str | None:
        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                text = a.get_text(" ").lower()
                cls = " ".join(a.get("class", [])).lower()
                id_ = a.get("id", "").lower()
                title = a.get("title", "").lower()
                aria = a.get("aria-label", "").lower()
                
                looks_like_pdf = (
                    href.lower().endswith(".pdf") or
                    self._ANCHOR_HINT_RE.search(text) or
                    self._ANCHOR_HINT_RE.search(href) or
                    self._ANCHOR_HINT_RE.search(cls) or
                    self._ANCHOR_HINT_RE.search(id_) or
                    self._ANCHOR_HINT_RE.search(title) or
                    self._ANCHOR_HINT_RE.search(aria)
                )
                
                if not looks_like_pdf:
                    continue
                
                anchor_link = urljoin(page.url, href)
                try:
                    anchor_bytes = await self.try_pdf_url(anchor_link)
                    if anchor_bytes: 
                        return anchor_bytes
                except Exception as e:
                    continue 
                        #log
        return None
                    
    async def find_via_redirect(self, page: Page) -> str | None:
        current_url = page.url
        if not  current_url.lower().endswith(".pdf") or "pdf" in current_url.lower():
            return None
        try:
            redirect_bytes = await self.try_pdf_url(current_url)
            if redirect_bytes: 
                return redirect_bytes
        except Exception as e: 
                #log 
            return None
    
   
    async def try_browser_strategies(self, domain:str,page: Page) -> str | None:
        """
        Run the **four** browser strategies in order.
        Return extracted text or *None* when everything fails.
        """
        
        strategies = [
            self.find_via_selector,
            self.find_via_redirect,
            self.find_pdf_button,
            self.find_via_anchor
        ]
        for fn in strategies:
            try:
                pdf_bytes_brows_str = await fn(domain, page) if fn is self.find_via_selector else await fn(page)
            except Exception:
                continue
            if pdf_bytes_brows_str:                # one of the strategies succeeded
                return pdf_bytes_brows_str  
        # All four strategies failed
        print("All browser methods failed to extract PDF.")
        return None

    async def fetch_pdf_with_browser(self, landing) -> Optional[str]:
    
        # client = self._client_required()
        # # if doi:
        # #     landing_url = str((await client.get(f"https://doi.org/{doi}")).url)
        # # else:
        # #     landing_url = str(landing)
        # landing_url = landing
        # domain = drop_www(urlparse(landing_url).netloc.lower())

        # username = os.getenv("uni_username")
        # password = os.getenv("uni_password")
        # if not username or not password:
        #     raise RuntimeError("Missing EZProxy credentials.")

        context = await self.context_required()
        async with await  context.new_page() as page:
            try:
                await stealth_async(page)
            except ImportError:
                logging.warning("playwright_stealth not installed; continuing without stealth")

            await page.goto(landing, wait_until='networkidle')
                
            pdf_bytes_brows = await self.try_browser_strategies(domain,page)
            if pdf_bytes_brows:
                return pdf_bytes_brows, landing
            print("All browser methods failed to extract PDF.")

            return None
        
    @staticmethod
    def _extract_meta_pdf(html: str) -> Optional[str]:
        tag = BeautifulSoup(html, "html.parser").find("meta", attrs={"name": "citation_pdf_url"})
        return tag["content"].strip() if tag and tag.get("content") else None
    
    @staticmethod
    def _crossref_fallback(doi: str) -> Optional[str]:
        try:
            r = requests.get(f"https://api.crossref.org/works/{doi}", timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception:
            return None
        links = data.get("message", {}).get("link", [])
        best = None
        for link in links:
            ct = link.get("content-type")
            url = link.get("URL")
            if ct == "text/html":
                return url
            if ct == "application/pdf" and best is None:
                best = url
            if url and url.endswith(".pdf") and best is None:
                best = url
        return best 


    async def doi_to_pmcid(
        self,
        id: str,
        tool: str = str(pmcid_tool_name),
        email: str = str(pmcid_email),
    ) -> Optional[str]:
        """
        Convert a DOI or PMID to a PMCID using the NCBI ID Converter API.

        Parameters
        ----------
        id : str
            The DOI (starts with "10.") or PMID.
        tool : str
            Name of your application/tool (required by NCBI API).
        email : str
            Email address of the maintainer (required by NCBI API).

        Returns
        -------
        Optional[str]
            PMCID if found, else None.
        """
        id = id.strip()
        idtype = "doi" if id.startswith("10.") else "pmid"

        base_url = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"
        params = {
            "tool": tool,
            "email": email,
            "ids": id,
            "idtype": idtype,
            "format": "json",
        }

        client = self._client_required()

        try:
            resp = await client.get(base_url, params=params)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            print(f"[PUBMED] HTTP error for {idtype}={id!r}: {e}")
            return None

        if data.get("status") != "ok" or not data.get("records"):
            print(f"[PUBMED] No records found for {idtype}={id!r}")
            return None

        record = data["records"][0]
        pmcid = record.get("pmcid")
        if not pmcid:
            print(f"[PUBMED] Record found but no PMCID for {idtype}={id!r}")
            return None

        print(f"[PUBMED] Found PMCID: {pmcid} for {idtype}={id!r}")
        return pmcid
    
    async def get_doc_from_pmc(
        self,
        input_id: str,
        context: BrowserContext | None = None,
    ) -> Optional[bytes]:
        """
        Given a DOI or PMCID, try to download the PDF from PubMed Central
        and return the raw PDF bytes.
        """
        base_url = "https://pmc.ncbi.nlm.nih.gov/articles"
        doi_pattern = r"^10\.\d{4,9}\/[^\s]+$"

        # 1) Convert DOI → PMCID if needed
        if re.match(doi_pattern, input_id):
            pmcid = await self.doi_to_pmcid(input_id)
            if pmcid is None:
                print(f"[PMC] Failed to resolve DOI to PMCID for {input_id}")
                return None
        else:
            pmcid = input_id

        # 2) Ensure we have a context + page
        if context is None:
            context = await self.context_required()

        page = await context.new_page()

        try:
            article_url = f"{base_url}/{pmcid}"
            print(f"[PMC] Visiting {article_url}")
            await page.goto(article_url, wait_until="networkidle")
            html_content = await page.content()

            # 3) Find a PDF link
            pattern = r'pdf/[^"\s]+'
            matches = re.findall(pattern, html_content)
            if not matches:
                print(f"[PMC] No PDF link found on PMC page for {input_id}")
                return None

            pdf_path = matches[0]
            pdf_url = f"{base_url}/{pmcid}/{pdf_path}"
            print(f"[PMC] Fetching PDF from {pdf_url}")

            # 4) Download PDF via Playwright's request API
            resp = await context.request.get(pdf_url, timeout=self.selector_timeout)
            if resp.status != 200:
                print(f"[PMC] Failed to GET PDF {pdf_url}: HTTP {resp.status}")
                return None

            raw = await resp.body()
            if not raw or not raw.startswith(b"%PDF"):
                print(f"[PMC] Invalid PDF content from {pdf_url}")
                return None

            print(f"[PMC] Successfully downloaded PDF for {input_id}")
            return raw

        except Exception as e:
            print(f"[PMC] Exception while downloading PDF for {input_id}: {e}")
            return None
        finally:
            await page.close()


    
    async def get_pdf(
        self,
        doi: Optional[str],
        paper_id: str,
        cross_ref_paper_link: Optional[str],
    ) -> tuple[bytes, str]:
        """
        Main entrypoint: try multiple strategies to obtain PDF bytes.

        Returns
        -------
        (pdf_bytes, landing_url)

        Raises
        ------
        MissingIdentifier
            If no DOI is provided and there is no usable link.
        CantDownload
            If all strategies fail to obtain a PDF.
        """
        print(f"Attempting to resolve pdf of paper_id: {paper_id!r}")
        client = self._client_required()
        browser_context = await self.context_required()

        # 1) If we have a direct CrossRef paper link, try that first
        if cross_ref_paper_link:
            print("[CROSS REF LINK] Trying cross_ref_paper_link first.")
            try:
                cross_ref_bytes = await self.try_pdf_url(
                    cross_ref_paper_link, context=browser_context
                )
            except Exception as e:
                cross_ref_bytes = None
                print(f"[CROSS REF LINK] Error: {e}")
            if cross_ref_bytes:
                print("[CROSS REF LINK] Extracted PDF.")
                return cross_ref_bytes, cross_ref_paper_link

        # 2) Require a DOI from here on
        if not doi:
            raise self.MissingIdentifier()

        # 3) Try PubMed Central via DOI → PMCID
        print("[PUBMED] Trying to fetch from PMC if available...")
        try:
            pub_med_bytes = await self.get_doc_from_pmc(doi, context=browser_context)
        except Exception as e:
            pub_med_bytes = None
            print(f"[PUBMED] Error while trying PMC: {e}")
        if pub_med_bytes:
            # Optional: resolve PMCID again just to get a nice landing URL
            pmcid = await self.doi_to_pmcid(doi)
            landing_pmc = (
                f"https://pmc.ncbi.nlm.nih.gov/articles/{pmcid}"
                if pmcid
                else "https://pmc.ncbi.nlm.nih.gov/"
            )
            print("[PUBMED] Extracted PDF via PMC.")
            return pub_med_bytes, landing_pmc

        # 4) Resolve DOI via EZProxy to a landing URL
        print("[DOI] Will attempt to resolve pdf via DOI.")
        target_url = f"https://doi.org/{doi}"
        # full_url = f"{self.ezbase}{quote(target_url, safe='')}"

        try:
            landing_resp = await client.get(target_url, timeout=15)
            landing_resp.raise_for_status()
        except Exception as e:
            print(f"[DOI] Error fetching landing page for {doi!r}: {e}")
            raise self.CantDownload(doi, target_url)

        landing = str(landing_resp.url)  # final publisher landing URL
        print(f"[DOI] Landing URL: {landing}")
        # domain = drop_www(urlparse(landing).netloc.lower())

        # 5) Springer
        if self._SPRINGER_HOST in landing:
            print("[SPRINGER] Trying with Springer landing page.")
            for springer_url in self._springer_candidates(landing, doi):
                try:
                    springer_bytes = await self.try_pdf_url(
                        springer_url, context=browser_context
                    )
                except Exception as e:
                    springer_bytes = None
                    print(f"[SPRINGER] Error for {springer_url}: {e}")
                if springer_bytes:
                    print("[SPRINGER] Extracted PDF from Springer landing page.")
                    return springer_bytes, landing

        # 6) Hindawi
        if self._HINDAWI_DOWNLOAD_RE.match(landing) or self._HINDAWI_LANDING_RE.match(landing):
            print("[HINDAWI] Trying with Hindawi landing page.")
            for hindawi_paper in (landing, f"https://doi.org/{doi}"):
                try:
                    hindawi_bytes = await self.try_pdf_url(
                        hindawi_paper, context=browser_context
                    )
                except Exception as e:
                    hindawi_bytes = None
                    print(f"[HINDAWI] Error for {hindawi_paper}: {e}")
                if hindawi_bytes:
                    print("[HINDAWI JOURNAL] Found! Extracting PDF.")
                    return hindawi_bytes, landing

        # 7) F1000
        if self._F1000_HOST_RE.match(landing):
            print("[F1000] Trying with F1000 landing page.")
            try:
                f100_pdf_bytes = await self._f1000_pdf(landing, doi)
            except Exception as e:
                f100_pdf_bytes = None
                print(f"[F1000] Error: {e}")
            if f100_pdf_bytes:
                print("[F1000] Extracted PDF.")
                return f100_pdf_bytes, landing

        # 8) OUP
        if self._OUP_HOST_RE.match(landing):
            print("[OUP] Trying with OUP landing page.")
            try:
                oup_pdf_bytes = await self._oup_pdf(landing)
            except Exception as e:
                oup_pdf_bytes = None
                print(f"[OUP] Error: {e}")
            if oup_pdf_bytes:
                print("[OUP] Extracted PDF.")
                return oup_pdf_bytes, landing

        # 9) Taylor & Francis (tandfonline)
        if "tandfonline.com" in landing:
            print("[TANDFONLINE] Trying with Tandfonline landing page.")
            try:
                tand_pdf_bytes = await self._tand_pdf(landing, doi)
            except Exception as e:
                tand_pdf_bytes = None
                print(f"[TANDFONLINE] Error: {e}")
            if tand_pdf_bytes:
                print("[TANDFONLINE] Extracted PDF.")
                return tand_pdf_bytes, landing

        # 10) SAGE
        if "sagepub.com" in landing:
            print("[SAGEPUB] Trying with Sagepub landing page.")
            try:
                sage_pdf_bytes = await self._sage_pdf(landing, doi)
            except Exception as e:
                sage_pdf_bytes = None
                print(f"[SAGEPUB] Error: {e}")
            if sage_pdf_bytes:
                print("[SAGEPUB] Extracted PDF.")
                return sage_pdf_bytes, landing

        # 11) Wiley
        if "onlinelibrary.wiley.com" in landing:
            print("[WILEY] Trying with Wiley landing page.")
            try:
                wiley_pdf_bytes = await self._wiley_pdf(landing, doi)
            except Exception as e:
                wiley_pdf_bytes = None
                print(f"[WILEY] Error: {e}")
            if wiley_pdf_bytes:
                print("[WILEY] Extracted PDF.")
                return wiley_pdf_bytes, landing

        # 12) Karger
        if "karger.com" in landing:
            print("[KARGER] Trying with Karger landing page.")
            try:
                karger_pdf_bytes = await self._karger_pdf(landing, doi)
            except Exception as e:
                karger_pdf_bytes = None
                print(f"[KARGER] Error: {e}")
            if karger_pdf_bytes:
                print("[KARGER] Extracted PDF.")
                return karger_pdf_bytes, landing

        # 13) Generic anchor/meta scoring
        print("[ANCHOR] Trying generic anchor/meta scoring on landing HTML...")
        try:
            html = (await client.get(landing)).text
        except Exception as e:
            html = ""
            print(f"[ANCHOR] Error fetching landing HTML: {e}")

        anchor_url = self._extract_anchor_pdf_score(html, landing) if html else None

        if anchor_url:
            print(f"[ANCHOR] Candidate PDF link: {anchor_url}")
            try:
                anchor_bytes = await self.try_pdf_url(
                    anchor_url, context=browser_context
                )
            except Exception as e:
                anchor_bytes = None
                print(f"[ANCHOR] Error for {anchor_url}: {e}")
            if anchor_bytes:
                print("[ANCHOR] Extracted PDF.")
                return anchor_bytes, landing

        # 14) CrossRef API fallback
        if doi:
            print("[CROSSREF API] Trying CrossRef API fallback...")
            crossref_candidate = self._crossref_fallback(doi)
            if crossref_candidate:
                print(f"[CROSSREF API] Candidate: {crossref_candidate}")
                try:
                    crossref_bytes = await self.try_pdf_url(
                        crossref_candidate, context=browser_context
                    )
                except Exception as e:
                    crossref_bytes = None
                    print(f"[CROSSREF API] Error: {e}")
                if crossref_bytes:
                    print("[CROSSREF API] Extracted PDF.")
                    return crossref_bytes, landing

        # 15) Browser automation fallback
        print("[BROWSER] Trying browser automation fallback...")
        try:
            browser_pdf_bytes = await self.fetch_pdf_with_browser(landing)
        except Exception as e:
            browser_pdf_bytes = None
            print(f"[BROWSER] Error in fetch_pdf_with_browser: {e}")
        if browser_pdf_bytes:
            print("[BROWSER] Extracted PDF via browser automation.")
            return browser_pdf_bytes, landing

        # 16) Nothing worked
        print("[FATAL] All strategies failed to extract PDF.")
        raise self.CantDownload(doi, landing)
    