
import asyncio , re , httpx
from bs4 import BeautifulSoup
from typing import Optional , AsyncIterator , List
import urllib.parse
import requests
from dotenv import load_dotenv
import os, sys
from playwright.async_api import async_playwright
import asyncio
import os
from typing import Optional
from urllib.parse import quote , urlparse , urljoin
import httpx
import random
load_dotenv()



class PDFResolver:
    _SPRINGER_HOST = "link.springer.com"
    _OUP_HOST_RE = re.compile(r"^https?://academic\.oup\.com/")
    _F1000_HOST_RE = re.compile(r"^https?://(?:f1000research|wellcomeopenresearch|gatesopenresearch)\.org/")
    _HINDAWI_DOWNLOAD_RE = re.compile(r"^https?://downloads\.hindawi\.com/")
    _HINDAWI_LANDING_RE = re.compile(r"^https?://(?:www\.)?hindawi\.com/")
    _ANCHOR_HINT_RE = re.compile(r"""(?x)                        ]
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
    wiley_token = os.getenv("wiley_api_token")

    class CantDownload(RuntimeError):
        pass

    def __init__(self, *, headers: Optional[dict] = None, timeout: int = 30):
        self.headers = headers or {"User-Agent": "Mozilla/5.0 (easy-pdf-resolver/1.2)"}
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(headers=self.headers, follow_redirects=True, timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._client:
            await self._client.aclose()
            self._client = None

    def _client_required(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(headers=self.headers, follow_redirects=True, timeout=self.timeout)
        return self._client

    async def get_pdf(self, doi: str) -> str:
        client = self._client_required()
        landing = str((await client.get(f"https://doi.org/{doi}")).url)

        if self._SPRINGER_HOST in landing:
            for url in self._springer_candidates(landing, doi):
                if await self._is_pdf(url):
                    return url

        if self._HINDAWI_DOWNLOAD_RE.match(landing) or self._HINDAWI_LANDING_RE.match(landing):
            for url in (landing, f"https://doi.org/{doi}"):
                if await self._is_pdf(url):
                    return url

        if self._F1000_HOST_RE.match(landing):
            api_pdf = await self._f1000_pdf(landing, doi)
            if api_pdf and await self._is_pdf(api_pdf):
                return api_pdf

        if self._OUP_HOST_RE.match(landing):
            oup_pdf = await self._oup_pdf(landing)
            if oup_pdf:
                return oup_pdf
            
        if "onlinelibrary.wiley.com" in landing:
            wiley_pdf = await self._wiley_pdf(landing, doi)
            if wiley_pdf:
                return wiley_pdf
        
        html = (await client.get(landing)).text
        anchor = self._extract_anchor_pdf_score(html, landing)
        if anchor:
            return anchor

        cr_pdf = self._crossref_fallback(doi)
        if cr_pdf:
            return cr_pdf
        
        browser_pdf = await self.fetch_pdf_with_browser(doi)
        if browser_pdf:
            return browser_pdf
        

        raise self.CantDownload(f"Cant Downlaod  {doi} → {landing}")

    async def _springer_candidates(self, landing: str, doi: str) -> List[str]:
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
        api_url = f"https://api.{host}/article/{article_id}"
        try:
            r = await client.get(api_url)
            r.raise_for_status()
            return r.json()["data"]["pdf_url"]
        except Exception:
            return None

    async def _oup_pdf(self, landing: str) -> Optional[str]:
        client = self._client_required()
        html = (await client.get(landing)).text
        meta = self._extract_meta_pdf(html)
        if meta:
            return meta
        anchor = self._extract_anchor_pdf(html, landing)
        return anchor

    async def _wiley_pdf(self , landing:str , doi:str) -> Optional[str]:
        client = self._client_required()
        
        #1 TDM Api
        api_url = f"https://api.wiley.com/tdm/v1/articles?{doi}/pdf"
        r = await client.get(
            api_url,
            headers= {"Authorization": f"Bearer {self.wiley_token}"})
        print(f"[Wiley] API status: {r.status_code}, content-type: {r.headers.get('content-type')}")
        print(f"[Wiley] Token present? {'yes' if self.wiley_token else 'no'}")
        
        if r.status_code == 200 and r.headers.get("content-type", "").startswith("application/pdf"):
            return api_url
        
        #pdfdirect 
        
        pdf_url = f"https://onlinelibrary.wiley.com/doi/pdfdirect/{doi}"
        r2 = await client.get(pdf_url)
        if r2.status_code == 200 and r2.headers.get("content-type", "").startswith("application/pdf"):
            return pdf_url
        
        #htmlparse 
        
        html = (await client.get(landing)).text
        soup = BeautifulSoup(html, "html.parser")
        btn = soup.find("a", class_="pdf-download-link", href=True)
        if btn:
            return urllib.parse.urljoin(landing, btn["href"])
        return None 
    
    async def _tand_pdf(self , landing:str , doi:str) -> Optional[str]:
        client = self._client_required()
        
        #direct pdf endpoint 
        
        pdf_endpoint = f"https://www.tandfonline.com/doi/pdf/{doi}"
        resp = await client.get(pdf_endpoint)
        if resp.status_code == 200 and resp.headers.get("content-type", "").startswith("application/pdf"):
            return pdf_endpoint
        
        #meta tag
        html = (await client.get(landing)).text
        if (meta:= self._extract_meta_pdf(html)):
            return urllib.parse.urljoin(landing, meta)
        
        if (anchor := self._extract_anchor_pdf(html, landing)):
            return anchor
        return None
        
    async def _sage_pdf(self, landing: str, doi: str) -> Optional[str]:
        client = self._client_required()
        
        #pdf endpoint
        
        pdf_url = f"https://journals.sagepub.com/doi/pdf/{doi}"
        r = await client.get(pdf_url)
        if r.status_code == 200 and r.headers.get("content-type", "").startswith("application/pdf"):
            return pdf_url
        
        html = (await client.get(landing)).text
        if (meta:= self._extract_meta_pdf(html)):
            return urllib.parse.urljoin(landing, meta)
        
        if (anchor := self._extract_anchor_pdf(html, landing)):
            return anchor
        return None
        
        
    async def _karger_pdf(self, landing: str, doi: str) -> Optional[str]:
        clinet = self._client_required()
        
        #must get article id 
        
        html = (await clinet.get(landing)).text
        soup = BeautifulSoup(html, "html.parser")
        tag = soup.find("meta", attrs={"name": "dc.identifier"})
        if not tag or not tag.get("data-article-id"):
            return None
        article_id = tag["data-article-id"]
        return f"https://www.karger.com/Article/Pdf/{article_id}"
    
    async def _is_pdf(self, url: str) -> bool:
        client = self._client_required()
        try:
            r = await client.head(url)
            if r.status_code in (403, 405):
                r = await client.get(url, headers={"Range": "bytes=0-0"})
            return r.status_code == 200 and r.headers.get("content-type", "").startswith("application/pdf")
        except Exception:
            return False

    @staticmethod
    def _extract_meta_pdf(html: str) -> Optional[str]:
        tag = BeautifulSoup(html, "html.parser").find("meta", attrs={"name": "citation_pdf_url"})
        return tag["content"].strip() if tag and tag.get("content") else None

    def _extract_anchor_pdf(self, html: str, base_url: str) -> Optional[str]:
        soup = BeautifulSoup(html, "html.parser")
        join = lambda h: urllib.parse.urljoin(base_url, h)
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            lower = href.lower()
            full_url = join(href)
            if lower.endswith(".pdf") or "/article-pdf/" in lower or "/advance-article-pdf/" in lower:
                return full_url
            text = a.get_text(" ").lower()
            cls = " ".join(a.get("class", [])).lower()
            ident = a.get("id", "").lower()
            if "download pdf" in text or self._ANCHOR_HINT_RE.search(cls) or self._ANCHOR_HINT_RE.search(ident):
                return full_url
        return None
    
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
    
    async def fetch_pdf_with_browser(self ,doi: str) -> Optional[str]:
        EZPROXY_PREFIX = "https://ezproxy.lib.ucalgary.ca/login?url="
        JOURNAL_PDF_SELECTORS = {
        "bmj.com": 'a[title="Download this article as a PDF"]',
        "thelancet.com": 'a.download-pdf-link',
        "nejm.org": 'a[href*="pdf"]',
        "sciencedirect.com": 'a.pdf-download',
        "jamanetwork.com": 'a[href*="/pdf"]',
        "nature.com": 'a[data-track-action="download pdf"]',
        "springer.com": 'a[href$=".pdf"]',
        "oup.com": 'a[href$=".pdf"]',
        "frontiersin.org": 'a.article-pdf-download',
        "tandfonline.com": 'a[href*="/pdf"]',
        "sagepub.com": 'a.article-tools-download',
        "karger.com": 'a[href$=".pdf"]',
        # Add more as needed...
    }
        USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/125.0",
]
        client = self._client_required()
        landing_url = str((await client.get(f"https://doi.org/{doi}")).url)
        proxied_url = EZPROXY_PREFIX + quote(landing_url, safe="")
        domain = urlparse(landing_url).netloc

        username = os.getenv("uni_username")
        password = os.getenv("uni_password")
        if not username or not password:
            raise RuntimeError("Missing EZProxy credentials.")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True, 
                args=[ "--disable-blink-features=AutomationControlled",
                    "--disable-web-security",
                    "--no-sandbox",
                    "--disable-gpu"
                ])
            context = await browser.new_context(user_agent=random.choice(USER_AGENTS))
            page = await context.new_page()

            # EZProxy login page
            if "login" in page.url.lower():
                await page.goto(proxied_url)
                await page.fill('input[name="user"]', username)
                await page.fill('input[name="pass"]', password)
                await page.click('input[type="submit"]')
                await page.wait_for_load_state("networkidle")

            # Journal-specific selector
            selector = JOURNAL_PDF_SELECTORS.get(domain)
            if selector:
                try:
                    await page.wait_for_selector(selector, timeout=5000)
                    pdf_link = await page.locator(selector).get_attribute("href")
                    if pdf_link:
                        return urljoin(page.url, pdf_link)
                except Exception:
                    pass  # fallback to anchor hint matching

            # Fallback using _ANCHOR_HINT_RE
            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                text = a.get_text(" ").lower()
                cls = " ".join(a.get("class", [])).lower()
                id_ = a.get("id", "").lower()
                title = a.get("title", "").lower()
                aria = a.get("aria-label", "").lower()

                if (
                    href.lower().endswith(".pdf") or
                    _ANCHOR_HINT_RE.search(text) or
                    _ANCHOR_HINT_RE.search(href) or
                    _ANCHOR_HINT_RE.search(cls) or
                    _ANCHOR_HINT_RE.search(id_) or
                    _ANCHOR_HINT_RE.search(title) or
                    _ANCHOR_HINT_RE.search(aria)
                ):
                    return urljoin(page.url, href)

            return None