#!/usr/bin/env python3
"""
learncpp_to_pdf.py - export learncpp.com as one PDF per chapter (personal use only).

The site's FAQ allows converting its pages for your own private use as long as
you do not distribute them. Please do not redistribute the resulting files.

    pip install requests beautifulsoup4 playwright lxml
    playwright install chromium

    python learncpp_to_pdf.py                       # everything
    python learncpp_to_pdf.py --chapters 0 1 A      # some chapters / appendices
    python learncpp_to_pdf.py --workers 3 --rate 1  # gentler
    python learncpp_to_pdf.py --force               # rebuild existing PDFs

Design
  * Downloads: N threads share ONE global rate limiter (total req/s). 429/403/503 pause
    every thread and slow the rate down, but the rate RECOVERS gradually while the
    server behaves. Several refusals in a row stop the run cleanly.
  * Everything downloaded is saved atomically (temp + fsync + rename) in ./cache and
    validated on read. Crash / outage / power cut: just re-run to resume.
  * Rendering is pipelined: each chapter is sent to Chromium the moment its last lesson
    is downloaded, so PDF rendering overlaps with downloading instead of following it.
  * Code blocks use the website's own Prism theme + Prism script + Monaco font, so they
    look like they do on the site (colours, background, font).
"""
import argparse, asyncio, base64, hashlib, html, mimetypes, os, queue, random, re, sys
import tempfile, threading, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from importlib.util import find_spec
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE = "https://www.learncpp.com/"
PRISM_DIR = BASE + "blog/wp-content/plugins/learncpp-prism/"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
PARSER = "lxml" if find_spec("lxml") else "html.parser"
TITLE_RE = re.compile(r"^\s*([0-9]+|[A-Za-z])\.(\d+|x)\s*[—–-]\s*(.+?)\s*$")
MAX_ATTEMPTS, MAX_CONSECUTIVE_BLOCKS = 6, 6

CSS = """
*{-webkit-print-color-adjust:exact;print-color-adjust:exact}
html{font-family:"DejaVu Serif",Georgia,serif;font-size:10.5pt;line-height:1.5;color:#222}
body{margin:0}
.cover{page-break-after:always;text-align:center;padding-top:70mm}
.cover h1{font-size:28pt;margin-bottom:4mm}.cover p{color:#666;font-family:sans-serif}
.toc{page-break-after:always}.toc h2{font-family:sans-serif}
.toc ol{list-style:none;padding:0}.toc li{margin:1.5mm 0}.toc a{color:#222;text-decoration:none}
section.lesson{page-break-before:always}
section.lesson>h1{font-family:sans-serif;font-size:20pt;border-bottom:2px solid #365da0;padding-bottom:2mm;color:#1d3a6e}
.cpp-section,.cpp-topline{font-family:sans-serif;font-weight:bold;font-size:14pt;color:#1d3a6e;margin-top:7mm;page-break-after:avoid}
h2,h3,h4{font-family:sans-serif;page-break-after:avoid}
blockquote{margin:3mm 8mm;color:#444;border-left:2px solid #ccc;padding-left:4mm}
img{max-width:100%;height:auto}
table{border-collapse:collapse;margin:3mm 0;page-break-inside:avoid}
th,td{border:1px solid #bbb;padding:1.5mm 2.5mm}th{background:#eee}
div[class*="cpp-"]{margin:3mm 0}
.cpp-note,.cpp-warning,.cpp-success,.cpp-lightbluebackground,.cpp-aside,.cpp-quiz
  {border:1px solid #b9cfe8;background:#eef4fb;padding:2.5mm 4mm;page-break-inside:avoid}
.cpp-warning{background:#fdf0ef;border-color:#e3b5b0}.cpp-success{background:#eef8ee;border-color:#b4d8b4}
.cpp-note-title{font-family:sans-serif;font-weight:bold;margin:0 0 1.5mm 0}
a{color:#365da0;text-decoration:none}
:not(pre)>code{font-family:Consolas,"DejaVu Sans Mono",monospace;font-size:9pt;background:#f0f0f0;padding:0 1mm}
"""
# Used only if the website's own theme could not be downloaded
FALLBACK_CODE_CSS = """
pre{background:#f5f5f5;border:1px solid #ddd;border-left:3px solid #365da0;padding:2.5mm 3mm;
    font-family:Consolas,"DejaVu Sans Mono",monospace;font-size:8.6pt;line-height:1.35}
"""
# Always applied on top of the site's theme: a PDF cannot scroll, so wrap long lines
PRINT_CODE_CSS = """
pre[class*="language-"],pre{white-space:pre-wrap!important;word-wrap:break-word;overflow:visible!important;
    max-height:none!important;page-break-inside:avoid;margin:3mm 0}
pre code{white-space:inherit!important}
"""


# ------------------------------------------------------------ crash-safe I/O
def atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data); f.flush(); os.fsync(f.fileno())
        for attempt in range(5):  # Windows: antivirus may briefly lock the target
            try:
                os.replace(tmp, path); return
            except PermissionError:
                if attempt == 4: raise
                time.sleep(0.2 * (attempt + 1))
    except BaseException:
        try: os.unlink(tmp)
        except OSError: pass
        raise


# --------------------------------------------------- rate limiting + fetching
class Aborted(Exception): pass
class PermanentError(Exception): pass


class RateLimiter:
    """One limiter for all threads. A 429 pauses everybody and slows the rate;
    every good response then speeds it back up toward the configured rate."""
    def __init__(self, interval, abort):
        self.base = self.interval = interval
        self.abort = abort
        self._lock, self._next_ok = threading.Lock(), 0.0

    def wait(self):
        while True:
            if self.abort.is_set(): raise Aborted()
            with self._lock:
                now = time.monotonic()
                if now >= self._next_ok:
                    self._next_ok = now + self.interval * random.uniform(1.0, 1.3)
                    return
                delay = self._next_ok - now
            self.abort.wait(min(delay, 1.0) + random.uniform(0, 0.05))

    def penalize(self, seconds):
        with self._lock:
            self._next_ok = max(self._next_ok, time.monotonic() + seconds)
            self.interval = min(self.interval * 1.5, 10.0)

    def success(self):
        with self._lock:
            if self.interval > self.base:
                self.interval = max(self.base, self.interval * 0.95)


class Fetcher:
    def __init__(self, cache_dir: Path, rate: float):
        self.cache_dir = cache_dir
        self.abort = threading.Event()
        self.limiter = RateLimiter(1.0 / rate, self.abort)
        self.stats = {"downloaded": 0, "cache_hits": 0, "throttled": 0}
        self.abort_reason = ""
        self._local, self._guard = threading.local(), threading.Lock()
        self._key_locks: dict[str, threading.Lock] = {}
        self._blocks = 0

    def _session(self):
        if not hasattr(self._local, "s"):
            self._local.s = requests.Session()
            self._local.s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
        return self._local.s

    def _bump(self, key):
        with self._guard: self.stats[key] += 1

    def cache_path(self, url):
        key = hashlib.sha1(url.encode()).hexdigest()[:16]
        slug = re.sub(r"[^A-Za-z0-9._-]+", "_", urlparse(url).path.strip("/"))[-60:] or "index"
        return self.cache_dir / f"{slug}.{key}.bin"

    @staticmethod
    def _read_valid(path, validator):
        try: data = path.read_bytes()
        except OSError: return None
        return data if data and (not validator or validator(data)) else None  # else: re-download

    def get(self, url, validator=None, binary=False):
        path = self.cache_path(url)
        data = self._read_valid(path, validator)
        if data is None:
            with self._guard:
                lock = self._key_locks.setdefault(path.name, threading.Lock())
            with lock:                                   # one downloader per URL
                data = self._read_valid(path, validator)
                if data is None:
                    data = self._download(url, validator)
                    atomic_write(path, data)
                    self._bump("downloaded")
                else:
                    self._bump("cache_hits")
        else:
            self._bump("cache_hits")
        return data if binary else data.decode("utf-8", errors="replace")

    def _block(self, wait, why):
        with self._guard:
            self._blocks += 1
            self.stats["throttled"] += 1
            too_many = self._blocks >= MAX_CONSECUTIVE_BLOCKS
        self.limiter.penalize(wait)
        if too_many and not self.abort.is_set():
            self.abort_reason = f"server keeps refusing requests ({why}); stopped to avoid a ban"
            self.abort.set()

    def _download(self, url, validator):
        last = "unknown error"
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self.limiter.wait()
            try:
                r = self._session().get(url, timeout=(10, 40))
            except (requests.ConnectionError, requests.Timeout) as e:
                last = f"network error: {type(e).__name__}"
                self.abort.wait(min(2 ** attempt, 60)); continue
            code = r.status_code
            if code == 200:
                if validator is None or validator(r.content):
                    with self._guard: self._blocks = 0
                    self.limiter.success()
                    return r.content
                last = "unexpected page content (challenge/error page?)"
                self._block(30 * attempt, last)
            elif code in (403, 429, 503):
                try: ra = int(r.headers.get("Retry-After", "0"))
                except ValueError: ra = 0
                last = f"HTTP {code}"
                self._block(max(ra, 20 * attempt), last)
            elif code in (404, 410):
                raise PermanentError(f"HTTP {code}")
            else:
                last = f"HTTP {code}"
                self.abort.wait(min(2 ** attempt, 60))
        raise RuntimeError(f"giving up on {url}: {last}")


_complete_html = lambda d: b"</html>" in d
_lesson_page = lambda d: b"</html>" in d and b"entry-title" in d
_is_css = lambda d: b"{" in d and not d.lstrip()[:15].lower().startswith((b"<!doctype", b"<html"))
_is_prism_js = lambda d: b"Prism" in d and not d.lstrip()[:15].lower().startswith((b"<!doctype", b"<html"))


# --------------------------------------------------------- site code theme
def inline_css(css, base_url, fetcher):
    """Replace url(...) references (fonts etc.) with data URIs so the PDF page is self-contained."""
    def sub(m):
        ref = m.group(2).strip()
        path = urlparse(ref).path.lower()
        if ref.startswith("data:") or path.endswith((".eot", ".svg")):   # legacy formats: never used
            return m.group(0)
        full = urljoin(base_url, ref)
        try:
            raw = fetcher.get(full, binary=True)
        except Aborted: raise
        except Exception: return m.group(0)
        mime = mimetypes.guess_type(urlparse(full).path)[0] or "application/octet-stream"
        return f'url("data:{mime};base64,{base64.b64encode(raw).decode()}")'
    return re.sub(r"url\(\s*(['\"]?)([^)'\"]+)\1\s*\)", sub, css)


def load_theme(fetcher):
    """The website's own Prism theme, Prism script and Monaco font (cached like everything else)."""
    try:
        css = fetcher.get(PRISM_DIR + "prism-theme.css", validator=_is_css)
        try: fonts = fetcher.get(PRISM_DIR + "fonts/monaco.css", validator=_is_css)
        except (PermanentError, RuntimeError): fonts = ""
        js = fetcher.get(PRISM_DIR + "prism.js", validator=_is_prism_js)
        return {"css": inline_css(fonts, PRISM_DIR + "fonts/", fetcher) + inline_css(css, PRISM_DIR, fetcher),
                "js": js.replace("</script", "<\\/script")}
    except Aborted: raise
    except Exception as e:  # noqa: BLE001
        print(f"Warning: could not load the site's code theme ({e}); using a plain code style.", file=sys.stderr)
        return {"css": "", "js": ""}


# ------------------------------------------------------------------- parsing
norm = lambda p: str(int(p)) if p.isdigit() else p.upper()


def get_lesson_index(home_html):
    """[(url, 'N.M' or None)] in homepage order."""
    soup = BeautifulSoup(home_html, PARSER)
    entries, seen = [], set()

    def add(href, number):
        full = urljoin(BASE, href).split("#")[0]
        p = urlparse(full)
        if p.netloc.replace("www.", "") == "learncpp.com" and p.path.startswith("/cpp-tutorial/") \
                and full not in seen:
            seen.add(full); entries.append((full, number))

    for row in soup.select("div.lessontable-row"):
        a, num = row.select_one(".lessontable-row-title a[href]"), row.select_one(".lessontable-row-number")
        if a:
            n = num.get_text(strip=True) if num else None
            add(a["href"], n if n and "." in n else None)
    if not entries:  # layout changed: fall back to all links
        for a in soup.find_all("a", href=True): add(a["href"], None)
    return entries


def clean_content(content, fetcher, page_url, embed_images, hide_solutions):
    failures = []
    for sel in (".code-block", ".cf_monitor", ".prevnext", "script", "style", "ins", "noscript",
                "[data-ez-ph-id]", "[id^=ezoic]", ".printOnly"):
        for el in content.select(sel): el.decompose()
    for a in content.find_all("a", onclick=True):          # "Show solution" toggles
        if "Toggle" in a["onclick"]: a.decompose()
    for el in content.find_all(style=re.compile(r"display\s*:\s*none", re.I)):
        if hide_solutions: el.decompose()
    for el in content.find_all(style=True): del el["style"]  # also un-hides solutions

    for img in content.find_all("img"):
        src = img.get("src") or img.get("data-src")
        if not src or src.startswith("data:"): continue
        if not embed_images: img.decompose(); continue
        full = urljoin(page_url, src)
        try:
            raw = fetcher.get(full, binary=True)
            mime = mimetypes.guess_type(urlparse(full).path)[0] or "image/png"
            img["src"] = f"data:{mime};base64,{base64.b64encode(raw).decode()}"
            for attr in ("srcset", "data-src", "sizes", "loading"): img.attrs.pop(attr, None)
        except Aborted: raise
        except Exception as e:  # noqa: BLE001
            failures.append(f"{full} ({e})"); img.decompose()

    for a in content.find_all("a", href=True):
        if a["href"].startswith("javascript:"): a.unwrap()
        else: a["href"] = urljoin(page_url, a["href"])
    return failures


def process_url(url, fetcher, embed_images, hide_solutions):
    """Worker thread: fetch + parse one lesson. Shares nothing but the thread-safe fetcher."""
    soup = BeautifulSoup(fetcher.get(url, validator=_lesson_page), PARSER)
    h1 = soup.select_one("h1.entry-title") or soup.find("h1")
    m = TITLE_RE.match(h1.get_text(" ", strip=True)) if h1 else None
    body = soup.select_one("div.entry-content") or soup.select_one("[itemprop=articleBody]")
    if not m or body is None: return None
    failures = clean_content(body, fetcher, url, embed_images, hide_solutions)
    return {"prefix": norm(m.group(1)), "number": f"{m.group(1).upper()}.{m.group(2)}",
            "title": m.group(3), "url": url, "html": body.decode_contents(), "img_failures": failures}


# ----------------------------------------------------------------- rendering
def sort_key(k): return (0, int(k), "") if k.isdigit() else (1, 0, k)
def label(k): return f"Chapter {k}" if k.isdigit() else f"Appendix {k}"
def basename(k): return f"Chapter_{int(k):02d}" if k.isdigit() else f"Appendix_{k}"
def anchor(n): return "lesson-" + n.replace(".", "-")


def build_html(key, lessons, theme):
    e = html.escape
    toc = "".join(f'<li><a href="#{anchor(l["number"])}">{e(l["number"])} — {e(l["title"])}</a></li>'
                  for l in lessons)
    secs = "".join(
        f'<section class="lesson" id="{anchor(l["number"])}"><h1>{e(l["number"])} — {e(l["title"])}</h1>'
        f'{l["html"]}<p style="font-size:8pt;color:#888">Source: {e(l["url"])}</p></section>'
        for l in lessons)
    code_css = (theme["css"] or FALLBACK_CODE_CSS) + PRINT_CODE_CSS
    script = f"<script>{theme['js']}</script>" if theme["js"] else ""
    return (f'<!doctype html><html lang="en"><head><meta charset="utf-8"><title>{label(key)}</title>'
            f'<style>{CSS}</style><style>{code_css}</style>{script}</head><body>'
            f'<div class="cover"><h1>{label(key)}</h1>'
            f'<p>Learn C++ (learncpp.com)<br>Personal offline copy &mdash; not for redistribution</p></div>'
            f'<div class="toc"><h2>Contents</h2><ol>{toc}</ol></div>{secs}</body></html>')


class Renderer(threading.Thread):
    """Chromium in its own thread + event loop. Chapters are queued the moment they are
    complete, so rendering overlaps with the (rate-limited) downloading."""
    FOOTER = ('<div style="font-size:8px;width:100%;text-align:center;color:#666">'
              '<span class="pageNumber"></span></div>')

    def __init__(self, pdf_dir, jobs, theme):
        super().__init__(daemon=True)
        self.q, self.pdf_dir, self.jobs, self.theme = queue.Queue(), pdf_dir, jobs, theme
        self.errors = []

    def run(self):
        try: asyncio.run(self._main())
        except Exception as e:  # noqa: BLE001
            self.errors.append(("chromium", e))
            print(f"FAILED to run Chromium (did you run 'playwright install chromium'?): {e}", file=sys.stderr)

    async def _main(self):
        from playwright.async_api import async_playwright
        sem, tasks = asyncio.Semaphore(self.jobs), []
        async with async_playwright() as pw:
            browser = await pw.chromium.launch()
            while (item := await asyncio.to_thread(self.q.get)) is not None:
                tasks.append(asyncio.create_task(self._one(browser, sem, *item)))
            await asyncio.gather(*tasks)
            await browser.close()

    async def _one(self, browser, sem, key, lessons):
        async with sem:
            page = await browser.new_page()
            try:
                await page.set_content(build_html(key, lessons, self.theme), wait_until="load")
                await page.evaluate("() => { if (window.Prism) Prism.highlightAll(); }")  # site's own highlighter
                final = self.pdf_dir / f"{basename(key)}.pdf"
                part = final.with_name(final.name + ".part")
                opts = dict(path=str(part), format="A4", print_background=True,
                            margin={"top": "18mm", "bottom": "18mm", "left": "16mm", "right": "16mm"},
                            display_header_footer=True, header_template="<span></span>",
                            footer_template=self.FOOTER)
                try: await page.pdf(outline=True, **opts)   # bookmarks (newer Playwright)
                except TypeError: await page.pdf(**opts)
                os.replace(part, final)                      # final name only for complete PDFs
                print(f"  -> wrote {final.name}")
            except Exception as e:  # noqa: BLE001
                self.errors.append((key, e)); print(f"FAILED rendering {label(key)}: {e}", file=sys.stderr)
            finally:
                await page.close()


# ---------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description="Export learncpp.com to one PDF per chapter (personal use).")
    ap.add_argument("--out", default="learncpp_export")
    ap.add_argument("--cache", default="cache")
    ap.add_argument("--chapters", nargs="*", help="e.g. 0 1 2 A")
    ap.add_argument("--workers", type=int, default=4, help="download threads (max 8)")
    ap.add_argument("--rate", type=float, default=1.5, help="TOTAL requests/second (max 4)")
    ap.add_argument("--render-jobs", type=int, default=3, help="PDFs rendered in parallel (default 3)")
    ap.add_argument("--hide-solutions", action="store_true")
    ap.add_argument("--no-images", action="store_true")
    ap.add_argument("--force", action="store_true", help="rebuild PDFs that already exist")
    args = ap.parse_args()

    workers, rate = max(1, min(args.workers, 8)), max(0.1, min(args.rate, 4.0))
    out, cache = Path(args.out), Path(args.cache)
    pdf_dir = out / "pdf"
    pdf_dir.mkdir(parents=True, exist_ok=True); cache.mkdir(parents=True, exist_ok=True)
    for d, pat in ((cache, "*.tmp"), (pdf_dir, "*.part")):      # leftovers from a crash
        for f in d.glob(pat):
            try: f.unlink()
            except OSError: pass

    wanted = {norm(c) for c in args.chapters} if args.chapters else None
    fetcher = Fetcher(cache, rate)
    print("Fetching table of contents...")
    index = get_lesson_index(fetcher.get(BASE, validator=_complete_html))
    done_pdf = lambda k: (pdf_dir / f"{basename(k)}.pdf").exists() and not args.force

    # Decide what to process BEFORE touching any lesson: skip other/finished chapters entirely.
    expected: dict[str, set[int]] = {}
    jobs = []
    for i, (url, number) in enumerate(index):
        if number:
            ck = norm(number.split(".")[0])
            expected.setdefault(ck, set()).add(i)
            if (wanted and ck not in wanted) or done_pdf(ck): continue
        jobs.append((i, url, number))
    skipped = sorted((k for k in expected if done_pdf(k) and (not wanted or k in wanted)), key=sort_key)
    if skipped: print(f"Already exported (skipped): {', '.join(label(k) for k in skipped)}")
    if not jobs:
        print("Nothing to do. Use --force to rebuild."); return 0

    theme = load_theme(fetcher)
    print(f"{len(jobs)} lessons to process. Workers: {workers}, global rate: {rate}/s")

    lessons, finished, bad, failures = {}, set(), set(), []
    queued, renderer = set(), None

    def try_queue(key, final=False):
        """Send a chapter to Chromium once every one of its lessons is in and healthy."""
        nonlocal renderer
        if key in queued or key in bad or done_pdf(key) or (wanted and key not in wanted): return
        if key not in expected and not final: return
        if expected.get(key, set()) - finished: return
        chap = [lessons[i] for i in sorted(lessons) if lessons[i]["prefix"] == key]
        if not chap: return
        if renderer is None:
            renderer = Renderer(pdf_dir, max(1, args.render_jobs), theme); renderer.start()
        queued.add(key); renderer.q.put((key, chap))
        print(f"  {label(key)} complete -> rendering")

    ex = ThreadPoolExecutor(max_workers=workers)
    futs = {ex.submit(process_url, url, fetcher, not args.no_images, args.hide_solutions): (i, url, number)
            for i, url, number in jobs}
    try:
        for n, fut in enumerate(as_completed(futs), 1):
            i, url, number = futs[fut]
            try:
                les = fut.result()
            except Aborted:
                continue
            except Exception as e:  # noqa: BLE001
                failures.append((url, str(e)))
                if number: bad.add(norm(number.split(".")[0]))
                print(f"[{n}/{len(futs)}] FAILED {url}: {e}", file=sys.stderr); continue
            finished.add(i)
            if les is None or (wanted and les["prefix"] not in wanted):
                if number: try_queue(norm(number.split(".")[0]))
                continue
            if les["img_failures"]:
                bad.add(les["prefix"])
                for f in les["img_failures"]: print(f"    ! image failed: {f}", file=sys.stderr)
            lessons[i] = les
            print(f"[{n}/{len(futs)}] {les['number']} {les['title']}")
            try_queue(les["prefix"])
    except KeyboardInterrupt:
        fetcher.abort.set(); fetcher.abort_reason = "interrupted by user"
    finally:
        ex.shutdown(wait=True, cancel_futures=True)

    for key in sorted({l["prefix"] for l in lessons.values()}, key=sort_key):
        try_queue(key, final=True)
        if key not in queued and not done_pdf(key):
            miss = len(expected.get(key, set()) - finished)
            print(f"Skipping {label(key)}: incomplete ({miss} lesson(s) missing/failed); re-run to finish it.")

    s = fetcher.stats
    print(f"\nDownloads: {s['downloaded']} new, {s['cache_hits']} cached, {s['throttled']} throttle events, "
          f"final rate {1 / fetcher.limiter.interval:.2f} req/s.")
    if fetcher.abort.is_set():
        print(f"STOPPED EARLY: {fetcher.abort_reason}. Progress is saved in '{cache}'; re-run to continue.")
    if renderer:
        print("Waiting for PDF rendering to finish...")
        renderer.q.put(None); renderer.join()
    for url, err in failures[:20]: print(f"  failed: {url}: {err}")
    print(f"\nDone. Output folder: {pdf_dir.resolve()}")
    return 1 if (failures or (renderer and renderer.errors) or fetcher.abort.is_set()) else 0


if __name__ == "__main__":
    sys.exit(main())