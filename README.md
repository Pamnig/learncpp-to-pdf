# learncpp_to_pdf

A small script that downloads the lessons from [learncpp.com](https://www.learncpp.com/) and turns them into readable PDFs, one PDF per chapter (appendices get their own PDFs too). It is meant for offline reading.

The site's FAQ says you may convert its pages to PDF for your own private use as long as you don't distribute them. Keep the PDFs to yourself. If you like the site, consider supporting it (there is a donate link on its About page).

## What you get

```
learncpp_export/
  pdf/
    Chapter_00.pdf
    Chapter_01.pdf
    ...
    Chapter_28.pdf
    Appendix_A.pdf
    ...
cache/            <- raw downloads, safe to keep, safe to delete
```

Each PDF has a cover page, a clickable contents list, bookmarks, page numbers, and one section per lesson. Ads, comments and navigation are left out. Code blocks are rendered with the website's own highlighting theme and font, and quiz solutions are included unless you ask otherwise.

## Setup

You need Python 3.10 or newer.

```
pip install -r requirements.txt
playwright install chromium
```

The first command installs the Python packages listed in `requirements.txt`. Chromium is what actually draws the PDFs and is downloaded separately by the second command, so both are required. No other system libraries are needed.

If you prefer to keep things tidy, do it inside a virtual environment first:

```
python -m venv venv
venv\Scripts\activate          # Windows
source venv/bin/activate       # Linux / macOS
```

## Running it

Everything:

```
python learncpp_to_pdf.py
```

Only some chapters (numbers for chapters, letters for appendices):

```
python learncpp_to_pdf.py --chapters 0 1 2
python learncpp_to_pdf.py --chapters A B
```

A first full run takes a while. That is deliberate, see "Being polite to the server" below.

## Flags

| Flag | Default | What it does |
| --- | --- | --- |
| `--chapters X Y ...` | all | Only process these chapters, for example `0 1 2 A`. |
| `--out DIR` | `learncpp_export` | Output folder. PDFs go in `DIR/pdf`. |
| `--cache DIR` | `cache` | Where downloaded pages and images are stored. |
| `--workers N` | 4 | Number of download threads (max 8). |
| `--rate R` | 1.5 | Total requests per second across all threads (max 4). |
| `--render-jobs N` | 3 | How many PDFs are rendered at the same time. Lower it if you are short on RAM. |
| `--hide-solutions` | off | Leave out the quiz solutions and hints. |
| `--no-images` | off | Skip images. |
| `--force` | off | Rebuild PDFs that already exist. |

## Resuming and re-running

Everything that gets downloaded is saved to the cache folder in a way that survives a crash or power cut, and is checked again when it is read. If the run stops for any reason (outage, rate limiting, Ctrl+C, closed laptop), run the same command again and it carries on with what is missing.

On a re-run:

- chapters whose PDF already exists are skipped, before anything is parsed
- chapters that were only partly downloaded are not rendered, so you never get a half-empty PDF
- a PDF only gets its final name once it is completely written

If you change something that affects the look of the PDFs (a flag like `--hide-solutions`, or a new version of the script), use `--force` to rebuild. With a full cache this needs almost no network traffic.

## Being polite to the server

The script is built so it does not hammer the site or get your IP blocked:

- all download threads share one global request limit, so more workers do not mean more requests per second
- if the site answers with 429, 403 or 503, every thread pauses, the script respects `Retry-After`, and the rate is lowered; it speeds back up slowly while the site behaves
- after several refusals in a row it stops by itself and tells you to try again later; nothing is lost

You can raise `--rate` if you are in a hurry, but I would not go much above 2 or 3.

## Troubleshooting

**"Executable doesn't exist" or Chromium fails to start.** Run `playwright install chromium`.

**"STOPPED EARLY".** The site started refusing requests, or you pressed Ctrl+C. Wait a few minutes and run the command again.

**"Skipping Chapter N: incomplete".** Some lessons or images of that chapter failed to download. Run the command again; only the missing pieces are fetched.

**The PDFs look the same after I changed something.** Existing PDFs are skipped on purpose. Add `--force`.

**Code blocks look plain.** The script could not download the site's code theme and printed a warning. It falls back to a simple grey style. Run it again later with `--force` once the connection is fine.

## How it works, briefly

1. Reads the table of contents on the homepage to get the list of lessons.
2. Downloads each lesson page and its images through a rate-limited, cached fetcher.
3. Strips everything except the lesson content and groups lessons into chapters by their number (`3.1`, `3.2`, ... belong to chapter 3).
4. As soon as a chapter is complete it is handed to headless Chromium, which prints it to PDF while the remaining chapters keep downloading.

If the site changes its layout the script may stop finding lessons or content. In that case the selectors in `get_lesson_index` and `process_url` are the places to look.