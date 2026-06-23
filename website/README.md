# Website

The showcase site for the MBTA On-Time Lakehouse — built with [Material for MkDocs](https://squidfunk.github.io/mkdocs-material/).

## Local preview

```bash
cd website
uvx --with mkdocs-material mkdocs serve   # live preview at http://127.0.0.1:8000
```

## Build

```bash
uvx --with mkdocs-material mkdocs build    # outputs static site to website/site/
```

## Deploy

Publish the static `site/` anywhere (GitHub Pages, Netlify, Cloudflare Pages). For GitHub Pages:

```bash
uvx --with mkdocs-material mkdocs gh-deploy   # builds + pushes to the gh-pages branch
```

## Structure

- `mkdocs.yml` — site config + nav.
- `docs/` — the pages (Introduction, How it works, Under the hood, Showcase, Roadmap, Getting started).
- `site/` — built static output.
