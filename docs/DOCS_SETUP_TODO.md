# Documentation setup — TODO / items to update

This documentation build was ported from the **FDB** (`ecmwf/fdb`) "read the docs"
setup (Sphinx + Doxygen + Breathe + AutoAPI, published to `sites.ecmwf.int` via
the ECMWF reusable GitHub Actions workflows).

The files below were added/adapted:

- `docs/conf.py` — Sphinx configuration
- `docs/Doxyfile.in` — Doxygen configuration (templated)
- `docs/build_docs.sh` — local/CI build script (Doxygen → Sphinx)
- `docs/CMakeLists.txt` — CMake integration (`gribjump-doc` target)
- `docs/requirements.txt` — Python build dependencies
- `docs/index.rst` — top-level landing page / toctree
- `cmake/FindSphinx.cmake` — locates `sphinx-build`
- `.github/workflows/docs.yml` — build + preview + publish workflow
- CMake option `GRIBJUMP_DOCUMENTATION` wired into the top-level `CMakeLists.txt`

## Structure

The documentation is organised into three top-level sections:

- **GribJump** (`docs/gribjump/`) — the main library / C++ docs. The pre-existing
  notes (setup, index format, configuration options, RemoteFDB, round-robin
  scheduling, internals) plus a curated C++ API page (`docs/gribjump/api.rst`,
  Breathe) live here.
- **pygribjump** (`docs/pygribjump/`) — the Python bindings docs, auto-generated
  from the `pygribjump` source via AutoAPI.
- **Doxygen** (`docs/doxygen.rst`) — surfaces the full Doxygen-generated HTML
  (all class/file/namespace indexes and tables). The Doxygen HTML is copied
  verbatim into the site under `doxygen/` (via `html_extra_path`, staged by
  `build_docs.sh` / `docs/CMakeLists.txt`) and embedded/linked from the section
  page.

## ⚠️ Things that MUST be updated on the GribJump side

### 1. GitHub Actions secret / token
- `.github/workflows/docs.yml` references the secret
  **`ECMWF_SITES_DOCS_GRIBJUMP_TOKEN`** (renamed from FDB's
  `ECMWF_SITES_DOCS_FDB_TOKEN`).
- This secret does **not** exist yet. A publishing token for the `gribjump`
  docs space on `sites.ecmwf.int` must be created and added to the repository
  (or organisation) secrets under exactly this name. Coordinate with the ECMWF
  web/docs team to provision the `gribjump` space and issue the token.

### 2. sites.ecmwf.int docs "space" / name
- The workflow uses `space: docs` and `name: gribjump`. Confirm the
  `gribjump` site/space has been created on `sites.ecmwf.int`, otherwise
  publishing will fail.

### 3. URLs to verify
- `docs/conf.py`:
  - version switcher `json_url`:
    `https://sites.ecmwf.int/docs/gribjump/versions.json`
    — confirm this path once the space exists (a `versions.json` must be
    published for the version switcher to work).
  - GitHub icon link: `https://github.com/ecmwf/gribjump`
    — confirm this is the correct public repository URL.
- `docs/index.rst` and `docs/gribjump/index.rst` also link to
  `https://github.com/ecmwf/fdb`.

### 4. Doxygen input / API coverage (GribJump + Doxygen sections)
- `docs/build_docs.sh` and `docs/CMakeLists.txt` point Doxygen at
  `src/gribjump/api`, which currently contains only `ExtractionIterator.h`.
  Add/adjust the public C++ headers you want documented (and update the
  `INPUT`/glob paths if the public API lives elsewhere). The headers currently
  have no Doxygen doc-comments, so Doxygen emits "not documented" warnings —
  add `///`/`/** */` comments to populate the reference.
- `docs/gribjump/api.rst` hand-picks the classes shown under **GribJump →
  C++ API** via Breathe (`doxygenclass`). Update that list as the public API
  grows; the full auto-generated reference lives under the **Doxygen** section.

### 5. AutoAPI (Python) source path
- `docs/conf.py` `autoapi_dirs = ["../python/pygribjump/src/pygribjump"]`.
  Update if the `pygribjump` package layout changes.

### 6. Branch names in the publish workflow
- `.github/workflows/docs.yml` publishes for pushes to `develop` and `master`
  (with `develop` → `latest`, `master` → `stable`). If GribJump's default
  branch is `main` (or otherwise differs), update the branch filters and the
  `softlink` mapping accordingly.

### 7. Content review
- `docs/index.rst`, `docs/gribjump/index.rst` and `docs/pygribjump/index.rst`
  are new pages — review the wording.
- The pre-existing `docs/README.rst` is currently excluded from the Sphinx
  build (superseded by `index.rst`). Decide whether to keep, merge, or remove
  it.
- Some AutoAPI docstrings in `pygribjump` produce reStructuredText warnings
  (indentation / block-quote / cyclic import). Clean up the docstrings to
  silence them if desired.

## Building locally

```bash
python -m pip install -r docs/requirements.txt   # needs doxygen on PATH too
DOCBUILD_OUTPUT=docs_out ./docs/build_docs.sh
# output: docs_out/sphinx/index.html
```

Or via CMake:

```bash
cmake -DENABLE_GRIBJUMP_DOCUMENTATION=ON ...
cmake --build . --target gribjump-doc
```
