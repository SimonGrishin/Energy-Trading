# Energy Trading Repo Guidance

- This repository is organized around `data-pipelines/`, `backtesting/`, `trading/`, and asset-specific research folders under `assets/`.
- The clearest executable example today is `data-pipelines/requests_try.ipynb`, which calls ERCOT public report endpoints through `data-pipelines/requests_base.py`.
- Treat the repo as research-first: notebooks and small helpers are the main entry points, while the root `README.md` describes the intended multi-stage workflow.

## Architecture to keep in mind

- `data-pipelines/` is for pulling and normalizing market data locally for downstream analysis and testing.
- `backtesting/` is the validation layer for strategy ideas before they move toward trading logic.
- `trading/supply-demand/` is the main strategy concept currently documented: it combines fuel, weather, calendar, and spike signals into supply and demand curves.
- `assets/` separates research by market type (`Capacity`, `Coal`, `Crude Oil`, `Natural Gas`, `Renewables`) so work can stay asset-specific before being combined.

## Data pipeline patterns

- Reuse `data-pipelines/requests_base.py` for ERCOT API access instead of duplicating `requests.get` calls.
- The helper currently wraps `https://api.ercot.com/api/public-reports` and sends the `Ocp-Apim-Subscription-Key` header.
- `ercot_get(endpoint, params=None)` returns parsed JSON and raises on non-2xx responses; callers should pass endpoint paths like `/np3-908-er/2d_agg_esc_wind`.
- Notebook cells in `requests_try.ipynb` use pagination-style params such as `SCEDTimestampFrom`, `SCEDTimestampTo`, `page`, and `size`.

## Working conventions

- Prefer adding new data-access logic next to `requests_base.py` or in nearby notebooks rather than introducing a large framework.
- Keep code aligned with the existing style: small helper functions, direct `requests` usage, and pandas for tabular handling.
- When referencing ERCOT data, preserve the market-specific naming used in the docs: SCED, real-time gen/load, wind, solar, storage, and controllable load.
- If you need to add a new notebook example, follow the current pattern of a compact import/setup cell followed by a single request cell.

## External dependencies and integration points

- `requests` is the only explicit HTTP dependency in the checked-in code; `pandas` is imported for downstream shaping and analysis.
- ERCOT is the main external integration visible in the repo today; other ISOs are mentioned in `data-pipelines/README.md` as future or parallel targets.
- The root README frames the long-term flow as data pipelines -> backtesting -> asset research -> trading strategy synthesis.

## Practical guidance for agents

- Before changing a notebook, check whether the same logic belongs in `requests_base.py` so it can be reused.
- Keep changes localized; this repo does not currently show a build system, package manifest, or automated test suite.
- If you introduce new examples, prefer concrete asset or ISO names already used in the repo to stay consistent with the research taxonomy.