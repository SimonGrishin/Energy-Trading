"""
ERCOT NP3-908-ER Houston 2-Day SCED Energy Curve Pipeline

Uses GridStatus's ErcotAPI client for ERCOT Public API authentication and
request handling, then calls NP3-908-ER endpoints directly because these
energy-curve endpoints may not be in GridStatus's endpoint-validation map.

Environment variables required:
    ERCOT_API_USERNAME (sgrishin3@gatech.edu)
    ERCOT_API_PASSWORD (E*********25)
    ERCOT_PUBLIC_API_SUBSCRIPTION_KEY (6c84e29101b448ac8e34a8c5b096136c)

Install:
    pip install gridstatus pandas

Example:
    python ercot_houston_energy_curves.py \
        --start "2026-09-05T00:00:00" \
        --end   "2026-09-06T23:59:59"        

Timestamps are ERCOT-local timestamps as expected by the API.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from gridstatus.ercot_api.ercot_api import ErcotAPI
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

BASE_URL = "https://api.ercot.com/api/public-reports"

# Houston disclosure-area curves from NP3-908-ER.
ENDPOINTS = {
    "demand_clr": "/np3-908-er/2d_agg_edc_clr_houston",
    "supply_esr": "/np3-908-er/2d_agg_esc_esr_houston",
    "supply_pvgr": "/np3-908-er/2d_agg_esc_pvgr_houston",
    "supply_wind": "/np3-908-er/2d_agg_esc_wind_houston",
    "supply_nonirr": "/np3-908-er/2d_agg_esc_nonirr_houston",
}

HOUSTON_ENDPOINTS = {
    "wind": "np3-908-er/2d_agg_esc_wind_houston",
    "solar": "np3-908-er/2d_agg_esc_pvgr_houston",
    "nonirr": "np3-908-er/2d_agg_esc_nonirr_houston",
    "storage": "np3-908-er/2d_agg_esc_esr_houston",
    "demand": "np3-908-er/2d_agg_edc_clr_houston",
}


LOG = logging.getLogger("ercot_energy_curves")


def safe_filename(value: str) -> str:
    """Convert an endpoint name to a filesystem-safe name."""
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip("/"))


def validate_credentials() -> None:
    required = [
        "ERCOT_API_USERNAME",
        "ERCOT_API_PASSWORD",
        "ERCOT_PUBLIC_API_SUBSCRIPTION_KEY",
    ]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise EnvironmentError(
            "Missing ERCOT environment variable(s): "
            + ", ".join(missing)
            + ". Set these locally; do not hard-code or share the values."
        )


def make_client() -> ErcotAPI:
    validate_credentials()
    # GridStatus manages token retrieval/refresh and supplies both required
    # authentication headers in make_api_call().
    return ErcotAPI()


def fetch_endpoint(
    client: ErcotAPI,
    endpoint: str,
    start: str,
    end: str,
    page_size: int = 1000,
    pause_seconds: float = 0.2,
    max_pages: int | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Fetch every page for one endpoint.

    Returns:
        dataframe: rows aligned to the API's `fields` array
        audit: metadata and raw page responses, retained for reproducibility
    """
    if page_size < 1 or page_size > 1000:
        raise ValueError("page_size must be between 1 and 1000.")

    base_params: dict[str, Any] = {
        "SCEDTimestampFrom": start,
        "SCEDTimestampTo": end,
        "size": page_size,
    }

    all_rows: list[list[Any]] = []
    fields: list[dict[str, Any]] | None = None
    raw_pages: list[dict[str, Any]] = []
    first_meta: dict[str, Any] = {}
    total_pages = 1
    page = 1

    while page <= total_pages:
        params = {**base_params, "page": page}
        url = f"{BASE_URL}{endpoint}"
        LOG.info("Fetching %s page %s", endpoint, page)

        # GridStatus's lower-level method handles Bearer-token refresh,
        # subscription-key headers, request timeouts, and rate-limit retries.
        response = client.make_api_call(url, api_params=params)

        if not isinstance(response, dict):
            raise TypeError(
                f"Expected JSON object from {endpoint}; got "
                f"{type(response).__name__}."
            )

        raw_pages.append(response)

        if fields is None:
            fields = response.get("fields", [])
            first_meta = response.get("_meta", {}) or {}
            total_pages = int(first_meta.get("totalPages", 1) or 1)

            if not fields:
                raise RuntimeError(
                    f"{endpoint} response had no 'fields'. "
                    f"Response keys: {list(response.keys())}. "
                    "Inspect the saved/printed API error; endpoint access, "
                    "authentication, or response schema may have changed."
                )

        page_rows = response.get("data", [])
        # ERCOT normally returns each row as a list corresponding to `fields`.
        if isinstance(page_rows, list):
            all_rows.extend(page_rows)
        else:
            raise TypeError(
                f"Unexpected 'data' type for {endpoint}: "
                f"{type(page_rows).__name__}"
            )

        if max_pages is not None and page >= max_pages:
            break

        page += 1
        if page <= total_pages and pause_seconds:
            time.sleep(pause_seconds)

    column_names = [field["name"] for field in (fields or [])]
    df = pd.DataFrame(all_rows, columns=column_names)

    audit = {
        "endpoint": endpoint,
        "requested_start": start,
        "requested_end": end,
        "page_size": page_size,
        "reported_total_pages": total_pages,
        "pages_downloaded": len(raw_pages),
        "reported_total_records": first_meta.get("totalRecords"),
        "fields": fields or [],
        "raw_pages": raw_pages,
        "retrieved_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    return df, audit


def run_pipeline(
    start: str,
    end: str,
    output_dir: str | Path = "../data/raw/np3_908_er/houston",
    page_size: int = 10000,
    pause_seconds: float = 0.2,
    max_pages: int | None = None,
) -> dict[str, pd.DataFrame]:
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    client = make_client()
    results: dict[str, pd.DataFrame] = {}
    summary: list[dict[str, Any]] = []

    for dataset_name, endpoint in ENDPOINTS.items():
        df, audit = fetch_endpoint(
            client=client,
            endpoint=endpoint,
            start=start,
            end=end,
            page_size=page_size,
            pause_seconds=pause_seconds,
            max_pages=max_pages,
        )

        # Preserve the raw API response, including page metadata and field labels.
        raw_path = output_root / f"{dataset_name}.raw.json"
        with raw_path.open("w", encoding="utf-8") as f:
            json.dump(audit, f, ensure_ascii=False, indent=2, default=str)

        # Save a tabular convenience copy. Any curve/array-valued fields are
        # retained as values rather than transformed into fitted curves.
        csv_path = output_root / f"{dataset_name}.csv"
        df.to_csv(csv_path, index=False)

        # Add source identity to a combined table for downstream filtering.
        df["dataset"] = dataset_name
        df["source_endpoint"] = endpoint
        results[dataset_name] = df

        summary.append({
            "dataset": dataset_name,
            "endpoint": endpoint,
            "rows_downloaded": len(df),
            "pages_downloaded": audit["pages_downloaded"],
            "reported_total_records": audit["reported_total_records"],
            "csv_path": str(csv_path),
            "raw_json_path": str(raw_path),
        })

        LOG.info("%s: downloaded %s rows", dataset_name, len(df))

    combined = pd.concat(results.values(), ignore_index=True, sort=False)
    combined_path = output_root / "houston_energy_curves_combined.csv"
    combined.to_csv(combined_path, index=False)

    summary_path = output_root / "run_summary.json"
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "requested_start": start,
                "requested_end": end,
                "combined_csv": str(combined_path),
                "datasets": summary,
            },
            f,
            indent=2,
        )

    print("\nDownload summary")
    print(pd.DataFrame(summary).to_string(index=False))
    print(f"\nCombined CSV: {combined_path}")
    print(f"Run summary:  {summary_path}")
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Houston NP3-908-ER 2-Day SCED energy curves."
    )
    parser.add_argument("--start", required=True, help="ERCOT-local start timestamp")
    parser.add_argument("--end", required=True, help="ERCOT-local end timestamp")
    parser.add_argument(
        "--output-dir",
        default="../data/raw/np3_908_er/houston",
        help="Output directory",
    )
    parser.add_argument("--page-size", type=int, default=10000)
    parser.add_argument("--pause-seconds", type=float, default=0.2)
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional testing limit; if set, results may be incomplete.",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def fetch_energy_curve(
    resource: str,
    start: str,
    end: str,
    *,
    page_size: int = 10000,
    save_to: str | Path | None = None,
) -> pd.DataFrame:
    """
    Fetch a Houston NP3-908-ER SCED energy curve.

    Parameters
    ----------
    resource : str
        One of: wind, solar, nonirr, storage, demand.
    start, end : str
        ERCOT-local timestamps, e.g. '2026-09-05T00:00:00'.
    page_size : int
        Number of records requested per API page.
    save_to : str or Path, optional
        Optional CSV output path.

    Returns
    -------
    pandas.DataFrame
        The records returned by the API.
    """
    resource = resource.strip().lower()

    if resource not in HOUSTON_ENDPOINTS:
        valid = ", ".join(HOUSTON_ENDPOINTS)
        raise ValueError(
            f"Unknown resource {resource!r}. Choose from: {valid}"
        )

    if page_size < 1 or page_size > 10000:
        raise ValueError("page_size must be between 1 and 10000.")

    if start > end:
        raise ValueError("'start' must be earlier than or equal to 'end'.")

    client = make_client()  # Your existing authenticated client
    url = f"{BASE_URL}/{HOUSTON_ENDPOINTS[resource]}"

    all_rows = []
    page = 1

    while True:
        params = {
            "SCEDTimestampFrom": start,
            "SCEDTimestampTo": end,
            "page": page,
            "size": page_size,
        }
        try:
            # Reuse the existing GridStatus authentication/request logic.
            response = client.make_api_call(url, api_params=params)

        except Exception as e:
            raise RuntimeError(
                f"Failed on page {page} for {resource}.\n"
                f"URL: {url}\n"
                f"Params: {params}\n"
                f"Original error: {e}"
            ) from e

        fields = response.get("fields", [])
        records = response.get("data", [])

        if not records:
            break

        # ERCOT responses commonly supply column names separately.
        if fields and isinstance(fields[0], dict):
            columns = [field["name"] for field in fields]
            page_df = pd.DataFrame(records, columns=columns)
        else:
            page_df = pd.DataFrame(records)

        all_rows.append(page_df)

        metadata = response.get("_meta", {})
        total_pages = metadata.get("totalPages")

        if total_pages is not None:
            if page >= int(total_pages):
                break
        elif len(records) < page_size:
            break

        page += 1

    if all_rows:
        result = pd.concat(all_rows, ignore_index=True)
    else:
        result = pd.DataFrame()

    if save_to is not None:
        output_path = Path(save_to)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(output_path, index=False)

    return result


# if __name__ == "__main__":
#     args = parse_args()
#     logging.basicConfig(
#         level=getattr(logging, args.log_level.upper(), logging.INFO),
#         format="%(asctime)s %(levelname)s %(name)s: %(message)s",
#     )
#     run_pipeline(
#         start=args.start,
#         end=args.end,
#         output_dir=args.output_dir,
#         page_size=args.page_size,
#         pause_seconds=args.pause_seconds,
#         max_pages=args.max_pages,
#     )
