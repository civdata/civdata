"""CivData CLI — query environmental compliance data from the terminal.

Queries the CivData REST API for environmental facility data, violations,
risk scores, and screening reports across all 50 US states + DC.

Usage:
    civdata search --state TX --county Harris
    civdata nearby "123 Main St, Houston, TX" --radius 1.5
    civdata facility epa_echo 110000350174
    civdata violations epa_echo 110000350174 --since 2y
    civdata screen "123 Main St, Houston, TX"
    civdata stats
    civdata sources
"""

from __future__ import annotations

import argparse
import csv
import json
import sys

_DEFAULT_API_URL = "https://civdata.dev"


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------


def _api_get(args, path: str, params: dict | None = None) -> dict | list:
    """GET request to the CivData REST API. Exits on error."""
    import httpx

    base = args.api_url.rstrip("/")
    url = f"{base}/api/v1{path}"
    headers = {}
    if args.api_key:
        headers["X-API-Key"] = args.api_key

    try:
        resp = httpx.get(url, params=params, headers=headers, timeout=30, follow_redirects=True)
        resp.raise_for_status()
        if "application/json" not in resp.headers.get("content-type", ""):
            print(f"Unexpected response from {base} (not JSON). Check --api-url.", file=sys.stderr)
            sys.exit(1)
        return resp.json()
    except httpx.HTTPStatusError as e:
        print(f"API error {e.response.status_code}: {e.response.text}", file=sys.stderr)
        sys.exit(1)
    except httpx.ConnectError:
        print(f"Could not connect to {base}", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------


def _output(data, fmt: str):
    """Write data to stdout in the requested format."""
    if fmt == "json":
        json.dump(data, sys.stdout, indent=2, default=str)
        sys.stdout.write("\n")
    elif fmt == "table":
        _print_table(data)
    elif fmt == "csv":
        _print_csv(data)


def _extract_rows(data) -> list[dict]:
    """Pull the list of records from various response shapes."""
    if isinstance(data, list):
        if data and isinstance(data[0], dict):
            return data
        return []
    if isinstance(data, dict):
        for key in ("facilities", "violations"):
            if key in data and isinstance(data[key], list):
                return data[key]
        # sources — only if it's a list of dicts (local mode), not strings (API)
        if "sources" in data and isinstance(data["sources"], list) and data["sources"] and isinstance(data["sources"][0], dict):
            return data["sources"]
        # Single record (facility detail)
        if "source" in data and "source_id" in data:
            return [data]
        # Flat summary dict (stats) — display as key-value pairs
        if "total_facilities" in data:
            return [{"metric": k, "value": v} for k, v in data.items() if not isinstance(v, (dict, list))]
    return []


# Columns to show per data type, in display order
_FACILITY_COLS = [
    "source", "source_id", "name", "city", "state", "zip_code",
    "risk_score", "risk_level", "violation_count",
]
_FACILITY_RADIUS_COLS = [
    "distance_miles", "source", "source_id", "name", "city", "state",
    "risk_score", "risk_level", "violation_count",
]
_VIOLATION_COLS = [
    "violation_date", "violation_type", "program", "status", "description",
]
_SOURCE_COLS = [
    "name", "facility_count", "violation_count", "last_ingest",
]


def _pick_columns(sample_row: dict) -> list[str]:
    """Choose which columns to display based on what fields exist."""
    if "distance_miles" in sample_row:
        return [c for c in _FACILITY_RADIUS_COLS if c in sample_row]
    if "violation_date" in sample_row:
        return [c for c in _VIOLATION_COLS if c in sample_row]
    if "facility_count" in sample_row:
        return [c for c in _SOURCE_COLS if c in sample_row]
    if "source_id" in sample_row:
        return [c for c in _FACILITY_COLS if c in sample_row]
    # Generic: show all non-dict/list keys
    return [k for k, v in sample_row.items() if not isinstance(v, (dict, list))]


def _print_table(data):
    """Print data as a human-readable table."""
    rows = _extract_rows(data)
    if not rows:
        print("No results.")
        return

    columns = _pick_columns(rows[0])
    if not columns:
        for row in rows:
            print(row)
        return

    # Calculate widths
    widths = {c: len(c) for c in columns}
    str_rows = []
    for row in rows:
        str_row = {}
        for c in columns:
            val = row.get(c)
            if val is None:
                s = ""
            elif isinstance(val, float):
                s = f"{val:.3f}" if "distance" in c or "lat" in c or "lon" in c else f"{val:.1f}"
            else:
                s = str(val)
            if len(s) > 60:
                s = s[:57] + "..."
            str_row[c] = s
            widths[c] = max(widths[c], len(s))
        str_rows.append(str_row)

    # Print
    print("  ".join(h.ljust(widths[h]) for h in columns))
    print("  ".join("-" * widths[h] for h in columns))
    for sr in str_rows:
        print("  ".join(sr[c].ljust(widths[c]) for c in columns))

    # Total hint (skip for key-value summary tables)
    if not (rows and "metric" in rows[0]):
        total = data.get("total") or data.get("total_found") or data.get("total_facilities")
        if total and isinstance(total, int) and total > len(rows):
            print(f"\n({len(rows)} of {total} shown)")


def _print_csv(data):
    """Print data as CSV."""
    rows = _extract_rows(data)
    if not rows:
        return
    columns = _pick_columns(rows[0])
    writer = csv.writer(sys.stdout)
    writer.writerow(columns)
    for row in rows:
        writer.writerow([row.get(c, "") for c in columns])


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_search(args):
    if not any([args.query, args.state, args.zip, args.county, args.program]):
        print("Provide at least one filter: --query, --state, --zip, --county, --program", file=sys.stderr)
        sys.exit(1)
    params = {"limit": args.limit}
    if args.query:
        params["query"] = args.query
    if args.state:
        params["state"] = args.state
    if args.zip:
        params["zip_code"] = args.zip
    if args.county:
        params["county"] = args.county
    if args.program:
        params["program"] = args.program
    _output(_api_get(args, "/facilities", params), args.format)


def cmd_nearby(args):
    params = {"address": args.address, "radius": args.radius, "limit": args.limit}
    _output(_api_get(args, "/search", params), args.format)


def cmd_facility(args):
    _output(_api_get(args, f"/facilities/{args.source}/{args.source_id}"), args.format)


def cmd_violations(args):
    params = {"limit": args.limit}
    if args.since:
        params["since"] = args.since
    _output(_api_get(args, f"/facilities/{args.source}/{args.source_id}/violations", params), args.format)


def cmd_screen(args):
    params = {"address": args.address, "radius": args.radius, "format": "json"}
    _output(_api_get(args, "/reports/screening", params), args.format)


def cmd_stats(args):
    _output(_api_get(args, "/stats"), args.format)


def cmd_sources(args):
    _output(_api_get(args, "/sources"), args.format)


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--format", "-f",
        choices=["json", "table", "csv"],
        default="json",
        help="Output format (default: json)",
    )
    common.add_argument("--api-url", default=_DEFAULT_API_URL, help=f"API base URL (default: {_DEFAULT_API_URL})")
    common.add_argument("--api-key", default=None, help="API key for authenticated endpoints")

    parser = argparse.ArgumentParser(
        prog="civdata",
        description="Query US environmental compliance data — facilities, violations, risk scores, and screening reports.",
        parents=[common],
    )

    sub = parser.add_subparsers(dest="command")

    # search
    p = sub.add_parser("search", parents=[common], help="Search facilities by name, state, ZIP, county, or program")
    p.add_argument("--query", "-q", help="Facility name (partial match)")
    p.add_argument("--state", "-s", help="Two-letter state code")
    p.add_argument("--zip", help="5-digit ZIP code")
    p.add_argument("--county", help="County name (partial match)")
    p.add_argument("--program", help="Program type (partial match, e.g. RCRA, CWA)")
    p.add_argument("--limit", "-l", type=int, default=100, help="Max results (default: 100)")

    # nearby
    p = sub.add_parser("nearby", parents=[common], help="Find facilities near an address or coordinates")
    p.add_argument("address", help='Address or lat,lon (e.g. "30.27,-97.74")')
    p.add_argument("--radius", "-r", type=float, default=1.0, help="Radius in miles (default: 1.0)")
    p.add_argument("--limit", "-l", type=int, default=50, help="Max results (default: 50)")

    # facility
    p = sub.add_parser("facility", parents=[common], help="Get detailed facility info with violations and score")
    p.add_argument("source", help="Data source (e.g. epa_echo, tceq)")
    p.add_argument("source_id", help="Source-specific facility ID")

    # violations
    p = sub.add_parser("violations", parents=[common], help="Get violations for a facility")
    p.add_argument("source", help="Data source (e.g. epa_echo, tceq)")
    p.add_argument("source_id", help="Source-specific facility ID")
    p.add_argument("--since", help="Filter by date: ISO (2024-01-01) or relative (2y, 6m, 90d)")
    p.add_argument("--limit", "-l", type=int, default=50, help="Max results (default: 50)")

    # screen
    p = sub.add_parser("screen", parents=[common], help="Generate environmental screening report for a location")
    p.add_argument("address", help='Address or lat,lon (e.g. "30.27,-97.74")')
    p.add_argument("--radius", "-r", type=float, default=1.0, help="Radius in miles (default: 1.0)")

    # stats
    sub.add_parser("stats", parents=[common], help="Show dataset coverage statistics")

    # sources
    sub.add_parser("sources", parents=[common], help="List all active data sources")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    dispatch = {
        "search": cmd_search,
        "nearby": cmd_nearby,
        "facility": cmd_facility,
        "violations": cmd_violations,
        "screen": cmd_screen,
        "stats": cmd_stats,
        "sources": cmd_sources,
    }

    dispatch[args.command](args)


if __name__ == "__main__":
    main()
