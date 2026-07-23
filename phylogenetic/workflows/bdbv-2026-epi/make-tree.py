#! /usr/bin/env python3

"""
Build a custom Auspice v2 dataset JSON for the 2026 Bundibugyo ebolavirus
outbreak directly from the collected epi TSVs, bypassing the usual augur
tooling. Most of the dataset metadata is hardcoded below; the script only fills
in the parts that depend on the data:

  1. meta.updated  -> today's date (YYYY-MM-DD).
  2. geo_resolutions -> one deme per health zone and one per province, with
     coordinates read from data/geo.tsv (collect-coords.py output). A province's
     coordinate is the mean of its health-zone coordinates.
  3. tree -> a single ARTIFICIAL_ROOT, one giant polytomy with a branch per
     case. Cases come from a single cases.tsv column chosen with --count
     (default cumulative_confirmed_cases_clamped). A cumulative column is
     differenced per health zone into per-date new cases (so a zone's tip total
     equals its final cumulative); a "new_*" column is used as per-date counts
     directly. Each case is two nodes: a branch node (direct child of the root)
     carrying only a num_date `--lag` days before the case date, whose single
     child is the case tip with node_attrs health_zone, province, case_type,
     date (YYYY-MM-DD) and num_date (decimal year). All tips share one case_type
     value, derived from the chosen column.

Inputs default to the data/ directory beside this script. Output is the dataset
JSON (stdout unless --output is given).
"""

import argparse
import csv
import json
import os
import sys
from datetime import date as date_cls

HERE = os.path.dirname(os.path.abspath(__file__))
LAT_LONGS_TSV = os.path.join(HERE, "..", "..", "defaults", "lat_longs.tsv")
DEFAULT_LAG = 7 # days
DEFAULT_COUNT = "cumulative_confirmed_cases_clamped"

# The earliest reporting in the data is ~May 2026; the artificial root simply
# anchors the temporal axis just before the outbreak's first cases.
ROOT_DATE = "2026-05-01"

DESCRIPTION = {
    "cumulative_confirmed_cases_clamped": f"""
All data is taken from the [INRB-UMIE/Ebola_DRC_2026](https://github.com/INRB-UMIE/Ebola_DRC_2026) GitHub repo

We use the running totals of "cumulative_confirmed_cases" reported in sitreps.
To avoid situations where the count decreases, which happens due to data reconciliation upstream, we decrease a timepoints
reported case count if the subsequent timepoint's count was lower.
Increases in reported counts may be due to newly confirmed cases but there also seem to be increases due to backfilling /
incomplete data; the exact tip dates shown here therefore should be taken as approximate only.
We use a {DEFAULT_LAG} day window prior to the case reporting date so that we can visualise the change in cases over time. 
    """
}


def to_num_date(iso):
    """Convert a YYYY-MM-DD string to a decimal year (e.g. 2026-07-02 -> 2026.5).

    The fractional part is the elapsed fraction of the calendar year, which
    handles leap years correctly.
    """
    year, month, day = (int(part) for part in iso.split("-"))
    year_start = date_cls(year, 1, 1).toordinal()
    next_year_start = date_cls(year + 1, 1, 1).toordinal()
    elapsed = date_cls(year, month, day).toordinal() - year_start
    return round(year + elapsed / (next_year_start - year_start), 4)


def shift_days(iso, days):
    """Return the YYYY-MM-DD that is `days` away from the given YYYY-MM-DD."""
    year, month, day = (int(part) for part in iso.split("-"))
    return date_cls.fromordinal(date_cls(year, month, day).toordinal() + days).isoformat()


def is_iso_date(value):
    """True if `value` is a valid YYYY-MM-DD calendar date."""
    try:
        date_cls.fromisoformat(value)
        return True
    except ValueError:
        return False


def load_geo(path):
    geo = {'location': {}, 'division': {}}
    with open(path, "r", newline="", encoding="utf-8") as fh:
        lines = [line for line in fh.readlines() if line and not line.startswith('#')]
        for row in csv.DictReader(lines, fieldnames=("resolution", "name", "latitude", "longitude"), delimiter="\t"):
            if row['resolution']!='location' and row['resolution']!='division':
                continue

            deme_name = row["name"].strip()
            if not deme_name:
                continue
                
            geo[row['resolution']][deme_name] = {
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
            }

    return geo


def build_geo_resolutions(geo, observed_noms, observed_provinces):
    geo_resolutions = [
        {"key": "health_zone", "demes": {}},
        {"key": "province", "demes": {}},
    ]

    for nom in observed_noms:
        if latlong:=geo['location'].get(nom, False): # "nom" (epi terminology) is "location" (nextstrain terminology)
            geo_resolutions[0]['demes'][nom] = {"latitude": latlong["latitude"], "longitude": latlong["longitude"]}
        else:
            print(f"[warn] Health Zone {nom!r} missing from lat/longs", file=sys.stderr)

    for province in observed_provinces:
        if latlong:=geo['division'].get(province, False): # "province" (epi terminology) is "division" (nextstrain terminology)
            geo_resolutions[1]['demes'][province] = {"latitude": latlong["latitude"], "longitude": latlong["longitude"]}
        else:
            print(f"[warn] Province {province!r} missing from lat/longs", file=sys.stderr)

    return geo_resolutions


def case_type_label(count_column):
    return count_column.replace('_', ' ')

def per_date_new_counts(series, cumulative):
    """One zone's [(date, raw value)] (sorted by date) -> [(date, new_count>0)].

    For a cumulative column, the new cases at a date are the increase over that
    zone's running maximum so far (so the totals equal the final cumulative and
    are never negative even if the raw series dips); for a non-cumulative "new_*"
    column the value is the per-date count itself. Blank/ND (any non-integer)
    cells carry no information and are skipped.
    """
    counts = []
    running_max = 0
    for date, raw in series:
        try:
            value = int(str(raw).strip())
        except (TypeError, ValueError):
            continue
        if cumulative:
            new = value - running_max
            running_max = max(running_max, value)
        else:
            new = value
        if new > 0:
            counts.append((date, new))
    return counts


def build_children(cases_path, count_column, lag):
    """Per new case, a branch node under the root whose single child is the case.

    Cases are expanded from a single cases.tsv column (`count_column`). A
    cumulative column is differenced per health zone into per-date new cases; a
    "new_*" column is used directly. Each new case is two nodes: an intermediate
    branch node that is the direct child of the root and carries only a num_date
    `lag` days before the case date, and the case tip itself (with the full
    node_attrs, including a single shared case_type) hanging off that branch node.
    """
    cumulative = count_column.startswith("cumulative")
    case_type = case_type_label(count_column)

    # Group rows by zone, preserving province and the per-date count value.
    zones = {}  # nom -> {"province": str, "series": [(date, raw value)]}
    with open(cases_path, "r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        if count_column not in (reader.fieldnames or []):
            sys.exit(
                f"--count column {count_column!r} not found in {cases_path}. "
                f"Available columns: {', '.join(reader.fieldnames or [])}"
            )
        for row in reader:
            nom = row["nom"].strip()
            date = row["date"].strip()
            if not nom or not date:
                print(f"[warn] skipping row with missing nom/date: {row}", file=sys.stderr)
                continue
            if not is_iso_date(date):
                print(f"[warn] skipping row with malformed date {date!r} (nom {nom!r})", file=sys.stderr)
                continue
            zone = zones.setdefault(nom, {"province": row["province"].strip(), "series": []})
            zone["series"].append((date, row.get(count_column, "")))

    children = []
    noms = set()  # health zones
    provinces = set()
    for nom, zone in zones.items():
        province = zone["province"]
        noms.add(nom)
        provinces.add(province)
        for date, count in per_date_new_counts(sorted(zone["series"]), cumulative):
            num_date = to_num_date(date)
            branch_num_date = to_num_date(shift_days(date, -1 * int(lag)))
            for i in range(count):
                name = f"{nom}|{date}|{i + 1}"
                case_node = {
                    "name": name,
                    "node_attrs": {
                        "health_zone": {"value": nom},
                        "province": {"value": province},
                        "case_type": {"value": case_type},
                        "date": {"value": date},
                        "num_date": {"value": num_date},
                    },
                }
                children.append(
                    {
                        "name": f"{name}|branch",
                        "node_attrs": {
                            "num_date": {"value": branch_num_date},
                            "hidden": "always",
                        },
                        "children": [case_node],
                    }
                )

    return (children, noms, provinces)


def build_dataset(cases_path, geo, count_column, lag):
    (children, observed_noms, observed_provinces) = build_children(cases_path, count_column, lag)
    dataset = {
        "version": "v2",
        "meta": {
            "title": "INRB-UMIE DRC case counts for the ongoing 2026 Bundibugyo ebolavirus outbreak",
            "updated": date_cls.today().isoformat(),
            "build_url": "https://github.com/nextstrain/ebola",
            "data_provenance": [
                {
                    "name": "INRB-UMIE/Ebola_DRC_2026",
                    "url": "https://www.github.com/INRB-UMIE/Ebola_DRC_2026",
                }
            ],
            "maintainers": [
                {"name": "James Hadfield"},
                {"name": "Nextstrain", "url": "https://nextstrain.org"},
            ],
            "colorings": [
                {"key": "num_date", "title": "Reporting date", "type": "temporal"},
                {"key": "date", "title": "Reporting date (YYYY-MM-DD)", "type": "temporal"},
                {"key": "health_zone", "title": "Health Zone", "type": "categorical"},
                {"key": "province", "title": "Province", "type": "categorical"},
                {"key": "case_type", "title": "Case type", "type": "categorical"},
            ],
            "geo_resolutions": build_geo_resolutions(geo, observed_noms, observed_provinces),
            "display_defaults": {
                "geo_resolution": "health_zone",
                "color_by": "health_zone",
                "map_triplicate": False,
                "panels": ["map"]
            },
            "panels": ["tree", "map"],
        },
        "tree": {
            "name": "ARTIFICIAL_ROOT",
            "node_attrs": {
                "health_zone": {"value": "N/A"},
                "date": {"value": ROOT_DATE},
                "num_date": {"value": to_num_date(ROOT_DATE)},
                "hidden": "always",
            },
            "children": children,
        },
    }
    if description:=DESCRIPTION.get(count_column, ''):
        dataset['meta']['description'] = description
    return dataset


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--cases",
        required=True,
        help=f"Path to collect-cases.py TSV output",
    )
    parser.add_argument(
        "--count",
        default=DEFAULT_COUNT,
        help=(
            "cases.tsv column to build the tree from (default: %(default)s). A "
            "'cumulative_*' column is differenced per health zone into per-date new "
            "cases; other columns are used as per-date counts directly."
        ),
    )
    parser.add_argument(
        "--lag",
        default=DEFAULT_LAG,
        help=f"Lag (in days) between infection and case reporting dates (default: {DEFAULT_LAG})",
    )
    parser.add_argument("--output", help="Output dataset JSON path (default: stdout)")

    args = parser.parse_args()

    geo = load_geo(LAT_LONGS_TSV)
    dataset = build_dataset(args.cases, geo, args.count, args.lag)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            json.dump(dataset, fh, indent=2)
            fh.write("\n")
        print(
            f"Wrote dataset with {len(dataset['tree']['children'])} case tips to {args.output}",
            file=sys.stderr,
        )
    else:
        json.dump(dataset, sys.stdout, indent=2)
        sys.stdout.write("\n")
