#! /usr/bin/env python3

"""
Collect Bundibugyo Ebola virus (DRC, 2026) case counts per health zone over time
from the INRB-UMIE/Ebola_DRC_2026 data repository and emit a single tidy TSV.

Source: data/insp_sitrep/processed/ in that repo, which holds the per-health-zone
daily counts parsed from the INSP "SitRep MVE" PDFs. Each metric lives in its own
file shaped `nom,date,<metric>`; here we merge the four per-zone case metrics on
(nom, date) into one table:

    nom  province  date  new_confirmed_cases  new_suspected_cases  \
                         cumulative_confirmed_cases  \
                         cumulative_confirmed_cases_clamped  \
                         cumulative_suspected_cases

`cumulative_confirmed_cases_clamped` is a derived column (not from the source):
the source `cumulative_confirmed_cases` is transcribed verbatim from the sitreps
and is occasionally revised downward, so per health zone it is not monotonic. The
clamped column pulls each day's value down to the smallest value it or any later
day reports for that zone — i.e. clamped[d] = min(value[d], value[d+1], ...,
value[last]) — giving a monotonically non-decreasing series. Only integer cells
participate; blank/ND (and any other non-integer) cells are passed through
unchanged and ignored by the running minimum. See compute_clamped().

`nom` is the canonical MoH health-zone name. Source zone labels are resolved to
canonical form via that repo's data/aliases.csv (observed_name -> canonical_nom),
so the keys here match the shapefile attribute `Nom` and the zone names emitted by
collect-coords.py (e.g. "Mongbwalu" -> "Mongbalu", "Nyankunde" -> "Nyakunde").
`province` is the province each canonical zone belongs to, read (keyed on that same
`nom`) from the built map product build/drc_health_zones.geojson, the same source
collect-coords.py uses. It is "" for any zone absent from the geojson.
Dates are the raw report dates from the sitreps (calendar dates, not epi weeks).
National-total rows live in separate `national_*` files in the source repo and are
intentionally not included here.

Empty cells vs "ND" mean different things, and this distinction is preserved from
the source:
  - "ND" ("non disponible") is a literal value present in a metric's source file:
    the zone WAS reported on that date, but that specific metric was unavailable.
    It is passed through verbatim.
  - An empty string means that metric's source file had NO row for that
    (nom, date) at all. The key exists in the output only because another metric
    reported it; the merge fills the gap with "". Note two underlying causes look
    identical here: a metric whose series simply ends earlier in time (the
    suspected-case files stop around 2026-05-30 while confirmed runs later), and a
    one-off gap on a date that other metrics covered.
"""

import argparse
import csv
import json
import sys
from collections import defaultdict

# Per-zone metrics to collect, in output column order. Each is a file
# `insp_sitrep__<metric>__daily.csv` under data/insp_sitrep/processed/.
METRICS = [
    "new_confirmed_cases",
    "new_suspected_cases",
    "cumulative_confirmed_cases",
    "cumulative_suspected_cases",
]

# Derived (not read from source): a monotonic-non-decreasing version of
# cumulative_confirmed_cases, emitted immediately after it. See compute_clamped().
CLAMPED_SOURCE = "cumulative_confirmed_cases"
CLAMPED_COLUMN = "cumulative_confirmed_cases_clamped"

PROCESSED_SUBDIR = "data/insp_sitrep/processed"
ALIASES_SUBPATH = "data/aliases.csv"
GEOJSON_SUBPATH = "build/drc_health_zones.geojson"


def load_provinces(repo):
    """Load an {nom: province} map from the built health-zone geojson.

    The geojson holds one feature per canonical health zone, each carrying `nom`
    and `province` properties — the same source collect-coords.py reads. Keyed on
    `nom`, which matches the canonical zone names in the case table. Returns an
    empty map (with a warning) if the geojson is absent.
    """
    path = f"{repo}/{GEOJSON_SUBPATH}"
    provinces = {}
    try:
        with open(path, encoding="utf-8") as fh:
            fc = json.load(fh)
    except FileNotFoundError:
        print(
            f"[warn] geojson not found, province left blank: {path}",
            file=sys.stderr,
        )
        return provinces
    for feat in fc["features"]:
        props = feat["properties"]
        nom = (props.get("nom") or "").strip()
        if nom:
            provinces[nom] = (props.get("province") or "").strip()
    return provinces


def load_aliases(repo):
    """Load data/aliases.csv into an {observed_name: canonical_nom} map.

    The file is repo-wide (column `source_dataset` only records provenance); the
    same spelling variant is resolved identically regardless of which metric it
    appears in. Returns an empty map (with a warning) if the file is absent.
    """
    path = f"{repo}/{ALIASES_SUBPATH}"
    aliases = {}
    try:
        with open(path, "r", newline="", encoding="utf-8-sig") as fh:
            for row in csv.DictReader(fh):
                observed = (row.get("observed_name") or "").strip()
                canonical = (row.get("canonical_nom") or "").strip()
                if not observed or not canonical:
                    continue
                if observed in aliases and aliases[observed] != canonical:
                    print(
                        f"[warn] conflicting alias for {observed!r}: "
                        f"{aliases[observed]!r} vs {canonical!r}; keeping the first",
                        file=sys.stderr,
                    )
                    continue
                aliases[observed] = canonical
    except FileNotFoundError:
        print(f"[warn] aliases file not found, names left unresolved: {path}", file=sys.stderr)
    return aliases


def read_metric(path, metric, aliases):
    """Yield (nom, date, value) rows from one source CSV, skipping blank/NA zones.

    `nom` is resolved to its canonical form via `aliases` before being yielded.
    """
    with open(path, "r", newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            nom = (row.get("nom") or "").strip()
            date = (row.get("date") or "").strip()
            value = (row.get(metric) or "").strip()
            if not nom or nom == "NA" or not date:
                continue
            yield aliases.get(nom, nom), date, value


def collect(repo):
    """Merge all METRICS on (canonical nom, date). Returns a dict keyed by that."""
    aliases = load_aliases(repo)
    table = defaultdict(dict)
    skipped = 0
    for metric in METRICS:
        path = f"{repo}/{PROCESSED_SUBDIR}/insp_sitrep__{metric}__daily.csv"
        try:
            for nom, date, value in read_metric(path, metric, aliases):
                cell = table[(nom, date)]
                if metric in cell and cell[metric] != value:
                    # Two source spellings collapsed onto one canonical zone for
                    # the same date with differing values. None exist in the data
                    # today; warn rather than silently overwrite if that changes.
                    print(
                        f"[warn] alias collision for ({nom}, {date}) {metric}: "
                        f"{cell[metric]!r} vs {value!r}; keeping the first",
                        file=sys.stderr,
                    )
                    continue
                cell[metric] = value
        except FileNotFoundError:
            print(f"[warn] missing source file, skipping metric: {path}", file=sys.stderr)
            skipped += 1
    if skipped == len(METRICS):
        sys.exit(
            f"No source files found under {repo}/{PROCESSED_SUBDIR}. "
            "Is --repo pointing at a clone of INRB-UMIE/Ebola_DRC_2026?"
        )
    return table


def compute_clamped(table):
    """Map each (nom, date) to a monotonic-non-decreasing cumulative_confirmed_cases.

    Per health zone, each day's value is pulled down to the smallest value it or
    any later day reports: clamped[d] = min(value[d], value[d+1], ..., value[last]),
    computed as a running minimum walking each zone's dates newest-first. This
    undoes the source's occasional downward revisions (a later date reporting fewer
    cumulative cases) by lowering the earlier peak to that smaller value.

    Only integer cells participate. Blank/ND (or any other non-integer) cells are
    passed through unchanged and left out of the running minimum, so the clamp is
    computed over the real values on either side of them.
    """
    by_zone = defaultdict(list)  # nom -> [(date, raw cumulative_confirmed_cases)]
    for (nom, date), metrics in table.items():
        by_zone[nom].append((date, metrics.get(CLAMPED_SOURCE, "")))

    clamped = {}
    for nom, series in by_zone.items():
        running_min = None
        for date, raw in sorted(series, reverse=True):  # newest date first
            try:
                value = int(raw)
            except (TypeError, ValueError):
                clamped[(nom, date)] = raw  # pass blanks/ND/etc. through untouched
                continue
            running_min = value if running_min is None else min(running_min, value)
            clamped[(nom, date)] = str(running_min)
    return clamped


def output_columns():
    """Metric columns in output order: METRICS with CLAMPED_COLUMN inserted
    immediately after its source column."""
    cols = []
    for metric in METRICS:
        cols.append(metric)
        if metric == CLAMPED_SOURCE:
            cols.append(CLAMPED_COLUMN)
    return cols


def write_tsv(table, provinces, out):
    clamped = compute_clamped(table)
    metric_cols = output_columns()
    header = ["nom", "province", "date", *metric_cols]
    writer = csv.writer(out, delimiter="\t", lineterminator="\n")
    writer.writerow(header)
    # Sort by zone, then by date, for a stable, human-readable file.
    for (nom, date) in sorted(table):
        cells = dict(table[(nom, date)])
        cells[CLAMPED_COLUMN] = clamped.get((nom, date), "")
        province = provinces.get(nom, "")
        writer.writerow([nom, province, date, *(cells.get(c, "") for c in metric_cols)])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--repo",
        required=True,
        help=f"Path to a clone of INRB-UMIE/Ebola_DRC_2026",
    )
    parser.add_argument(
        "--output",
        help="Output TSV path (default: stdout)",
    )
    args = parser.parse_args()

    table = collect(args.repo)
    provinces = load_provinces(args.repo)

    missing = sorted({nom for (nom, _date) in table} - provinces.keys())
    if missing:
        print(
            f"[warn] {len(missing)} zone(s) in the case data missing from the geojson, "
            f"province left blank: {', '.join(missing)}",
            file=sys.stderr,
        )

    if args.output:
        with open(args.output, "w", newline="", encoding="utf-8") as fh:
            write_tsv(table, provinces, fh)
        print(
            f"Wrote {len(table)} (health-zone, date) rows to {args.output}",
            file=sys.stderr,
        )
    else:
        write_tsv(table, provinces, sys.stdout)
