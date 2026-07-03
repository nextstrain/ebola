#! /usr/bin/env python3

"""
Check that the geographic values in a curated metadata table are covered by the
phylogenetic build's lat-longs table, and report any that are not.

For each geographic resolution (region, country, division, location) the check is
independent: every non-empty value in that metadata column is looked up, by exact
string match, against the set of places defined at the same resolution in the
lat-longs TSV (4-column Augur form: resolution, place, latitude, longitude;
'#'-prefixed comment lines and blank lines are ignored). A value present in the
metadata but absent from the lat-longs table has no coordinate and would fall back
to (0, 0) when the phylogenetic build renders the map, so it is worth surfacing.

Augur also ships a default lat-longs table (augur/data/lat_longs.tsv) that it
consults as a fallback, so a value missing from the provided --lat-longs file but
present in that default still resolves to a coordinate at build time. We load the
installed augur's copy the same way augur does (via importlib.resources) and treat
its places as covered too, so only genuinely uncovered values are reported.

Each unmatched value is reported once, with the number of records it appears in and
the accessions of those records, followed by the distinct (country, division,
location) tuples those records carry — context for locating the value, e.g.

    Location Beni (n=3) not found in lat-longs (found in accessions A, B, C)
        (country, division, location) = (Democratic Republic of the Congo, Nord-Kivu, Beni)   n=3

The report is written to --output (grouped by resolution); an empty file means full
coverage.
"""

import argparse
import csv
import sys
from collections import defaultdict
from importlib.resources import files

# Metadata columns to check, in report order, each keyed to its lat-longs resolution.
GEOGRAPHIC_FIELDS = ["region", "country", "division", "location"]

# Fields printed as context beneath each unmatched value, to locate it geographically.
CONTEXT_FIELDS = ["country", "division", "location"]


def read_lat_long_places(path):
    """Return {resolution: {place, ...}} of places defined in a lat-longs TSV."""
    places = defaultdict(set)
    with open(path, encoding="utf-8", newline="") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            fields = line.split("\t")
            if len(fields) < 2:
                continue
            resolution, place = fields[0], fields[1]
            places[resolution].add(place)
    return places


def augur_default_lat_longs():
    """Locate the lat-longs table bundled with the installed augur (fallback source)."""
    path = files("augur") / "data" / "lat_longs.tsv"
    if not path.is_file():
        sys.exit(f"augur default lat-longs not found at {path}")
    return path


def read_metadata(path, id_column):
    """Return (rows, fieldnames) for a metadata TSV read as dicts."""
    with open(path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        fieldnames = reader.fieldnames or []
        if id_column not in fieldnames:
            sys.exit(f"Metadata {path} has no {id_column!r} column")
        return list(reader), fieldnames


def find_uncovered(rows, field, known_places):
    """Return {value: [row, ...]} for values of `field` absent from known_places.

    Values are collected in first-seen order; rows preserve their order.
    """
    uncovered = {}
    for row in rows:
        value = (row.get(field) or "").strip()
        if not value or value in known_places:
            continue
        uncovered.setdefault(value, []).append(row)
    return uncovered


def context_tuples(rows):
    """Return [(tuple, count), ...] of distinct (country, division, location) values.

    An empty cell is shown as <empty>. Most common tuple first, then alphabetically.
    """
    counts = defaultdict(int)
    for row in rows:
        tup = tuple((row.get(f) or "").strip() or "<empty>" for f in CONTEXT_FIELDS)
        counts[tup] += 1
    return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--metadata", required=True, help="Curated metadata TSV to check")
    parser.add_argument("--lat-longs", required=True, help="Augur lat-longs TSV to check against")
    parser.add_argument(
        "--id-column", default="accession", help="Metadata column identifying each record"
    )
    parser.add_argument("--output", help="Report path (default: stdout)")
    args = parser.parse_args()

    # A value counts as covered if it's in the provided file or augur's default,
    # since augur consults the default as a fallback at build time.
    known = read_lat_long_places(args.lat_longs)
    for resolution, places in read_lat_long_places(augur_default_lat_longs()).items():
        known[resolution] |= places

    rows, fieldnames = read_metadata(args.metadata, args.id_column)

    lines = []
    n_values = 0
    for field in GEOGRAPHIC_FIELDS:
        if field not in fieldnames:
            print(f"Skipping {field!r}: not a column in {args.metadata}", file=sys.stderr)
            continue
        uncovered = find_uncovered(rows, field, known.get(field, set()))
        n_values += len(uncovered)
        # Most-common values first, then alphabetically, so the report is stable.
        for value, matched in sorted(uncovered.items(), key=lambda kv: (-len(kv[1]), kv[0])):
            accessions = [row.get(args.id_column, "") for row in matched]
            lines.append(
                f"{field.capitalize()} {value} (n={len(accessions)}) "
                f"not found in lat-longs (found in accessions {', '.join(accessions)})"
            )
            for tup, count in context_tuples(matched):
                lines.append(f"    (country, division, location) = ({', '.join(tup)})   n={count}")

    # write output to args.output AND stdout
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines) + "\n")
    print("\n".join(lines) + "\n")

    print(
        f"{n_values} geographic value(s) not found in {args.lat_longs}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
