#! /usr/bin/env python3

"""
Re-order, annotate and merge the canonical phylogenetic/defaults/lat_longs.tsv.

Augur's lat-longs file is a 4-column TSV (resolution, place, latitude, longitude)
with one row per geography at each resolution (region, country, division,
location). This script leaves coordinates untouched but rewrites the row order so
that

  * countries (and regions) are listed alphabetically,
  * divisions are grouped under their country, and
  * locations are grouped under their (country, division) pairing,

with a blank line and a '#'-prefixed comment introducing each section and each
country / division group.

The (division -> country) and (location -> country, division) associations are
learned from two sources, canonical first:

  1. A canonical DRC geography table (--canonical) organised like the output of
     this script: DRC divisions, then locations grouped under `# DRC, <province>`
     headers. Every entry here is a real geography (never "unknown"), even if it
     was never sampled, and its coordinates are treated as authoritative.
  2. The ingest metadata tables (--metadata, ingest/data/{species}/metadata.tsv),
     which carry country, division and location columns, for everything else.

A geography found in neither source is placed under an "(unknown)" group at the
end of its section.

The canonical table is also merged into the coordinates. For a place present in
both files, if the canonical and lat-longs coordinates agree to within 0.1 in
both latitude and longitude the canonical value is used silently; otherwise a
`# TODO XXX ...` comment is emitted and both rows are written so the conflict can
be resolved by hand. The same TODO marker flags a place that appears more than
once at one resolution with differing coordinates, and a place that maps to more
than one country / (country, division) pair (filed under its first match).

Output goes to stdout by default; pass --output to write a file. Do not redirect
stdout back onto an input file (it is truncated before it is read); write
elsewhere and move it into place, or use --output.
"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LAT_LONGS = REPO_ROOT / "phylogenetic" / "defaults" / "lat_longs.tsv"
DEFAULT_CANONICAL = REPO_ROOT / "phylogenetic" / "defaults" / "tmp-canonical-drc-geo.tsv"
DEFAULT_METADATA_GLOB = "ingest/data/*/metadata.tsv"

# Section order in the output. Regions and countries are flat (grouped only by
# themselves); divisions and locations are grouped as described in the docstring.
SECTION_ORDER = ["region", "country", "division", "location"]
UNKNOWN = "(unknown)"
DRC = "Democratic Republic of the Congo"
DRC_HEADER_PREFIX = "# DRC, "
# Coordinates within this many degrees (lat and lon) count as agreement.
AGREEMENT_THRESHOLD = 0.1


def read_lat_longs(path):
    """Return ({resolution: {place: {(lat, lon), ...}}}, n_exact_duplicates)."""
    coords = defaultdict(lambda: defaultdict(set))
    seen = set()
    duplicates = 0
    with open(path, encoding="utf-8", newline="") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            fields = line.split("\t")
            if len(fields) < 4:
                print(f"Skipping malformed line: {line!r}", file=sys.stderr)
                continue
            resolution, place, lat, lon = fields[0], fields[1], fields[2], fields[3]
            key = (resolution, place, lat, lon)
            if key in seen:
                duplicates += 1
                continue
            seen.add(key)
            coords[resolution][place].add((lat, lon))
    return coords, duplicates


def read_canonical(path):
    """Parse the canonical DRC geography table.

    Returns (coords, division_to_countries, location_to_pairs) where coords is
    {resolution: {place: {(lat, lon), ...}}} and the association dicts mirror
    read_metadata_associations(). Locations are associated with the province from
    the nearest preceding `# DRC, <province>` header; every entry is DRC.
    """
    coords = defaultdict(lambda: defaultdict(set))
    division_to_countries = defaultdict(set)
    location_to_pairs = defaultdict(set)
    province = None
    with open(path, encoding="utf-8", newline="") as fh:
        for line in fh:
            line = line.rstrip("\n")
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("#"):
                if stripped.startswith(DRC_HEADER_PREFIX):
                    province = stripped[len(DRC_HEADER_PREFIX):].strip()
                else:
                    province = None  # e.g. "# DRC provinces" or the file preamble
                continue
            fields = line.split("\t")
            if len(fields) < 4:
                print(f"Skipping malformed canonical line: {line!r}", file=sys.stderr)
                continue
            resolution, place, lat, lon = fields[0], fields[1], fields[2], fields[3]
            coords[resolution][place].add((lat, lon))
            if resolution == "division":
                division_to_countries[place].add(DRC)
            elif resolution == "location":
                location_to_pairs[place].add((DRC, province or ""))
    return coords, division_to_countries, location_to_pairs


def read_metadata_associations(metadata_paths):
    """Learn geography groupings from the ingest metadata tables.

    Returns (division_to_countries, location_to_pairs):
      division_to_countries: {division: {country, ...}}
      location_to_pairs:      {location: {(country, division), ...}}
    """
    division_to_countries = defaultdict(set)
    location_to_pairs = defaultdict(set)
    for path in metadata_paths:
        with open(path, encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh, delimiter="\t")
            for row in reader:
                country = (row.get("country") or "").strip()
                division = (row.get("division") or "").strip()
                location = (row.get("location") or "").strip()
                if division:
                    division_to_countries[division].add(country)
                if location:
                    location_to_pairs[location].add((country, division))
    return division_to_countries, location_to_pairs


def within_threshold(a, b):
    """True if two (lat, lon) string pairs agree to within AGREEMENT_THRESHOLD."""
    return (
        abs(float(a[0]) - float(b[0])) <= AGREEMENT_THRESHOLD
        and abs(float(a[1]) - float(b[1])) <= AGREEMENT_THRESHOLD
    )


def merge_coords(place, ll_coords, canonical_coords):
    """Merge lat-longs and canonical coordinates for one place.

    Returns (rows, todo): `rows` is the list of (lat, lon) pairs to emit and
    `todo` is None or a string describing a disagreement/ambiguity to flag.
    """
    if not canonical_coords:
        rows = sorted(ll_coords)
        todo = None
        if len(rows) > 1:
            todo = f"{place!r} has multiple differing coordinates in lat-longs"
        return rows, todo

    if len(canonical_coords) > 1:
        # Shouldn't happen for a well-formed canonical table; surface it if it does.
        rows = sorted(canonical_coords)
        return rows, f"{place!r} has multiple differing coordinates in the canonical file"

    canonical = next(iter(canonical_coords))
    disagreeing = sorted(c for c in ll_coords if not within_threshold(c, canonical))
    if not disagreeing:
        # Canonical wins; any lat-longs values agree closely enough to drop.
        return [canonical], None

    rows = [canonical, *disagreeing]
    alternatives = ", ".join(f"lat-longs ({lat}, {lon})" for lat, lon in disagreeing)
    todo = (
        f"{place!r} coordinate disagreement: canonical ({canonical[0]}, {canonical[1]}) "
        f"vs {alternatives}"
    )
    return rows, todo


def merge_resolution(resolution, ll_coords, canonical_coords):
    """Return {place: (rows, coord_todo)} across all places at one resolution."""
    places = set(ll_coords.get(resolution, {})) | set(canonical_coords.get(resolution, {}))
    merged = {}
    for place in places:
        ll = ll_coords.get(resolution, {}).get(place, set())
        canonical = canonical_coords.get(resolution, {}).get(place, set())
        merged[place] = merge_coords(place, ll, canonical)
    return merged


def resolve_group(place, matches, render):
    """Pick a group for `place` from candidate association matches.

    Returns (group_label, todo); todo names the alternatives not chosen, or None.
    """
    if not matches:
        return UNKNOWN, None
    ordered = sorted(matches, key=render)
    chosen = render(ordered[0])
    if len(ordered) == 1:
        return chosen, None
    others = ", ".join(render(m) for m in ordered[1:])
    return chosen, f"{place!r} also matches: {others}"


def combine_todos(*todos):
    """Collect the non-empty todo strings into a list."""
    return [t for t in todos if t]


def assign_groups(merged, matches_for, render):
    """Bucket merged entries into groups.

    `merged` is {place: (rows, coord_todo)}. Returns
    {group_label: [(place, rows, [todo, ...]), ...]} sorted within each group.
    """
    groups = defaultdict(list)
    for place, (rows, coord_todo) in merged.items():
        group, group_todo = resolve_group(place, matches_for(place), render)
        groups[group].append((place, rows, combine_todos(coord_todo, group_todo)))
    for entries in groups.values():
        entries.sort(key=lambda e: e[0])
    return groups


def group_sort_key(group):
    """Sort helper putting the "(unknown)" bucket last, everything else A-Z."""
    return (group == UNKNOWN, group)


def normalized_name(name):
    """Lowercase and drop punctuation/whitespace, keeping alphanumeric characters."""
    return "".join(ch for ch in name.lower() if ch.isalnum())


def edit_distance_at_most_one(a, b):
    """True if `a` and `b` are within a single-character edit of each other."""
    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False
    if la == lb:
        return sum(x != y for x, y in zip(a, b)) <= 1
    if la > lb:
        a, b, la, lb = b, a, lb, la
    # `a` is shorter by one; allow exactly one insertion into `b` to align them.
    i = j = 0
    skipped = False
    while i < la and j < lb:
        if a[i] == b[j]:
            i += 1
            j += 1
        elif skipped:
            return False
        else:
            skipped = True
            j += 1
    return True


def names_similar(a, b):
    """Similar if, ignoring case and punctuation, the names differ by at most one
    further character (one insertion, deletion or substitution)."""
    return edit_distance_at_most_one(normalized_name(a), normalized_name(b))


def find_similar_clusters(names):
    """Group names into connected components under the `names_similar` relation."""
    parent = {n: n for n in names}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    names = list(names)
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            if names_similar(a, b):
                parent[find(a)] = find(b)
    clusters = defaultdict(set)
    for n in names:
        clusters[find(n)].add(n)
    return list(clusters.values())


def write_rows(out, resolution, place, rows, todos):
    """Write a place's TODO comment(s) followed by its data row(s)."""
    for todo in todos:
        out.write(f"# TODO XXX {todo}\n")
    for lat, lon in rows:
        out.write(f"{resolution}\t{place}\t{lat}\t{lon}\n")


def order_group_entries(entries, canonical_places):
    """Order a group's entries, clustering similar names together.

    `entries` is a list of (place, rows, todos). Returns a list of
    (cluster_todo, [entry, ...]) blocks. Members of a similar-name cluster are
    written together, canonical-file names first, under a shared TODO comment;
    a name with no similar neighbour is its own block with no cluster todo.
    """
    by_place = {entry[0]: entry for entry in entries}
    blocks = []
    for cluster in find_similar_clusters(by_place):
        members = sorted(cluster, key=lambda place: (place not in canonical_places, place))
        cluster_entries = [by_place[place] for place in members]
        todo = None
        if len(members) > 1:
            labels = ", ".join(
                f"{place!r} (canonical)" if place in canonical_places else repr(place)
                for place in members
            )
            todo = f"similar place names in this group: {labels}"
        blocks.append((min(cluster), todo, cluster_entries))
    blocks.sort(key=lambda block: block[0])
    return [(todo, cluster_entries) for _, todo, cluster_entries in blocks]


def emit_entries(out, resolution, entries, canonical_places):
    """Write one group's entries, flagging clusters of similar place names."""
    for cluster_todo, cluster_entries in order_group_entries(entries, canonical_places):
        if cluster_todo:
            out.write(f"# TODO XXX {cluster_todo}\n")
        for place, rows, todos in cluster_entries:
            write_rows(out, resolution, place, rows, todos)


def emit_flat(out, resolution, merged, canonical_places):
    """Write a self-grouped section (region / country): one comment, then rows."""
    out.write("\n")
    out.write(f"# {resolution}\n")
    entries = [
        (place, rows, combine_todos(coord_todo)) for place, (rows, coord_todo) in merged.items()
    ]
    emit_entries(out, resolution, entries, canonical_places)


def emit_grouped(out, resolution, groups, header, canonical_places):
    """Write a grouped section (division / location) with a comment per group."""
    out.write("\n")
    out.write(f"# {header}\n")
    for group in sorted(groups, key=group_sort_key):
        out.write(f"# {group}\n")
        emit_entries(out, resolution, groups[group], canonical_places)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "lat_longs",
        nargs="?",
        type=Path,
        default=DEFAULT_LAT_LONGS,
        help=f"lat-longs TSV to reformat (default: {DEFAULT_LAT_LONGS})",
    )
    parser.add_argument(
        "--canonical",
        type=Path,
        default=DEFAULT_CANONICAL,
        help=f"Canonical DRC geography TSV to merge in (default: {DEFAULT_CANONICAL})",
    )
    parser.add_argument(
        "--metadata",
        nargs="+",
        type=Path,
        help=f"ingest metadata TSV(s) providing geography associations "
        f"(default: {DEFAULT_METADATA_GLOB} under the repo root)",
    )
    parser.add_argument("--output", type=Path, help="Output TSV path (default: stdout)")
    args = parser.parse_args()

    metadata_paths = args.metadata or sorted(REPO_ROOT.glob(DEFAULT_METADATA_GLOB))
    if not metadata_paths:
        sys.exit("No metadata tables found; pass --metadata explicitly.")
    for path in [args.lat_longs, args.canonical, *metadata_paths]:
        if not path.is_file():
            sys.exit(f"Not a file: {path}")

    ll_coords, duplicates = read_lat_longs(args.lat_longs)
    canonical_coords, can_div, can_loc = read_canonical(args.canonical)
    md_div, md_loc = read_metadata_associations(metadata_paths)

    # Canonical associations take precedence over metadata for the same place.
    def division_matches(place):
        return can_div.get(place) or md_div.get(place, set())

    def location_matches(place):
        return can_loc.get(place) or md_loc.get(place, set())

    division_groups = assign_groups(
        merge_resolution("division", ll_coords, canonical_coords),
        matches_for=division_matches,
        render=lambda country: country or UNKNOWN,
    )
    location_groups = assign_groups(
        merge_resolution("location", ll_coords, canonical_coords),
        matches_for=location_matches,
        render=lambda pair: f"{pair[0] or UNKNOWN} / {pair[1] or UNKNOWN}",
    )

    def canonical_places(resolution):
        return set(canonical_coords.get(resolution, {}))

    out = open(args.output, "w", newline="", encoding="utf-8") if args.output else sys.stdout
    try:
        emit_flat(
            out, "region", merge_resolution("region", ll_coords, canonical_coords),
            canonical_places("region"),
        )
        emit_flat(
            out, "country", merge_resolution("country", ll_coords, canonical_coords),
            canonical_places("country"),
        )
        emit_grouped(
            out, "division", division_groups, "divisions (grouped by country)",
            canonical_places("division"),
        )
        emit_grouped(
            out, "location", location_groups, "locations (grouped by country / division)",
            canonical_places("location"),
        )
        # Preserve any resolutions we don't model explicitly (should be none).
        for resolution in ll_coords:
            if resolution not in SECTION_ORDER:
                emit_flat(
                    out, resolution, merge_resolution(resolution, ll_coords, {}),
                    canonical_places(resolution),
                )
    finally:
        if args.output:
            out.close()

    if duplicates:
        print(f"Collapsed {duplicates} exact-duplicate row(s).", file=sys.stderr)


if __name__ == "__main__":
    main()
