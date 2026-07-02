#! /usr/bin/env python3

"""
Collect a representative GPS coordinate (latitude/longitude) for each MoH health
zone from the INRB-UMIE/Ebola_DRC_2026 data repository and emit a tidy TSV.

The repo has no point coordinates for health zones, only polygon geometry. The
built map product build/drc_health_zones.geojson holds one Polygon/MultiPolygon
per zone, already in WGS84 (EPSG:4326) lon/lat per data/shapefiles/
DRC_Health_zones.prj, so we read that and use the polygon centroid as the
zone's point.

Output is a 4-column TSV in Augur lat-longs form:

    "location"	<health zone>	<latitude>	<longitude>

The literal first column is the geographic resolution ("location"). Rows are
grouped by province, each group preceded by a comment header row naming the
province, e.g. `# DRC, Ituri`. The health-zone name is `nom`, the same join key
emitted by collect-cases.py and matched to the shapefile attribute `Nom` (see
that repo's data/aliases.csv). All 519 zones are emitted, a superset of those
appearing in the case data.

Centroid method (no third-party deps; shapely is not assumed installed): the
area-weighted centroid of each polygon's exterior ring via the shoelace
formula. For a MultiPolygon the parts are combined weighted by their absolute
areas. Interior rings (holes) are ignored — they shift a health-zone centroid
negligibly, and exterior-only avoids relying on GeoJSON ring-winding order,
which real-world shapefile exports do not always follow. A degenerate
(zero-area) ring falls back to the mean of its vertices.
"""

import argparse
import csv
import json
import sys

DEFAULT_REPO = "/Users/naboo/github/INRB-UMIE/Ebola_DRC_2026"
GEOJSON_SUBPATH = "build/drc_health_zones.geojson"
ALIASES_SUBPATH = "data/aliases.csv"


def ring_area_centroid(ring):
    """Signed shoelace area and area-weighted centroid (cx, cy) of one ring.

    Returns (area, cx, cy) with `area` signed (sign follows ring winding).
    Falls back to the vertex mean for a degenerate (near-zero-area) ring.
    """
    a = cx = cy = 0.0
    n = len(ring)
    for i in range(n - 1):
        x0, y0 = ring[i][0], ring[i][1]
        x1, y1 = ring[i + 1][0], ring[i + 1][1]
        cross = x0 * y1 - x1 * y0
        a += cross
        cx += (x0 + x1) * cross
        cy += (y0 + y1) * cross
    a *= 0.5
    if a == 0:
        pts = ring[:-1] or ring  # drop closing point if present
        mx = sum(p[0] for p in pts) / len(pts)
        my = sum(p[1] for p in pts) / len(pts)
        return 0.0, mx, my
    return a, cx / (6 * a), cy / (6 * a)


def polygon_exteriors(geometry):
    """Yield the exterior ring of each polygon part in a (Multi)Polygon."""
    gtype = geometry["type"]
    coords = geometry["coordinates"]
    if gtype == "Polygon":
        yield coords[0]
    elif gtype == "MultiPolygon":
        for part in coords:
            yield part[0]
    else:
        raise ValueError(f"Unsupported geometry type: {gtype}")


def accumulate(geometry, acc):
    """Add a (Multi)Polygon's area-weighted centroid moments to acc [total, wx, wy]."""
    for ring in polygon_exteriors(geometry):
        area, cx, cy = ring_area_centroid(ring)
        w = abs(area) or 1e-12  # weight tiny/degenerate parts minimally
        acc[0] += w
        acc[1] += cx * w
        acc[2] += cy * w


def centroid(geometry):
    """Area-weighted (lon, lat) centroid across a (Multi)Polygon's exteriors."""
    acc = [0.0, 0.0, 0.0]
    accumulate(geometry, acc)
    return acc[1] / acc[0], acc[2] / acc[0]

def report_aliases(output_fname, provinces, zones):

    alias_path = f"{args.repo}/{ALIASES_SUBPATH}"
    try:
        with open(alias_path, encoding="utf-8") as fh:
            alias_reader = list(csv.DictReader(fh))
    except FileNotFoundError:
        sys.exit(f"Aliases not found: {alias_path}")

    alias_rows = []
    for row in alias_reader:
        canonical = row["canonical_nom"]
        if canonical in provinces:
            resolution = "division"
        elif canonical in zones:
            resolution = "location"
        else:
            print(
                f"Skipping alias {row['observed_name']!r} -> {canonical!r}: "
                "canonical name is not a known province or health zone",
                file=sys.stderr,
            )
            continue
        alias_rows.append([resolution, row["observed_name"], canonical])
    alias_rows.sort(key=lambda r: (r[0], r[2], r[1]))

    # format for `augur curate apply-geolocation-rules`
    rules = []
    for r in alias_rows:
        if r[0]=='division':
            rules.append([
                f"*/Democratic Republic of the Congo/{r[1]}/*",
                f"*/Democratic Republic of the Congo/{r[2]}/*"
            ])
        else:
            rules.append([
                f"Africa/Democratic Republic of the Congo/*/{r[1]}",
                f"Africa/Democratic Republic of the Congo/*/{r[2]}"
            ])


    with open(output_fname, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter="\t", lineterminator="\n")
        writer.writerows(rules)
    print(f"Wrote {len(alias_rows)} geography aliases to {output_fname}", file=sys.stderr)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--repo",
        default=DEFAULT_REPO,
        help=f"Path to a clone of INRB-UMIE/Ebola_DRC_2026 (default: {DEFAULT_REPO})",
    )
    parser.add_argument("--output", help="Output TSV path (default: stdout)")
    parser.add_argument(
        "--alias",
        help=f"Optional path to write a 3-column geography alias TSV built from "
        f"{ALIASES_SUBPATH} (resolution, alias name, canonical name)",
    )
    args = parser.parse_args()

    path = f"{args.repo}/{GEOJSON_SUBPATH}"
    try:
        with open(path, encoding="utf-8") as fh:
            fc = json.load(fh)
    except FileNotFoundError:
        sys.exit(
            f"Geometry not found: {path}\n"
            "Is --repo pointing at a clone of INRB-UMIE/Ebola_DRC_2026 with a built "
            f"{GEOJSON_SUBPATH}?"
        )

    rows = []
    province_moments = {}  # province -> [total, wx, wy] accumulated across its zones
    for feat in fc["features"]:
        props = feat["properties"]
        province = props.get("province", "")
        lon, lat = centroid(feat["geometry"])
        rows.append(
            {
                "nom": props.get("nom", ""),
                "province": province,
                "lat": f"{lat:.6f}",
                "lon": f"{lon:.6f}",
            }
        )
        # A province is the union of its zones, so its centroid is the
        # area-weighted centroid across every ring of every zone within it.
        accumulate(feat["geometry"], province_moments.setdefault(province, [0.0, 0.0, 0.0]))

    # Group by province (header row per group), zones sorted within each.
    rows.sort(key=lambda r: (r["province"], r["nom"]))

    province_rows = []
    for province in sorted(province_moments):
        total, wx, wy = province_moments[province]
        province_rows.append([province, f"{wy / total:.6f}", f"{wx / total:.6f}"])

    header = (
        "# DRC Ministry of Health (MoH) health zones — representative point coordinates.\n"
        "# Source: INRB-UMIE/Ebola_DRC_2026, build/drc_health_zones.geojson (WGS84 polygons\n"
        "# per data/shapefiles/DRC_Health_zones.prj).\n"
        "# Each point is the area-weighted centroid of the polygon exterior ring(s) (shoelace\n"
        "# formula; MultiPolygon parts combined by absolute area; holes ignored). Provinces\n"
        "# (division) are the centroid over every ring of their constituent zones (location).\n"
        "# Columns: resolution, place, latitude, longitude.\n"
    )

    out = open(args.output, "w", newline="", encoding="utf-8") if args.output else sys.stdout
    try:
        out.write(header)
        writer = csv.writer(out, delimiter="\t", lineterminator="\n")
        out.write("# DRC provinces\n")
        for province, lat, lon in province_rows:
            writer.writerow(["division", province, lat, lon])
        current = None
        for r in rows:
            if r["province"] != current:
                current = r["province"]
                out.write(f"# DRC, {current}\n")
            writer.writerow(["location", r["nom"], r["lat"], r["lon"]])
    finally:
        if args.output:
            out.close()
            print(
                f"Wrote {len(province_rows)} province and {len(rows)} health-zone "
                f"coordinates to {args.output}",
                file=sys.stderr,
            )

    if args.alias:
        report_aliases(args.alias, set(province_moments), {r["nom"] for r in rows})


