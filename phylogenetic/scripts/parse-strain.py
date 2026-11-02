"""
Parse the "specimenCollectorSampleId" column of a (large) metadata TSV and
summarise the sequencing centers encoded in the sample IDs.

Sample IDs are expected to look like {sequencing center}-{number}, e.g.
"BIA-4924". This script prints the counts of each sequencing center, plus the
IDs which don't conform to that scheme. A sequencing center which contains
digits is treated as non-conforming.
"""
import argparse
import csv
import json
import re
from collections import Counter

# sample ID = {sequencing center}-{number}, where the number is the trailing
# run of digits and the center is everything before the final hyphen.
CENTER_RE = re.compile(r"^(.+)-(\d+)$")


def identify_center(sample_id):
    """Return the sequencing center for a sample ID, or None if it doesn't
    conform to the {sequencing center}-{number} scheme (which includes any
    'center' that itself contains digits)."""
    match = CENTER_RE.match(sample_id)
    if not match:
        return None
    center = match.group(1)
    if any(ch.isdigit() for ch in center):
        return None
    return center


def parse(metadata):
    rows = []  # (id, center_or_None)
    centers = Counter()
    nonconforming = []  # sample IDs with a value that don't match the scheme
    missing = 0  # samples with an empty specimenCollectorSampleId

    with open(metadata, newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for col in ("id", "specimenCollectorSampleId"):
            if col not in reader.fieldnames:
                raise SystemExit(f"No '{col}' column found in {metadata}")

        for row in reader:
            sample_id = row["specimenCollectorSampleId"]
            center = identify_center(sample_id) if sample_id else None
            rows.append((row["id"], center))
            if center is not None:
                centers[center] += 1
            elif sample_id:
                nonconforming.append(sample_id)
            else:
                missing += 1

    return rows, centers, nonconforming, missing


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--metadata", required=True, help="Metadata TSV")
    parser.add_argument(
        "--output",
        required=True,
        help="Node data JSON, keyed on id, with a 'sequencing_center' per node "
        "(non-conforming strains are excluded)",
    )
    args = parser.parse_args()

    rows, centers, nonconforming, missing = parse(args.metadata)

    nodes = {
        sample_id: {"sequencing_center": center}
        for sample_id, center in rows
        if center is not None
    }
    with open(args.output, "w") as fh:
        json.dump({"nodes": nodes}, fh, indent=2)

    print("Sequencing center counts:")
    for center, count in centers.most_common():
        print(f"  {center}\t{count}")

    print(f"\nNon-conforming sample IDs ({len(nonconforming)}):")
    for sample_id in nonconforming:
        print(f"  {sample_id}")

    print(f"\nMissing (empty) sample IDs: {missing}")
