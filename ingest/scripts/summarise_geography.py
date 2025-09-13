#! /usr/bin/env python3

"""
Summarise geography values in a metadata TSV
OR
Summarise the changes in geo values across 2 metadata TSVs
"""

from cross_reference_inrb import parse_tsv
import argparse
from collections import Counter, defaultdict
import sys


def extract_geography_counts(metadata):
    """Extract geography tuples and return Counter object with occurrences and dict mapping tuples to sets of keys."""
    geography_tuples = []
    tuple_to_keys = defaultdict(set)

    for key, row in metadata.items():
        country = row.get('country', '')
        division = row.get('division', '')
        location = row.get('location', '')
        geo_tuple = (country, division, location)

        geography_tuples.append(geo_tuple)

        tuple_to_keys[geo_tuple].add(key)

    return Counter(geography_tuples), tuple_to_keys


def print_geography_summary(geography_counts, accessions_map, total_rows, show_accessions_if_count_less_than=10, sort_by="counts"):
    """Print formatted summary of geography counts."""
    print(f"Found {len(geography_counts)} unique geography combinations:")
    print("-" * 120)
    print(f"{'num':>4} | {'country':<35} | {'division':<30} | {'location':<25} | {'accessions'}")
    print("-" * 120)

    if sort_by == "counts":
        items = geography_counts.most_common()
    elif sort_by == "alphabetical":
        # Sort by tuple elements: country, then division, then location
        items = sorted(geography_counts.items(), key=lambda x: x[0])
    else:
        raise ValueError(f"Invalid sort_by value: {sort_by}. Must be 'counts' or 'alphabetical'")

    for (country, division, location), count in items:
        geo_tuple = (country, division, location)
        accessions = ""
        if count < show_accessions_if_count_less_than:
            accessions = ", ".join(sorted(accessions_map[geo_tuple]))
        print(f"{count:4d} | {country:<35} | {division:<30} | {location:<25} | {accessions}")
    print("-" * 120)
    print(f"Total rows: {total_rows}")


def print_geography_changes(before_counts, after_counts, before_accessions_map, after_accessions_map, before_total, after_total):
    """Print summary of changes between two geography count sets."""
    all_tuples = set(before_counts.keys()) | set(after_counts.keys())

    added = []
    removed = []
    changed = []

    for geo_tuple in all_tuples:
        before_count = before_counts.get(geo_tuple, 0)
        after_count = after_counts.get(geo_tuple, 0)

        if before_count == 0 and after_count > 0:
            added.append((geo_tuple, after_count))
        elif before_count > 0 and after_count == 0:
            removed.append((geo_tuple, before_count))
        elif before_count != after_count:
            changed.append((geo_tuple, before_count, after_count))

    print("Geography changes summary:")
    print("-" * 80)
    print(f"Total rows: {before_total} → {after_total} ({after_total - before_total:+d})")
    print(f"Unique geo tuple combinations: {len(before_counts)} → {len(after_counts)} ({len(after_counts) - len(before_counts):+d})")
    print()

    if added:
        print(f"Geo tuples present only in m2 ({len(added)}):")
        for (country, division, location), count in sorted(added, key=lambda x: x[1], reverse=True):
            print(f"  +{count:3d} | {country:<35} | {division:<25} | {location}")
        print()

    if removed:
        print(f"Geo tuples completely removed from m1 ({len(removed)}):")
        for (country, division, location), count in sorted(removed, key=lambda x: x[1], reverse=True):
            print(f"  -{count:3d} | {country:<35} | {division:<25} | {location}")
        print()

    if changed:
        print(f"Geo tuples present in both, but in different numbers ({len(changed)}):")
        for (country, division, location), before, after in sorted(changed, key=lambda x: abs(x[2] - x[1]), reverse=True):
            change = after - before
            print(f"  {change:+4d} ({before:3d} → {after:3d}) | {country:<35} | {division:<25} | {location}")
        print()

    if not added and not removed and not changed:
        print("No changes in geography tuples")

    print("-" * 80)

def prune_lab_hosts(metadata):
    return {x:z for x,z in metadata.items() if z['is_lab_host']!='True'}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--m1", required=True,  help="Metadata TSV (required)")
    parser.add_argument("--m2", required=False, help="Optional metadata TSV. If provided, we'll report differences in m1 against this one")
    parser.add_argument("--alphabetical", action='store_true', help="Sort tables alphabetically. Only works for single metadata file at the moment.")
    parser.add_argument("--include-lab-hosts", action='store_false', help="By default we'll drop rows where `is_lab_host==True`. Add this flag to not drop any rows.")
    args = parser.parse_args()

    m1 = parse_tsv(args.m1, id='accession')
    if (args.include_lab_hosts):
        m1 = prune_lab_hosts(m1)
    counts1, keys1 = extract_geography_counts(m1)

    # if we didn't provide a second metadata file, print a summary of the sole metadata file and exit:
    if args.m2 is None:
        print_geography_summary(counts1, keys1, len(m1), sort_by=('alphabetical' if args.alphabetical else 'counts'))
        sys.exit(0)

    # otherwise parse the second metadata file and generate a diff
    m2 = parse_tsv(args.m2, id='accession')
    if (args.include_lab_hosts):
        m2 = prune_lab_hosts(m2)
    counts2, keys2 = extract_geography_counts(m2)

    print_geography_changes(counts1, counts2, keys1, keys2, len(m1), len(m2))
