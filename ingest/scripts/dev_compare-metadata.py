#! /usr/bin/env python3

"""
Compare two runs of the ingest pipeline's per-species metadata and summarise how
field values changed, grouped by species and field.

The ingest pipeline writes one metadata table per species at

    <data dir>/{species}/metadata.tsv

Point this script at two such data directories (e.g. an "old" and a "new" run)
and it reports, for every species present in both, each field whose value changed
for one or more records. Records are matched on the canonical `accession` column,
which is assumed stable across runs. For each field it groups records by the
(old value -> new value) transition, e.g.

    species=ebov
      field=location
        Beni -> Butembo   n=5   PP_000LAAX, PP_000LAWQ, ...

Only fields common to both tables are diffed; columns added or removed between
runs are noted separately, as are accessions added or removed. Empty cells are
shown as <empty>.
"""

import argparse
import csv
import sys
from pathlib import Path

METADATA_SUBPATH = "{species}/metadata.tsv"
ID_FIELD = "accession"
EMPTY = "<empty>"


def read_metadata(path):
    """Return (rows_by_accession, fieldnames) for one metadata.tsv."""
    with open(path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        fieldnames = reader.fieldnames or []
        rows = {}
        for row in reader:
            accession = row.get(ID_FIELD, "")
            if not accession:
                continue
            rows[accession] = row
    return rows, fieldnames


def discover_species(data_dir):
    """Species subdirectories of data_dir that contain a metadata.tsv."""
    return {
        child.name
        for child in data_dir.iterdir()
        if child.is_dir() and (child / "metadata.tsv").is_file()
    }


def show(value):
    return value if value else EMPTY


def compare_species(old_dir, new_dir, species, ignore_fields):
    """Yield report lines for one species; empty generator if nothing changed."""
    old_rows, old_fields = read_metadata(old_dir / METADATA_SUBPATH.format(species=species))
    new_rows, new_fields = read_metadata(new_dir / METADATA_SUBPATH.format(species=species))

    lines = []

    old_ids, new_ids = set(old_rows), set(new_rows)
    added_ids = new_ids - old_ids
    removed_ids = old_ids - new_ids
    if added_ids:
        lines.append(f"  {len(added_ids)} accession(s) only in new: "
                     + ", ".join(sorted(added_ids)))
    if removed_ids:
        lines.append(f"  {len(removed_ids)} accession(s) only in old: "
                     + ", ".join(sorted(removed_ids)))

    added_cols = [f for f in new_fields if f not in old_fields]
    removed_cols = [f for f in old_fields if f not in new_fields]
    if added_cols:
        lines.append(f"  columns added: {', '.join(added_cols)}")
    if removed_cols:
        lines.append(f"  columns removed: {', '.join(removed_cols)}")

    # Diff only the fields common to both runs, over accessions in both runs.
    fields = [f for f in old_fields if f in new_fields and f != ID_FIELD
              and f not in ignore_fields]
    shared_ids = old_ids & new_ids

    for field in fields:
        # transition (old, new) -> [accessions]
        transitions = {}
        for accession in shared_ids:
            old_val = old_rows[accession].get(field, "")
            new_val = new_rows[accession].get(field, "")
            if old_val != new_val:
                transitions.setdefault((old_val, new_val), []).append(accession)
        if not transitions:
            continue
        lines.append(f"  field={field}")
        for (old_val, new_val), accessions in sorted(
            transitions.items(), key=lambda kv: (-len(kv[1]), kv[0])
        ):
            lines.append(
                f"    {show(old_val)} -> {show(new_val)}   n={len(accessions)}   "
                + ", ".join(sorted(accessions))
            )
    return lines


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("old_dir", type=Path, help="Old data directory (e.g. data_v1)")
    parser.add_argument("new_dir", type=Path, help="New data directory (e.g. data_v2)")
    parser.add_argument(
        "--species",
        nargs="+",
        help="Limit to these species (default: all found in both directories)",
    )
    parser.add_argument(
        "--ignore-fields",
        nargs="+",
        default=[],
        help="Field(s) to skip when diffing (e.g. volatile __url columns)",
    )
    args = parser.parse_args()

    for d in (args.old_dir, args.new_dir):
        if not d.is_dir():
            sys.exit(f"Not a directory: {d}")

    old_species = discover_species(args.old_dir)
    new_species = discover_species(args.new_dir)
    if args.species:
        species_list = args.species
    else:
        species_list = sorted(old_species & new_species)
        for missing in sorted(old_species ^ new_species):
            side = "new" if missing in old_species else "old"
            print(f"Skipping {missing!r}: no metadata.tsv on the {side} side",
                  file=sys.stderr)

    if not species_list:
        sys.exit("No species with metadata.tsv found in both directories.")

    any_changes = False
    for species in species_list:
        lines = compare_species(args.old_dir, args.new_dir, species, set(args.ignore_fields))
        if lines:
            any_changes = True
            print(f"species={species}")
            print("\n".join(lines))
            print()

    if not any_changes:
        print("No differences found.")
