#! /usr/bin/env python3

"""
Merges PPX metadata with NCBI Entrez metadata to spike in strain information.

Goal: merge the two inputs, joining on 'ppx.insdcAccessionBase' and
'ncbi_entrez.accession'. Note that some ppx.insdcAccessionBase might be empty
- just keep those rows as-is. For the rows that are not empty, update the
'strain' column based on this order of preference:

1. ncbi_entrez.strain
2. ncbi_entrez.isolate
3. ppx.strain
"""

import argparse
import pandas as pd

def update_strain(row):
    """
    Apply strain preference hierarchy for rows that have a match.
    Preference: ncbi.strain > ncbi.isolate > ppx.strain
    """
    if pd.notna(row['strain_ncbi']) and row['strain_ncbi'].strip():
        return row['strain_ncbi']
    elif pd.notna(row.get('isolate')) and row['isolate'].strip():
        return row['isolate']
    else:
        return row['strain']


def update_host(row):
    """
    Apply host using NCBI data if there's no PPX information    
    (As of 2025-09-22 no host information is in PPX)
    """
    if pd.notna(row['host']):
        return row['host']
    if pd.notna(row['host_ncbi']) and row['host_ncbi'].strip():
        return row['host_ncbi'].strip()
    return ''


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata-ppx", required=True, help="PPX metadata TSV file")
    parser.add_argument("--metadata-ncbi-entrez", required=True, help="NCBI Entrez metadata TSV file")
    parser.add_argument("--output", required=True, help="Output merged metadata TSV file")
    parser.add_argument("--add-fields", required=False, nargs="+", help="Columns in the NCBI table to add to the output")

    args = parser.parse_args()

    ppx = pd.read_csv(args.metadata_ppx, sep='\t')
    ncbi = pd.read_csv(args.metadata_ncbi_entrez, sep='\t')

    # rename NCBI columns so we can track attribution
    ncbi.columns = ncbi.columns + '_ncbi'

    # Keep all ppx rows, including those with empty insdcAccessionBase
    merged = ppx.merge(ncbi, left_on='insdcAccessionBase', right_on='accession_ncbi', how='left', suffixes=('', ''))

    # Apply strain preference hierarchy for rows that have a match
    merged['strain'] = merged.apply(update_strain, axis=1)

    # Apply host preference hierarchy for rows that have a match
    merged['host'] = merged.apply(update_host, axis=1)

    # Remove all ncbi columns from the merge unless they're in `--add-fields`, in which case keep them!
    if len(args.add_fields):
        merged = merged.rename(columns={x+"_ncbi":x for x in args.add_fields})
    merged = merged.drop(columns=[x for x in merged.columns if x.endswith('_ncbi')])

    merged.to_csv(args.output, sep='\t', index=False)

