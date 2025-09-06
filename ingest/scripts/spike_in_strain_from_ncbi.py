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

def spike_in_strain_from_ncbi(metadata_ppx, metadata_ncbi_entrez, output_file):
    """
    Merge PPX and NCBI metadata, applying strain preference hierarchy.
    """
    ppx = pd.read_csv(metadata_ppx, sep='\t')
    ncbi = pd.read_csv(metadata_ncbi_entrez, sep='\t')

    # Keep all ppx rows, including those with empty insdcAccessionBase
    merged = ppx.merge(ncbi, left_on='insdcAccessionBase', right_on='accession',
                       how='left', suffixes=('', '_ncbi'))

    # Apply strain preference hierarchy for rows that have a match
    merged['strain'] = merged.apply(update_strain, axis=1)

    # Remove temporary columns from the merge
    merged = merged.drop(columns=['strain_ncbi', 'isolate', 'accession_ncbi'])

    merged.to_csv(output_file, sep='\t', index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata-ppx", required=True, help="PPX metadata TSV file")
    parser.add_argument("--metadata-ncbi-entrez", required=True, help="NCBI Entrez metadata TSV file")
    parser.add_argument("--output", required=True, help="Output merged metadata TSV file")

    args = parser.parse_args()

    spike_in_strain_from_ncbi(
        args.metadata_ppx,
        args.metadata_ncbi_entrez,
        args.output
    )
