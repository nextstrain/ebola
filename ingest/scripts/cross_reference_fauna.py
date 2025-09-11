#! /usr/bin/env python3

"""
Takes the Nextstrain maintained / curated metadata for the West African 2013-
outbreak and spikes it into our canonical ebola metadata using the (INRB,
unversioned) accession as the matching ID.
"""

import argparse
from collections import defaultdict
from cross_reference_inrb import parse_tsv, write_tsv

def spike_in_fauna(metadata, fauna, insdc_id):
    """
    Modifies `metadata` in place
    """
    conflicts = defaultdict(int)
    updates = defaultdict(int)
    hits = 0
    field_map = [
        # PPX       FAUNA
        ('country', 'country'),
        ('division', 'division'),
        ('location', 'city'),
        ('date', 'date'),
    ]

    insdc_to_ppx = {ppx_row.get(insdc_id, ''): ppx_acc
                    for ppx_acc, ppx_row in metadata.items()
                    if ppx_row.get(insdc_id, '')}

    for accession, fauna_row in fauna.items():
        if accession in insdc_to_ppx:
            ppx_accession = insdc_to_ppx[accession]
            ppx_row = metadata[ppx_accession]
            hits +=1
            for fields in field_map:
                ppx_value = ppx_row[fields[0]]
                fauna_value = fauna_row[fields[1]]
                if fauna_value=='' or fauna_value=='?':
                    continue
                if ppx_value and fauna_value!=ppx_value:
                    print(f"[conflict] PPX accession {ppx_row['accession']}, NCBI accesssion {accession}. PPX {fields[0]} was {ppx_value}, changing to fauna's value: {fauna_value}")
                    conflicts[fields[0]]+=1
                ppx_row[fields[0]] = fauna_value
                updates[fields[0]]+=1

    print()
    print('-'*80)
    print("Summary of spiking in Fauna metadata:")
    print('-'*80)
    print(f"{hits}/{len(fauna)} fauna metadata rows were matched")
    print("Total updates:", updates)
    print("Total conflicts:", conflicts)
    print('-'*80)
    print()

def log_missing_fauna_samples(metadata, fauna, insdc_id):
    ppx_insdc_accessions = {row.get(insdc_id, '') for row in metadata.values() if row.get(insdc_id, '')}
    missing = set(fauna.keys()) - ppx_insdc_accessions
    print('-'*80)
    print(f"n={len(missing)} fauna sequences are missing from our metadata")
    print('-'*80)
    print("NCBI Accessions: ", "\t".join(missing))
    print("Fauna strain names: ", "\t".join([fauna[accession]['strain'] for accession in missing]))
    print('-'*80)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", help="Our canonical metadata TSV")
    parser.add_argument("--fauna-metadata", help="Nord Kivu (INRB-DRC) outbreak metadata")
    parser.add_argument("--output", help="Updated metadata")

    args = parser.parse_args()

    metadata = parse_tsv(args.metadata, id='accession')
    fauna = parse_tsv(args.fauna_metadata, id='accession')

    spike_in_fauna(metadata, fauna, insdc_id='insdcAccessionBase')

    log_missing_fauna_samples(metadata, fauna, insdc_id='insdcAccessionBase')

    write_tsv(metadata, args.output)
