#! /usr/bin/env python3

"""
Takes the Nextstrain & INRB-curated metadata from the 2018 DRC outbreak and spikes it into our
canonical metadata TSV. This involves heuristics for extracting the INRB strain name from the
NCBI strain (isolate) name which is used in our canonical metadata.
"""

import argparse
import csv
import re
from collections import defaultdict

def parse_tsv(tsv_filename, id):
    result = {}
    with open(tsv_filename, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter='\t')
        for row in reader:
            if id not in row:
                raise Exception(f"Metadata parsing error. ID key '{id}' not found in row {row}")
            result[row[id]] = row
    return result

def write_tsv(data, fname):
    header = next(iter(data.values())).keys()

    for row in data.values():
        for key in row.keys():
            if key not in header:
                header.append(key)

    with open(fname, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=header, delimiter='\t')
        writer.writeheader()
        for row in data.values():
            writer.writerow(row)

def ppx_to_inrb_names(metadata):
    """
    Create a best-effort map of PPX IDs (accessions) to INRB IDs (INRB strain names)
    """
    map = {}
    pattern = re.compile(r'^.*/[^/\s-]+-([^/\s-]+)$')
    # Example: 'Ebola virus/H.sapiens-wt/COD/2018/Ituri-BTB1284' - we want 'BTB1284'
    for ppx_accession, row in metadata.items():
        strain = row['strain']
        if m := pattern.search(strain):
            map[ppx_accession] = m.groups()[0]
    return map

def spike_in_inrb_data(metadata, nord_kivu_metadata, inrb_name_map):
    """
    Modifies `metadata` in place
    """
    conflicts = defaultdict(int)
    updates = defaultdict(int)
    hits = 0
    for ppx_accession, inrb_strain in inrb_name_map.items():
        if inrb_strain in nord_kivu_metadata:
            hits +=1
            ppx_country = metadata[ppx_accession]['country']
            ppx_division = metadata[ppx_accession]['division']
            ppx_location = metadata[ppx_accession]['location']
            ppx_date = metadata[ppx_accession]['date']
            inrb_country = nord_kivu_metadata[inrb_strain]['country'].replace('_', ' ')
            inrb_division = nord_kivu_metadata[inrb_strain]['province']
            inrb_location = nord_kivu_metadata[inrb_strain]['health_zone']
            inrb_date = nord_kivu_metadata[inrb_strain]['date']

            if inrb_country:  # should always be true, but makes code easier to read
                if ppx_country and inrb_country!=ppx_country:
                    print(f"[conflict] PPX {ppx_accession}, INRB strain {inrb_strain}. PPX country was {ppx_country}, changing to INRB's value: {inrb_country}")
                    conflicts['country']+=1
                metadata[ppx_accession]['country'] = inrb_country
                updates['country']+=1

            if inrb_division:
                if ppx_division and inrb_division!=ppx_division:
                    print(f"[conflict] PPX {ppx_accession}, INRB strain {inrb_strain}. PPX division was {ppx_division}, changing to INRB's value for province: {inrb_division}")
                    conflicts['division']+=1
                metadata[ppx_accession]['division'] = inrb_division
                updates['division']+=1

            if inrb_location:
                if ppx_location and inrb_location!=ppx_location:
                    print(f"[conflict] PPX {ppx_accession}, INRB strain {inrb_strain}. PPX location was {ppx_location}, changing to INRB's value for health zone: {inrb_location}")
                    conflicts['location']+=1
                metadata[ppx_accession]['location'] = inrb_location
                updates['location']+=1

            if inrb_date:
                if ppx_date and inrb_date!=ppx_date:
                    print(f"[conflict] PPX {ppx_accession}, INRB strain {inrb_strain}. PPX date was {ppx_date}, changing to INRB's: {inrb_date}")
                    conflicts['date']+=1
                metadata[ppx_accession]['date'] = inrb_date
                updates['date']+=1

    print()
    print('-'*80)
    print("Summary of spiking in INRB metaddata:")
    print('-'*80)
    print("n metadata rows:", len(metadata.keys()))
    print("n INRB (Nord-Kivu) metadata rows:", len(nord_kivu_metadata.keys()))
    print(f"parsed {len(inrb_name_map.keys())} putative INRB-compatible strain names, of which {hits} matched INRB metadata")
    print("Total updates:", updates)
    print("Total conflicts:", conflicts)
    print('-'*80)
    print()

def log_missing_inrb_samples(nord_kivu_metadata, inrb_name_map):
    missing = set(nord_kivu_metadata.keys()) - set(inrb_name_map.values())
    for strain in missing:
        print(f"[missing data] INRB strain name {strain} not found in our (PPX) metadata")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", help="Path to the metadata TSV to ")
    parser.add_argument("--nord-kivu-metadata", help="Nord Kivu (INRB-DRC) outbreak metadata")
    parser.add_argument("--output", help="Updated metadata")

    args = parser.parse_args()

    metadata = parse_tsv(args.metadata, id='accession')
    nord_kivu = parse_tsv(args.nord_kivu_metadata, id='strain')

    inrb_name_map = ppx_to_inrb_names(metadata)
    spike_in_inrb_data(metadata, nord_kivu, inrb_name_map)

    log_missing_inrb_samples(nord_kivu, inrb_name_map)

    write_tsv(metadata, args.output)
