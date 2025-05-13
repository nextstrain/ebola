"""
This part of the workflow handles the curation of data from Pathoplexus

REQUIRED INPUTS:

    data/ppx.ndjson
    data/ncbi_entrez.ndjson

OUTPUTS:

    metadata    = data/subset_metadata.tsv
    sequences   = results/sequences.fasta

"""


def format_field_map(field_map: dict[str, str]) -> list[str]:
    """
    Format entries to the format expected by `augur curate --field-map`.
    When used in a Snakemake shell block, the list is automatically expanded and
    spaces are handled by quoted interpolation.
    """
    return [f'{key}={value}' for key, value in field_map.items()]


# This curate pipeline is based on existing pipelines for pathogen repos using NCBI data.
# You may want to add and/or remove steps from the pipeline for custom metadata
# curation for your pathogen. Note that the curate pipeline is streaming NDJSON
# records between scripts, so any custom scripts added to the pipeline should expect
# the input as NDJSON records from stdin and output NDJSON records to stdout.
# The final step of the pipeline should convert the NDJSON records to two
# separate files: a metadata TSV and a sequences FASTA.
rule curate_ppx:
    input:
        sequences_ndjson="data/ppx.ndjson",
        geolocation_rules=config["curate"]["local_geolocation_rules"],
        annotations=config["curate"]["annotations"],
    output:
        metadata="data/metadata_ppx.tsv",
        sequences="results/sequences.fasta",
    params:
        field_map=format_field_map(config["curate"]["field_map"]),
        strain_regex=config["curate"]["strain_regex"],
        strain_backup_fields=config["curate"]["strain_backup_fields"],
        date_fields=config["curate"]["date_fields"],
        expected_date_formats=config["curate"]["expected_date_formats"],
        articles=config["curate"]["titlecase"]["articles"],
        abbreviations=config["curate"]["titlecase"]["abbreviations"],
        titlecase_fields=config["curate"]["titlecase"]["fields"],
        authors_field=config["curate"]["authors_field"],
        authors_default_value=config["curate"]["authors_default_value"],
        abbr_authors_field=config["curate"]["abbr_authors_field"],
        annotations_id=config["curate"]["annotations_id"],
        id_field=config["curate"]["output_id_field"],
        sequence_field=config["curate"]["output_sequence_field"],
    benchmark:
        "benchmarks/curate_ppx.txt"
    log:
        "logs/curate_ppx.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        cat {input.sequences_ndjson:q} \
            | augur curate rename \
                --field-map {params.field_map:q} \
            | augur curate normalize-strings \
            | augur curate transform-strain-name \
                --strain-regex {params.strain_regex:q} \
                --backup-fields {params.strain_backup_fields:q} \
            | augur curate format-dates \
                --date-fields {params.date_fields:q} \
                --expected-date-formats {params.expected_date_formats:q} \
            | augur curate titlecase \
                --titlecase-fields {params.titlecase_fields:q} \
                --articles {params.articles:q} \
                --abbreviations {params.abbreviations:q} \
            | augur curate abbreviate-authors \
                --authors-field {params.authors_field:q} \
                --default-value {params.authors_default_value:q} \
                --abbr-authors-field {params.abbr_authors_field:q} \
            | augur curate apply-geolocation-rules \
                --geolocation-rules {input.geolocation_rules:q} \
            | augur curate apply-record-annotations \
                --annotations {input.annotations:q} \
                --id-field {params.annotations_id:q} \
                --output-metadata {output.metadata:q} \
                --output-fasta {output.sequences:q} \
                --output-id-field {params.id_field:q} \
                --output-seq-field {params.sequence_field:q}
        """

rule curate_ncbi_entrez:
    input:
        metadata_ndjson="data/ncbi_entrez.ndjson",
    output:
        metadata="data/metadata_ncbi_entrez.tsv",
    benchmark:
        "benchmarks/curate_ncbi_entrez.txt"
    log:
        "logs/curate_ncbi_entrez.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        cat {input.metadata_ndjson:q} \
            | augur curate passthru \
                --output-metadata {output.metadata:q}
        """

# Goal: merge the two inputs, joining on 'ppx.insdcAccessionBase' and
# 'ncbi_entrez.accession'. Note that some ppx.insdcAccessionBase might be empty
# - just keep those rows as-is. For the rows that are not empty, update the
# 'strain' column based on this order of preference:
#
# 1. ncbi_entrez.strain
# 2. ncbi_entrez.isolate
# 3. ppx.strain
#
# Note: augur merge can't be used because some ppx sequences don't have
# insdcAccessionBase.
rule merge:
    input:
        metadata_ppx="data/metadata_ppx.tsv",
        metadata_ncbi_entrez="data/metadata_ncbi_entrez.tsv",
    output:
        metadata="data/metadata_merged.tsv",
    benchmark:
        "benchmarks/merge.txt"
    log:
        "logs/merge.txt"
    run:
        import pandas as pd

        # Read input files
        ppx = pd.read_csv(input.metadata_ppx, sep='\t')
        ncbi = pd.read_csv(input.metadata_ncbi_entrez, sep='\t')

        # Keep all ppx rows, including those with empty insdcAccessionBase
        merged = ppx.merge(ncbi, left_on='insdcAccessionBase', right_on='accession',
                           how='left', suffixes=('', '_ncbi'))

        # Apply strain preference hierarchy for rows that have a match
        def update_strain(row):
            # Apply preference: ncbi.strain > ncbi.isolate > ppx.strain
            if pd.notna(row['strain_ncbi']) and row['strain_ncbi'].strip():
                return row['strain_ncbi']
            elif pd.notna(row.get('isolate')) and row['isolate'].strip():
                return row['isolate']
            else:
                return row['strain']

        merged['strain'] = merged.apply(update_strain, axis=1)

        # Remove temporary columns from the merge
        merged = merged.drop(columns=['strain_ncbi', 'isolate', 'accession_ncbi'])

        # Save the merged metadata
        merged.to_csv(output.metadata, sep='\t', index=False)


rule extract_from_strain:
    input:
        metadata="data/metadata_merged.tsv",
    output:
        metadata="data/all_metadata.tsv",
    benchmark:
        "benchmarks/extract_from_strain.txt"
    log:
        "logs/extract_from_strain.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur curate passthru \
            --metadata {input.metadata:q} \
            | scripts/extract_from_strain \
            | augur curate passthru \
              --output-metadata {output.metadata:q}
        """

rule add_metadata_columns:
    """Add columns to metadata
    Notable columns:
    - url: URL linking to the NCBI GenBank record ('https://www.ncbi.nlm.nih.gov/nuccore/*').
    """
    input:
        metadata = "data/all_metadata.tsv"
    output:
        metadata = temp("data/all_metadata_added.tsv")
    params:
        pathoplexus_accession=config['curate']['pathoplexus_accession'],
        pathoplexus_accession_url=config['curate']['pathoplexus_accession'] + "__url",
        insdc_accession=config['curate']['insdc_accession'],
        insdc_accession_url=config['curate']['insdc_accession'] + "__url",
    benchmark:
        "benchmarks/add_metadata_columns.txt"
    log:
        "logs/add_metadata_columns.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        cat {input.metadata:q} \
            | csvtk mutate2 -t \
                -n {params.pathoplexus_accession_url:q} \
                -e '"https://pathoplexus.org/seq/" + ${params.pathoplexus_accession:q}' \
            | csvtk mutate2 -t \
                -n {params.insdc_accession_url:q} \
                -e '"https://www.ncbi.nlm.nih.gov/nuccore/" + ${params.insdc_accession:q}' \
        > {output.metadata:q}
        """

rule subset_metadata:
    input:
        metadata="data/all_metadata_added.tsv",
    output:
        subset_metadata="data/subset_metadata.tsv",
    params:
        metadata_fields=",".join(config["curate"]["metadata_columns"]),
    benchmark:
        "benchmarks/subset_metadata.txt"
    log:
        "logs/subset_metadata.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        csvtk cut -t -f {params.metadata_fields:q} \
            {input.metadata:q} > {output.subset_metadata:q}
        """

rule extract_open_data:
    input:
        metadata = "results/metadata.tsv",
        sequences = "results/sequences.fasta"
    output:
        metadata = "results/metadata_open.tsv",
        sequences = "results/sequences_open.fasta"
    benchmark:
        "benchmarks/extract_open_data.txt"
    log:
        "logs/extract_open_data.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur filter \
            --metadata {input.metadata:q} \
            --sequences {input.sequences:q} \
            --metadata-id-columns accession \
            --exclude-where "dataUseTerms=RESTRICTED" \
            --output-metadata {output.metadata:q} \
            --output-sequences {output.sequences:q}
        """
