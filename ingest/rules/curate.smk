"""
This part of the workflow handles the curation of data from Pathoplexus

REQUIRED INPUTS:

    data/sequences.ndjson
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
        sequences_ndjson="data/sequences.ndjson",
        annotations=config["curate"]["annotations"],
    output:
        metadata="data/metadata_ppx.tsv",
        sequences="results/sequences.fasta",
    params:
        field_map=format_field_map(config["curate"]["field_map"]),
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

# Note: augur merge can't be used because some ppx sequences don't have
# insdcAccessionBase.
rule spike_in_strain_from_ncbi:
    input:
        metadata_ppx="data/metadata_ppx.tsv",
        metadata_ncbi_entrez="data/metadata_ncbi_entrez.tsv",
    output:
        metadata="data/metadata_merged.tsv",
    benchmark:
        "benchmarks/spike_in_strain_from_ncbi.txt"
    log:
        "logs/spike_in_strain_from_ncbi.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        scripts/spike_in_strain_from_ncbi.py \
            --metadata-ppx {input.metadata_ppx:q} \
            --metadata-ncbi-entrez {input.metadata_ncbi_entrez:q} \
            --output {output.metadata:q}
        """


rule extract_date_from_strain:
    input:
        metadata="data/metadata_merged.tsv",
    output:
        metadata="data/metadata_date_improvements.tsv",
    benchmark:
        "benchmarks/extract_date_from_strain.txt"
    log:
        "logs/extract_date_from_strain.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur curate passthru \
            --metadata {input.metadata:q} \
            | scripts/extract_from_strain.py \
            | augur curate passthru \
              --output-metadata {output.metadata:q}
        """


rule curate_geography:
    input:
        metadata="data/metadata_date_improvements.tsv",
        geolocation_rules=config["curate_geography"]["local_geolocation_rules"],
        annotations=config["curate_geography"]["annotations"],
    output:
        metadata="data/metadata_geo_improvements.tsv",
    params:
        id_column=config["curate_geography"]["id_column"],
        annotations_id=config["curate_geography"]["annotations_id"],
    benchmark:
        "benchmarks/curate_geography.txt"
    log:
        "logs/curate_geography.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur curate apply-geolocation-rules \
                --metadata {input.metadata:q} \
                --id-column {params.id_column:q} \
                --geolocation-rules {input.geolocation_rules:q} \
            | augur curate apply-record-annotations \
                --annotations {input.annotations:q} \
                --id-field {params.annotations_id:q} \
                --output-metadata {output.metadata:q}
        """


rule add_accession_urls:
    """Add columns to metadata
    Notable columns:
    - url: URL linking to the NCBI GenBank record ('https://www.ncbi.nlm.nih.gov/nuccore/*').
    """
    input:
        metadata = "data/metadata_geo_improvements.tsv",
    output:
        metadata = temp("data/all_metadata_added.tsv")
    params:
        pathoplexus_accession=config['curate']['pathoplexus_accession'],
        pathoplexus_accession_url=config['curate']['pathoplexus_accession'] + "__url",
        insdc_accession=config['curate']['insdc_accession'],
        insdc_accession_url=config['curate']['insdc_accession'] + "__url",
    benchmark:
        "benchmarks/add_accession_urls.txt"
    log:
        "logs/add_accession_urls.txt"
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
