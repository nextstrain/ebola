"""
This part of the workflow handles the curation of data from Pathoplexus
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
        sequences_ndjson="data/{species}/sequences.ndjson",
        annotations=config["curate"]["annotations"],
    output:
        metadata="data/{species}/metadata_ppx.tsv",
        sequences="data/{species}/sequences.fasta",
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
        "benchmarks/{species}/curate_ppx.txt"
    log:
        "logs/{species}/curate_ppx.txt"
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
        metadata_ndjson="data/{species}/ncbi_entrez.ndjson",
    output:
        metadata="data/{species}/metadata_ncbi_entrez.tsv",
    benchmark:
        "benchmarks/{species}/curate_ncbi_entrez.txt"
    log:
        "logs/{species}/curate_ncbi_entrez.txt"
    wildcard_constraints:
        species='ebov'
    shell:
        r"""
        exec &> >(tee {log:q})

        cat {input.metadata_ndjson:q} \
            | augur curate passthru \
                --output-metadata {output.metadata:q}
        """

# Note: augur merge can't be used because some ppx sequences don't have
# insdcAccessionBase.
rule spike_in_ncbi_data:
    input:
        metadata_ppx="data/{species}/metadata_ppx.tsv",
        metadata_ncbi_entrez="data/{species}/metadata_ncbi_entrez.tsv",
    output:
        metadata="data/{species}/metadata_ppx-ncbi.tsv",
    params:
        fields=["title", "note"]
    benchmark:
        "benchmarks/{species}/spike_in_ncbi_data.txt"
    log:
        "logs/{species}/spike_in_ncbi_data.txt"
    wildcard_constraints:
        species='ebov'
    shell:
        r"""
        exec &> >(tee {log:q})

        scripts/spike_in_ncbi_data.py \
            --metadata-ppx {input.metadata_ppx:q} \
            --metadata-ncbi-entrez {input.metadata_ncbi_entrez:q} \
            --add-fields {params.fields:q} \
            --output {output.metadata:q}
        """


rule spike_in_inrb_metadata:
    input:
        metadata="data/{species}/metadata_ppx-ncbi.tsv",
        nord_kivu_metadata="data/{species}/inrb-drc-nord-kivu-metadata.tsv",
    output:
        metadata="data/{species}/metadata_ppx-ncbi-inrb.tsv",
    benchmark:
        "benchmarks/{species}/spike_in_inrb_metadata.txt"
    log:
        "logs/{species}/spike_in_inrb_metadata.txt"
    wildcard_constraints:
        species='ebov'
    shell:
        r"""
        exec &> >(tee {log:q})

        scripts/cross_reference_inrb.py \
            --metadata {input.metadata:q} \
            --nord-kivu-metadata {input.nord_kivu_metadata:q} \
            --output {output.metadata:q}
        """

rule spike_in_fauna_metadata:
    input:
        metadata="data/{species}/metadata_ppx-ncbi-inrb.tsv",
        fauna_metadata="defaults/west-africa-2013-metadata.tsv",
    output:
        metadata="data/{species}/metadata_ppx-ncbi-inrb-fauna.tsv",
    benchmark:
        "benchmarks/{species}/spike_in_fauna_metadata.txt"
    log:
        "logs/{species}/spike_in_fauna_metadata.txt"
    wildcard_constraints:
        species='ebov'
    shell:
        r"""
        exec &> >(tee {log:q})

        scripts/cross_reference_fauna.py \
            --metadata {input.metadata:q} \
            --fauna {input.fauna_metadata:q} \
            --output {output.metadata:q}
        """


def get_base_metadata(wildcards):
    # Zaire has a bunch of extra sources spiked in
    if wildcards.species == 'ebov':
        return "data/ebov/metadata_ppx-ncbi-inrb-fauna.tsv"
    return f"data/{wildcards.species}/metadata_ppx.tsv"


rule extract_date_from_strain:
    input:
        metadata=get_base_metadata,
    output:
        metadata="data/{species}/metadata_date-improvements.tsv",
    benchmark:
        "benchmarks/{species}/extract_date_from_strain.txt"
    log:
        "logs/{species}/extract_date_from_strain.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur curate passthru \
            --metadata {input.metadata:q} \
            | scripts/extract_from_strain.py \
            | augur curate passthru \
              --output-metadata {output.metadata:q}
        """

rule lab_hosts:
    """Mark strains as is_lab_host=True via metadata matching"""
    input:
        metadata = "data/{species}/metadata_date-improvements.tsv",
    output:
        metadata="data/{species}/metadata_lab-host-improvements.tsv",
    benchmark:
        "benchmarks/{species}/lab_hosts.txt"
    log:
        "logs/{species}/lab_hosts.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        scripts/lab_hosts.py \
            --metadata {input.metadata:q} \
            --output {output.metadata:q}
        """


rule curate_geography:
    input:
        metadata="data/{species}/metadata_lab-host-improvements.tsv",
        geolocation_rules=config["curate_geography"]["local_geolocation_rules"],
        annotations=config["curate_geography"]["annotations"],
    output:
        metadata="data/{species}/metadata_geo-improvements.tsv",
    params:
        id_column=config["curate_geography"]["id_column"],
        annotations_id=config["curate_geography"]["annotations_id"],
    benchmark:
        "benchmarks/{species}/curate_geography.txt"
    log:
        "logs/{species}/curate_geography.txt"
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
        metadata = "data/{species}/metadata_geo-improvements.tsv",
    output:
        metadata = "data/{species}/metadata_acessions.tsv",
    params:
        pathoplexus_accession=config['curate']['pathoplexus_accession'],
        pathoplexus_accession_url=config['curate']['pathoplexus_accession'] + "__url",
        insdc_accession=config['curate']['insdc_accession'],
        insdc_accession_url=config['curate']['insdc_accession'] + "__url",
    benchmark:
        "benchmarks/{species}/add_accession_urls.txt"
    log:
        "logs/{species}/add_accession_urls.txt"
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
        metadata="data/{species}/metadata_acessions.tsv",
    output:
        subset_metadata="data/{species}/metadata.tsv",
    params:
        metadata_columns=",".join(config["curate"]["metadata_columns"]),
    benchmark:
        "benchmarks/{species}/subset_metadata.txt"
    log:
        "logs/{species}/subset_metadata.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        csvtk cut -t -f {params.metadata_columns:q} \
            {input.metadata:q} > {output.subset_metadata:q}
        """

rule subset_to_open_data:
    input:
        metadata = "data/{species}/metadata.tsv",
        sequences = "data/{species}/sequences.fasta",
    output:
        metadata = "results/{species}/metadata_open.tsv",
        sequences = "results/{species}/sequences_open.fasta",
    benchmark:
        "benchmarks/{species}/subset_to_open_data.txt"
    log:
        "logs/{species}/subset_to_open_data.txt"
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


rule subset_to_restricted_data:
    input:
        metadata = "data/{species}/metadata.tsv",
        sequences = "data/{species}/sequences.fasta",
    output:
        metadata = "results/{species}/metadata_restricted.tsv",
        sequences = "results/{species}/sequences_restricted.fasta",
    benchmark:
        "benchmarks/{species}/subset_to_restricted_data.txt"
    log:
        "logs/{species}/subset_to_restricted_data.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        # Some species have no RESTRICTED records at all, in which case augur
        # filter would error ("All samples have been dropped!"). Check for the
        # presence of any RESTRICTED record first, and only run augur filter
        # when there's something to extract; otherwise write header-only
        # metadata and an empty FASTA so downstream rules see valid files.
        if csvtk cut -t -f dataUseTerms {input.metadata:q} | grep -qw RESTRICTED; then
            augur filter \
                --metadata {input.metadata:q} \
                --sequences {input.sequences:q} \
                --metadata-id-columns accession \
                --exclude-where "dataUseTerms!=RESTRICTED" \
                --output-metadata {output.metadata:q} \
                --output-sequences {output.sequences:q}
        else
            echo "No RESTRICTED records found; writing empty outputs."
            head -n 1 {input.metadata:q} > {output.metadata:q}
            : > {output.sequences:q}
        fi
        """
