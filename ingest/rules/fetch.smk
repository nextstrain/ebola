"""
This part of the workflow handles fetching sequences and metadata from NCBI and Pathoplexus.

REQUIRED INPUTS:

    None

OUTPUTS:

    ndjson = data/ncbi.ndjson

There are two different approaches for fetching data from NCBI.
Choose the one that works best for the pathogen data and edit the workflow config
to provide the correct parameter.

1. Fetch with NCBI Datasets (https://www.ncbi.nlm.nih.gov/datasets/)
    - requires `ncbi_taxon_id` config
    - Directly returns NDJSON without custom parsing
    - Fastest option for large datasets (e.g. SARS-CoV-2)
    - Only returns metadata fields that are available through NCBI Datasets
    - Only works for viral genomes

2. In some cases, it may be necessary or desirable to incorporate some data that
   is only available via Entrez (https://www.ncbi.nlm.nih.gov/books/NBK25501/)
   and merge it with the NCBI Datasets data.
    - requires `entrez_search_term` config
    - Returns all available data via a GenBank file
    - Requires a custom script to parse the necessary fields from the GenBank file

   Specific details of how this will work will depend on exactly what additional
   data is being extracted from Entrez. In Nextstrain pathogen repos, we have
   used various approaches, such as custom Python scripts that use BioPython to
   parse GenBank records,¹ or using the `bio json` tool in combination with `jq`
   to convert GenBank records to a JSON format and then extract particular fields.²
   In both cases, the additional metadata is combined with the NCBI Datasets
   metadata.tsv file using `augur merge`.³

   ¹ https://github.com/nextstrain/rubella/blob/6492af11bc0a4c7fcf4cfd8d30a94dcc145cf2ed/ingest/rules/fetch_from_ncbi.smk#L104-L120
   ² https://github.com/nextstrain/mumps/blob/417a69d9a28dca35165173db88ac8bd99f78028a/ingest/rules/fetch_from_ncbi.smk#L151-L164
   ³ https://github.com/nextstrain/rubella/blob/6492af11bc0a4c7fcf4cfd8d30a94dcc145cf2ed/ingest/rules/fetch_from_ncbi.smk#L123-L143
"""

# FIXME: This entire section is currently unusable due to a broken code path;
# see comment on config.sources
###########################################################################
####################### 1. Fetch from NCBI Datasets #######################
###########################################################################


rule fetch_ncbi_dataset_package:
    params:
        ncbi_taxon_id=config["ncbi_taxon_id"],
    output:
        dataset_package=temp("data/ncbi_dataset.zip"),
    # Allow retries in case of network errors
    retries: 5
    benchmark:
        "benchmarks/fetch_ncbi_dataset_package.txt"
    log:
        "logs/fetch_ncbi_dataset_package.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        datasets download virus genome taxon {params.ncbi_taxon_id:q} \
            --no-progressbar \
            --filename {output.dataset_package:q}
        """

# Note: This rule is not part of the default workflow!
# It is intended to be used as a specific target for users to be able
# to inspect and explore the full raw metadata from NCBI Datasets.
rule dump_ncbi_dataset_report:
    input:
        dataset_package="data/ncbi_dataset.zip",
    output:
        ncbi_dataset_tsv="data/ncbi_dataset_report_raw.tsv",
    benchmark:
        "benchmarks/dump_ncbi_dataset_report.txt"
    log:
        "logs/dump_ncbi_dataset_report.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        dataformat tsv virus-genome \
            --package {input.dataset_package:q} > {output.ncbi_dataset_tsv:q}
        """


rule extract_ncbi_dataset_sequences:
    input:
        dataset_package="data/ncbi_dataset.zip",
    output:
        ncbi_dataset_sequences=temp("data/ncbi_dataset_sequences.fasta"),
    benchmark:
        "benchmarks/extract_ncbi_dataset_sequences.txt"
    log:
        "logs/extract_ncbi_dataset_sequences.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        unzip -jp {input.dataset_package:q} \
            ncbi_dataset/data/genomic.fna > {output.ncbi_dataset_sequences:q}
        """


# FIXME: apply csvtk updates. can ncbi_field_map be replaced by ncbi_datasets_fields ?
rule format_ncbi_dataset_report:
    # Formats the headers to be the same as before we used NCBI Datasets
    # The only fields we do not have equivalents for are "title" and "publications"
    input:
        dataset_package="data/ncbi_dataset.zip",
        ncbi_field_map="defaults/ncbi-dataset-field-map.tsv",
    output:
        ncbi_dataset_tsv=temp("data/ncbi_dataset_report.tsv"),
    params:
        ncbi_datasets_fields=",".join(config["ncbi_datasets_fields"]),
    benchmark:
        "benchmarks/format_ncbi_dataset_report.txt"
    log:
        "logs/format_ncbi_dataset_report.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        dataformat tsv virus-genome \
            --package {input.dataset_package:q} \
            --fields {params.ncbi_datasets_fields:q} \
            | csvtk -tl rename2 -F -f '*' -p '(.+)' -r '{{kv}}' -k {input.ncbi_field_map} \
            | csvtk -tl mutate -f genbank_accession_rev -n genbank_accession -p "^(.+?)\." \
            | tsv-select -H -f genbank_accession --rest last \
            > {output.ncbi_dataset_tsv:q}
        """


###########################################################################
########################## 2. Fetch from Entrez ###########################
###########################################################################


rule fetch_from_ncbi_entrez:
    params:
        term=config["entrez_search_term"],
    output:
        genbank="data/genbank.gb",
    # Allow retries in case of network errors
    retries: 5
    benchmark:
        "benchmarks/fetch_from_ncbi_entrez.txt"
    log:
        "logs/fetch_from_ncbi_entrez.txt",
    shell:
        r"""
        exec &> >(tee {log:q})

        {workflow.basedir}/../shared/vendored/scripts/fetch-from-ncbi-entrez \
            --term {params.term:q} \
            --output {output.genbank:q}
        """


# If you are using additional Entrez data, add additional rules here for parsing
# the Entrez results and merging with the ncbi_dataset_report.tsv
# Remember to edit the `ncbi_dataset_tsv` input below to use the new merged TSV.


###########################################################################
########################## 2.5. Fetch from Pathoplexus ####################
###########################################################################

rule download_ppx_seqs:
    output:
        sequences= "data/ppx_sequences.fasta",
    params:
        sequences_url=config["ppx_fetch"]["seqs"],
    benchmark:
        "benchmarks/download_ppx_seqs.txt"
    log:
        "logs/download_ppx_seqs.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        curl {params.sequences_url:q} -o {output.sequences:q}
        """

rule download_ppx_meta:
    output:
        metadata= "data/ppx_metadata.csv"
    params:
        metadata_url=config["ppx_fetch"]["meta"],
        fields = ",".join(config["ppx_metadata_fields"])
    benchmark:
        "benchmarks/download_ppx_meta.txt"
    log:
        "logs/download_ppx_meta.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        curl '{params.metadata_url}&fields={params.fields}' -o {output.metadata:q}
        """

rule format_ppx_ndjson:
    input:
        sequences="data/ppx_sequences.fasta",
        metadata="data/ppx_metadata.csv"
    output:
        ndjson="data/ppx.ndjson"
    benchmark:
        "benchmarks/format_ppx_ndjson.txt"
    log:
        "logs/format_ppx_ndjson.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur curate passthru \
            --metadata {input.metadata} \
            --fasta {input.sequences} \
            --seq-id-column accessionVersion \
            --seq-field sequence \
            --unmatched-reporting warn \
            --duplicate-reporting warn \
            > {output.ndjson}
        """


###########################################################################
###################### 3. Produce final output NDJSON #####################
###########################################################################

# Technically you can bypass this step and directly provide FASTA and TSV files
# as input files for the curate pipeline.
# We do the formatting here to have a uniform NDJSON file format for the raw
# data that we host on data.nextstrain.org
rule format_ncbi_datasets_ndjson:
    input:
        ncbi_dataset_sequences="data/ncbi_dataset_sequences.fasta",
        ncbi_dataset_tsv="data/ncbi_dataset_report.tsv",
    output:
        ndjson="data/ncbi.ndjson",
    benchmark:
        "benchmarks/format_ncbi_datasets_ndjson.txt"
    log:
        "logs/format_ncbi_datasets_ndjson.txt",
    shell:
        r"""
        exec &> >(tee {log:q})

        augur curate passthru \
            --metadata {input.ncbi_dataset_tsv:q} \
            --fasta {input.ncbi_dataset_sequences:q} \
            --seq-id-column genbank_accession_rev \
            --seq-field sequence \
            --unmatched-reporting warn \
            --duplicate-reporting warn \
            > {output.ndjson:q}
        """


# FIXME: This is just aliasing 'genbank' to 'ncbi'. Doesn't seem necessary.
# Remove?
rule fetch_all_genbank_sequences:
    input:
        ncbi_ndjson="data/ncbi.ndjson",
    output:
        sequences_ndjson="data/genbank.ndjson",
    shell:
        """
        cp {input.ncbi_ndjson} {output.sequences_ndjson}
        """


def _get_all_sources(wildcards):
    return [f"data/{source}.ndjson" for source in config["sources"]]


# FIXME: The code path for having multiple sources here is broken; see comment
# on config.sources. Even if it did work, does it even make sense? It would be
# combining the Pathoplexus and GenBank datasets by simple concatenation,
# which doesn't seem right.
rule fetch_all_sequences:
    input:
        all_sources=_get_all_sources,
    output:
        sequences_ndjson="data/sequences.ndjson",
    shell:
        """
        cat {input.all_sources} > {output.sequences_ndjson}
        """
