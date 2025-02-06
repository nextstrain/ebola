"""
This part of the workflow handles running Nextclade on the curated metadata
and sequences.

REQUIRED INPUTS:

    metadata    = data/subset_metadata.tsv
    sequences   = results/sequences.fasta

OUTPUTS:

    metadata        = results/metadata.tsv
    nextclade       = results/nextclade.tsv
    alignment       = results/alignment.fasta
    translations    = results/translations.zip

See Nextclade docs for more details on usage, inputs, and outputs if you would
like to customize the rules:
https://docs.nextstrain.org/projects/nextclade/page/user/nextclade-cli.html
"""
DATASET_NAME = config["nextclade"]["dataset_name"]


rule get_nextclade_dataset:
    """Download Nextclade dataset"""
    output:
        dataset=f"data/nextclade_data/{DATASET_NAME}.zip",
    params:
        dataset_name=DATASET_NAME
    shell:
        """
        nextclade3 dataset get \
            --name={params.dataset_name:q} \
            --output-zip={output.dataset} \
            --verbose
        """


rule run_nextclade:
    input:
        dataset=f"data/nextclade_data/{DATASET_NAME}.zip",
        sequences="results/sequences.fasta",
    output:
        nextclade="results/nextclade.tsv",
        alignment="results/alignment.fasta",
        translations="results/translations.zip",
    params:
        # The lambda is used to deactivate automatic wildcard expansion.
        # https://github.com/snakemake/snakemake/blob/384d0066c512b0429719085f2cf886fdb97fd80a/snakemake/rules.py#L997-L1000
        translations=lambda w: "results/translations/{cds}.fasta",
    shell:
        """
        nextclade3 run \
            {input.sequences} \
            --input-dataset {input.dataset} \
            --output-tsv {output.nextclade} \
            --output-fasta {output.alignment} \
            --output-translations {params.translations}

        zip -rj {output.translations} results/translations
        """


rule nextclade_metadata:
    input:
        nextclade="results/nextclade.tsv",
    output:
        nextclade_metadata=temp("results/nextclade_metadata.tsv"),
    params:
        nextclade_id_field=config["nextclade"]["id_field"],
        nextclade_field_map=[f"{old}={new}" for old, new in config["nextclade"]["field_map"].items()],
        nextclade_fields=",".join(config["nextclade"]["field_map"].values()),
    shell:
        r"""
        augur curate rename \
            --metadata {input.nextclade:q} \
            --id-column {params.nextclade_id_field:q} \
            --field-map {params.nextclade_field_map:q} \
            --output-metadata - \
        | csvtk cut -t --fields {params.nextclade_fields:q} \
        > {output.nextclade_metadata:q}
        """


rule join_metadata_and_nextclade:
    input:
        metadata="data/subset_metadata.tsv",
        nextclade_metadata="results/nextclade_metadata.tsv",
    output:
        metadata="results/metadata.tsv",
    params:
        metadata_id_field=config["curate"]["output_id_field"],
        nextclade_id_field=config["nextclade"]["id_field"],
    shell:
        r"""
        augur merge \
            --metadata \
                metadata={input.metadata:q} \
                nextclade={input.nextclade_metadata:q} \
            --metadata-id-columns \
                metadata={params.metadata_id_field:q} \
                nextclade={params.nextclade_id_field:q} \
            --output-metadata {output.metadata:q} \
            --no-source-columns
        """
