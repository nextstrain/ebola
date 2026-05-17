"""
This part of the workflow prepares sequences for constructing the phylogenetic tree.

REQUIRED INPUTS:

    metadata    = data/metadata.tsv
    sequences   = data/sequences.fasta
    reference   = ../shared/reference.fasta

OUTPUTS:

    prepared_sequences = results/prepared_sequences.fasta

This part of the workflow usually includes the following steps:

    - augur index
    - augur filter
    - augur align
    - augur mask

See Augur's usage docs for these commands for more details.
"""

# TODO: upload ingest results to data.nextstrain.org and download them here.
rule filter:
    """
    Filtering to
      - {params.sequences_per_group} sequence(s) per {params.group_by!s}
      - from {params.min_date} onwards
      - excluding strains in {input.exclude}
    """
    input:
        sequences = lambda w: path_or_url(config["inputs"][0]['sequences']),
        metadata = lambda w: path_or_url(config["inputs"][0]['metadata']),
        # include/exclude are optional and the input values are handled as params
        # (the `config_path` helper doesn't allow the config values to be optional)
        exclude = lambda w: config["build_params"][w.build]["filter"].get("exclude", []),
        include = lambda w: config["build_params"][w.build]["filter"].get("include", []),
    output:
        sequences = "results/{build}/filtered.fasta",
        metadata = "results/{build}/filtered.tsv",
        log = "results/{build}/filter-log.txt",
    params:
        id_column = config["id_column"],
        min_length = conditional_config("--min-length", "filter", "min_length"),
        min_date = conditional_config("--min-date", "filter", "min_date"),
        max_date = conditional_config("--max-date", "filter", "max_date"),
        exclude_ambiguous_dates_by = conditional_config("--exclude-ambiguous-dates-by", "filter", "exclude_ambiguous_dates_by"),
        exclude_where = conditional_config("--exclude-where", "filter", "exclude_where"),
        group_by = conditional_config("--group-by", "filter", "group_by"),
        subsample_max_sequences = conditional_config("--subsample-max-sequences", "filter", "subsample_max_sequences"),
        query = conditional_config("--query", "filter", "query"),
        exclude =  lambda w, input: conditional_arg("--exclude", input.exclude),
        include =  lambda w, input: conditional_arg("--include", input.include),
    benchmark:
        "benchmarks/{build}/filter.txt"
    log:
        "logs/{build}/filter.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur filter \
            --sequences {input.sequences:q} \
            --metadata {input.metadata:q} \
            --metadata-id-columns {params.id_column:q} \
            {params.min_length:q} \
            {params.min_date:q} \
            {params.max_date:q} \
            {params.exclude_ambiguous_dates_by:q} \
            {params.exclude_where:q} \
            {params.group_by:q} \
            {params.subsample_max_sequences:q} \
            {params.query:q} \
            {params.include:q} \
            {params.exclude:q} \
            --output-sequences {output.sequences:q} \
            --output-metadata {output.metadata:q} \
            --output-log {output.log:q}
        """

rule align:
    """
    Aligning sequences to {input.reference}
      - filling gaps with N
      - removing reference sequence
    """
    input:
        sequences = "results/{build}/filtered.fasta",
        reference = config_path("align", "reference"),
    output:
        alignment = "results/{build}/aligned.fasta"
    benchmark:
        "benchmarks/{build}/align.txt"
    log:
        "logs/{build}/align.txt"
    threads: 4
    shell:
        r"""
        exec &> >(tee {log:q})

        augur align \
            --sequences {input.sequences:q} \
            --reference-sequence {input.reference:q} \
            --output {output.alignment:q} \
            --fill-gaps \
            --remove-reference \
            --nthreads {threads:q}
        """
