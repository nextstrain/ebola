

rule get_nextclade_dataset:
    """Download Nextclade dataset"""
    output:
        dataset="data/{species}/nextclade-dataset.zip",
    benchmark:
        "benchmarks/{species}/get_nextclade_dataset.txt"
    log:
        "logs/{species}/get_nextclade_dataset.txt"
    wildcard_constraints:
        species='ebov'
    shell:
        r"""
        exec &> >(tee {log:q})

        nextclade3 dataset get \
            --name nextstrain/ebola/zaire \
            --output-zip {output.dataset:q}
        """

rule run_nextclade:
    input:
        sequences="results/{species}/sequences.fasta",
        dataset="data/{species}/nextclade-dataset.zip",
    output:
        metadata="data/{species}/nextclade.tsv",
        alignment="results/{species}/alignment.fasta",
        translations="results/{species}/translations.zip",
    params:
        # The lambda is used to deactivate automatic wildcard expansion.
        # https://github.com/snakemake/snakemake/blob/384d0066c512b0429719085f2cf886fdb97fd80a/snakemake/rules.py#L997-L1000
        translations=lambda w: f"results/{w.species}/translations/{{cds}}.fasta",
        translations_dir = "results/{species}/translations/"
    benchmark:
        "benchmarks/{species}/run_nextclade.txt"
    log:
        "logs/{species}/run_nextclade.txt"
    wildcard_constraints:
        species='ebov'
    shell:
        r"""
        exec &> >(tee {log:q})

        nextclade3 run \
            {input.sequences:q} \
            --input-dataset {input.dataset:q} \
            --output-tsv {output.metadata:q} \
            --output-fasta {output.alignment:q} \
            --output-translations {params.translations:q}

        zip -rj {output.translations:q} {params.translations_dir:q}
        """
