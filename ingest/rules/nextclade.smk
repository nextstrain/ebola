

# NOTE: The dataset name/address is currently hardcoded here and points to the ebola branch
# (see https://github.com/nextstrain/nextclade_data/pull/184)
# This URL is expected to break once that PR is merged / the branch is deleted. At that point we
# should update the invocation and use config variables accordingly.
rule get_nextclade_dataset:
    """Download Nextclade dataset"""
    output:
        dataset=f"data/ebola-zaire.zip",
    benchmark:
        "benchmarks/get_nextclade_dataset.txt"
    log:
        "logs/get_nextclade_dataset.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        nextclade3 dataset get \
            --server https://raw.githubusercontent.com/nextstrain/nextclade_data/refs/heads/ebola/data_output/ \
            --name nextstrain/ebola/zaire \
            --output-zip {output.dataset:q}
        """

rule run_nextclade:
    input:
        sequences="results/sequences.fasta",
        dataset="data/ebola-zaire.zip",
    output:
        metadata="data/nextclade.tsv",
        alignment="results/alignment.fasta",
        translations="results/translations.zip",
    params:
        # The lambda is used to deactivate automatic wildcard expansion.
        # https://github.com/snakemake/snakemake/blob/384d0066c512b0429719085f2cf886fdb97fd80a/snakemake/rules.py#L997-L1000
        translations=lambda w: "results/translations/{cds}.fasta",
    benchmark:
        "benchmarks/run_nextclade.txt"
    log:
        "logs/run_nextclade.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        nextclade3 run \
            {input.sequences:q} \
            --input-dataset {input.dataset:q} \
            --output-tsv {output.metadata:q} \
            --output-fasta {output.alignment:q} \
            --output-translations {params.translations:q}

        zip -rj {output.translations:q} results/translations
        """
