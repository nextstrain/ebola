rule copy_example_data:
    input:
        sequences="example_data/sequences.fasta",
        metadata="example_data/metadata.tsv",
    output:
        sequences="data/sequences.fasta",
        metadata="data/metadata.tsv",
    benchmark:
        "benchmarks/copy_example_data.txt",
    log:
        "logs/copy_example_data.txt",
    shell:
        r"""
        exec &> >(tee {log:q})

        cp -f {input.sequences:q} {output.sequences:q}
        cp -f {input.metadata:q} {output.metadata:q}
        """

# Add a Snakemake ruleorder directive here if you need to resolve ambiguous rules
# that have the same output as the copy_example_data rule.

# ruleorder: copy_example_data > ...
