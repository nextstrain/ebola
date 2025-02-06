rule copy_example_data:
    input:
        sequences="example_data/sequences.fasta",
        metadata="example_data/metadata.tsv",
    output:
        sequences="data/sequences.fasta",
        metadata="data/metadata.tsv",
    shell:
        """
        cp -f {input.sequences} {output.sequences}
        cp -f {input.metadata} {output.metadata}
        """

# Add a Snakemake ruleorder directive here if you need to resolve ambiguous rules
# that have the same output as the copy_example_data rule.

# ruleorder: copy_example_data > ...
