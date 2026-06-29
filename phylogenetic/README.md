# Phylogenetic

The main `Snakefile` is a workflow which produces a number of phylogenetic analyses of Ebolaviruses.

The default configuration (`defaults/config.yaml`) will use open Pathoplexus data which our ingest workflow has curated and produce the following datasets:

   - **ebov/all-outbreaks** subsamples genomes across outbreaks to present an overview of the known genomic history of Ebola virus (EBOV), formerly _Zaïre ebolavirus_. Outbreaks are classified using Nextclade.
   This dataset is kept up-to-date on [nextstrain.org/ebola/all-outbreaks](https://nextstrain.org/ebola/all-outbreaks)

   - **ebov/west-africa-2014** a work-in-progress workflow which produces a single analysis of the West Africa outbreak. Note: this is NOT the workflow which produced [nextstrain.org/ebola/ebov-2013](https://nextstrain.org/ebola/ebov-2013)

   - **bdbv/all-outbreaks** This dataset is kept up-to-date on [nextstrain.org/ebola/bdbv](https://nextstrain.org/ebola/bdbv)

   - **bdbv/2026** work-in-progress

   - **sudv/all-outbreaks** This dataset is kept up-to-date on [nextstrain.org/ebola/sudv](https://nextstrain.org/ebola/sudv)


## TL;DR

Run a local ingest (see [ingest/README.md](../ingest/README.md)) then:

```
cd phylogenetic
snakemake --cores 4 --configfile defaults/config-local-inputs.yaml -pf
```


## How to run

To run the workflow from the source repo, from the phylogenetic directory either run `snakemake` or `nextstrain build .`

To run from an external analysis directory run (one of):

```
snakemake --snakefile path/to/ebola/phylogenetic/Snakefile
nextstrain run ebola phylogenetic .
nextstrain run path/to/ebola phylogenetic .
```


## Using Pathoplexus restricted data

If you are running within this repo and have run the ingest workflow you can add `--configfile defaults/config-local-inputs.yaml` to source all input data from `../ingest`. This is the best approach for development or local runs.

For external analysis directories, you can create a `config.yaml` config overlay which defines inputs: 
- To use locally ingested data, use [these inputs](defaults/config-local-inputs.yaml) with paths adjusted to be relative to your analysis directory
- To use S3 inputs, use the `additional_inputs` from [this config](build-configs/nextstrain-automation/config.yaml)

> If you are using RESTRICTED data in your own analysis, please refer to the [Pathoplexus Data Use Terms](https://pathoplexus.org/about/terms-of-use/restricted-data)


## Using additional inputs (private data)

> This is a work in progress and needs further testing 

The following steps assume you are running from your own analysis directory (see "How to run", above).

1. Provision your own sequences and metadata files. The 'accession' column of the metadata TSV is used as the unique ID and must match the headers in your FASTA file. You should also ensure the 'date' column (in YYYY-MM-DD format) is present, as well as other useful columns such as 'country', 'division' as needed by the build.
2. Create a custom config overlay `config.yaml` which describes the additional inputs:

```yaml
additional_inputs:
  - name: private_data
    species: ebov               # options: "ebov", "bdbv", "sudv"
    metadata: metadata.tsv      # your metadata filename
    sequences: sequences.fasta  # your sequences filename
```
 You may also wish to copy in the `additional_inputs` from the "Using Pathoplexus restricted data" section (above), as the default workflow will not source restricted data.





## Build configs

The build-configs directory contains custom configs and rules that override and/or
extend the default workflows.

- [ci](build-configs/ci/) - CI build that runs with example data
- [nextstrain-automation](build-configs/nextstrain-automation/) - Rebuilds and uploads certain datasets

[Nextstrain datasets]: https://docs.nextstrain.org/en/latest/reference/glossary.html#term-dataset
