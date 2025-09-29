# Phylogenetic

This workflow uses metadata and sequences to produce one or multiple [Nextstrain datasets][]
that can be visualized in Auspice.

There are currently two separate workflows:
* The outbreak-specific workflow (entrypoint: `./outbreak-specific/Snakefile`) which generates analyses for individual outbreaks
* The all-outbreaks workflow (entrypoint: `./all-outbreaks/Snakefile`)

## Workflows

> Currently the workflows use starting data from a local ingest run, so there must be
> files in `../ingest/results/`

### Outbreak-specific

This workflow produces analyses of specific ebola outbreaks. Configure which outbreaks in [outbreak-specific/config.yaml](./outbreak-specific/config.yaml).

The workflow can be run from the top level pathogen repo directory:
```
nextstrain build phylogenetic --snakefile outbreak-specific/Snakefile
```

The resulting datasets can be viewed in a web browser:
```
nextstrain view phylogenetic/outbreak-specific
```

There are currently two datasets configured, and each is a work in progress.

#### Ebov-2013 / West-Africa 2014

* The temporal analysis is wrong (root pushed back), which I think is due to the inclusion of relapse cases. We could try specifying a rate or explore ways to infer the rate based on a subset of samples within treetime. 
* Missing colors & lat-longs. Some of this is misspelt geographical metadata to be fixed in ingest.
* Alignment (mafft) takes a long time cf. nextclade alignment. We should compare the two approaches.

#### Ebov-2025

* Currently (2025-09-29) there are 4 available genomes. At this time [this virological post](https://virological.org/t/the-16th-ebola-virus-disease-outbreak-in-bulape-health-zone-kasai-democratic-republic-of-the-congo-a-new-spillover-event-from-an-unknown-reservoir-host/1003) is a better summary of the genomic situation.


### All-outbreaks

This workflow subsamples genomes across outbreaks to present an overview of the known genomic history
of Ebola virus (EBOV), formerly _Zaïre ebolavirus_. Outbreaks are classified using Nextclade as part
of the ingest workflow. The rooting of the tree is chosen to match [McCrone et al., Virological (2025)](https://virological.org/t/on-the-rooting-of-the-ebola-virus-phylogeny-and-its-consequences-for-understanding-the-diversity-in-the-reservoir/1005)
and the phylogeny is currently divergence-only.

The workflow can be run from the top level pathogen repo directory:
```
nextstrain build phylogenetic --snakefile all-outbreaks/Snakefile
```

The resulting dataset can be viewed in a web browser:
```
nextstrain view phylogenetic
```

## Data Requirements

The phylogenetic workflows will use metadata values as-is, so please do any
desired data formatting and curations as part of the [ingest](../ingest/) workflow.

1. The metadata must include an ID column that can be used as as exact match for
   the sequence ID present in the FASTA headers.
2. The `date` column in the metadata must be in ISO 8601 date format (i.e. YYYY-MM-DD).
3. Ambiguous dates should be masked with `XX` (e.g. 2023-01-XX).

## Snakefile and rules

> [!NOTE]
> This section only applies to the outbreak-specific workflow and not the all-outbreaks workflow.

The rules directory contains separate Snakefiles (`*.smk`) as modules of the core phylogenetic workflow.
The modules of the workflow are in separate files to keep the main phylogenetic [Snakefile](Snakefile) succinct and organized.

The `workdir` is hardcoded to be the phylogenetic directory so all filepaths for
inputs/outputs should be relative to the phylogenetic directory.

Modules are all [included](https://snakemake.readthedocs.io/en/stable/snakefiles/modularization.html#includes)
in the main Snakefile in the order that they are expected to run.

## Build configs

The build-configs directory contains custom configs and rules that override and/or
extend the default workflows.

- [ci](build-configs/ci/) - CI build that runs with example data

[Nextstrain datasets]: https://docs.nextstrain.org/en/latest/reference/glossary.html#term-dataset
