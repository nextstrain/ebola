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

This workflow produces a single analysis of the West Africa outbreak.

The workflow can be run from the top level pathogen repo directory:
```
nextstrain build phylogenetic --snakefile outbreak-specific/Snakefile
```

The resulting dataset can be viewed in a web browser:
```
nextstrain view phylogenetic/outbreak-specific
```

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

**Input data**

By default the workflow requires locally ingested data.
You can our canonical data from s3 by adding `--config s3=True` to your command.


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
