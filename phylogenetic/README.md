# Phylogenetic

This workflow uses metadata and sequences to produce one or multiple [Nextstrain datasets][]
that can be visualized in Auspice.

## Workflow Usage

The workflow can be run from the top level pathogen repo directory:
```
nextstrain build phylogenetic
```

Alternatively, the workflow can also be run from within the phylogenetic directory:
```
cd phylogenetic
nextstrain build .
```

This produces the default outputs of the phylogenetic workflow:

- auspice_json(s) = auspice/*.json

## Data Requirements

The core phylogenetic workflow will use metadata values as-is, so please do any
desired data formatting and curations as part of the [ingest](../ingest/) workflow.

1. The metadata must include an ID column that can be used as as exact match for
   the sequence ID present in the FASTA headers.
2. The `date` column in the metadata must be in ISO 8601 date format (i.e. YYYY-MM-DD).
3. Ambiguous dates should be masked with `XX` (e.g. 2023-01-XX).

## Defaults

The defaults directory contains all of the default configurations for the phylogenetic workflow.

[defaults/config.yaml](defaults/config.yaml) contains all of the default configuration parameters
used for the phylogenetic workflow. Use Snakemake's `--configfile`/`--config`
options to override these default values.

## Snakefile and rules

The rules directory contains separate Snakefiles (`*.smk`) as modules of the core phylogenetic workflow.
The modules of the workflow are in separate files to keep the main phylogenetic [Snakefile](Snakefile) succinct and organized.

The `workdir` is hardcoded to be the phylogenetic directory so all filepaths for
inputs/outputs should be relative to the phylogenetic directory.

Modules are all [included](https://snakemake.readthedocs.io/en/stable/snakefiles/modularization.html#includes)
in the main Snakefile in the order that they are expected to run.

## Build configs

The build-configs directory contains custom configs and rules that override and/or
extend the default workflow.

- [ci](build-configs/ci/) - CI build that runs with example data

[Nextstrain datasets]: https://docs.nextstrain.org/en/latest/reference/glossary.html#term-dataset
