
def _gather_inputs(species):
    """Inputs ('inputs' + 'additional_inputs') are validated and collected for
    each species independently
    """

    # Note: some basic checking of the expected structures done ahead-of-time in validate_config
    all_inputs = [i for i in [*config['inputs'], *config.get('additional_inputs', [])] if i['species']==species]
    
    if len(all_inputs)==0:
        raise InvalidConfigError("Config must define at least one element in config.inputs or config.additional_inputs lists"
            f"for species {species!r}, as this species was part of the config-specified builds")
    if len({i['name'] for i in all_inputs})!=len(all_inputs):
        raise InvalidConfigError(f"Names of inputs (config.inputs and config.additional_inputs) must be unique for species {species!r}")
    if not all(['name' in i and ('sequences' in i or 'metadata' in i) for i in all_inputs]):
        raise InvalidConfigError(f"Each input (config.inputs and config.additional_inputs) must have a 'name', and 'metadata' and/or 'sequences' for species {species!r}")
    if not any(['metadata' in i for i in all_inputs]):
        raise InvalidConfigError(f"At least one input must have 'metadata' for species {species!r}")
    if not any (['sequences' in i for i in all_inputs]):
        raise InvalidConfigError(f"At least one input must have 'sequences' for species {species!r}")

    available_keys = set(['name', 'species', 'metadata', 'sequences'])
    if any([len(set(el.keys())-available_keys)>0 for el in all_inputs]):
        raise InvalidConfigError(f"Each input (config.inputs and config.additional_inputs) can only include keys of {', '.join(available_keys)}")

    return {el['name']: {k:(v if k in ['name', 'species'] else path_or_url(v)) for k,v in el.items()} for el in all_inputs}


def _named_metadata_files(wildcards):
    inputs = _gather_inputs(wildcards.species)
    return [(name, info['metadata']) for name, info in inputs.items() if info.get('metadata')]

def _named_sequence_files(wildcards):
    inputs = _gather_inputs(wildcards.species)
    return [(name, info['sequences']) for name, info in inputs.items() if info.get('sequences')]


rule gather_metadata:
    """Produce a canonical (per-species) metadata table from a single input or multiple inputs"""
    input:
        lambda w: [meta for _name, meta in _named_metadata_files(w)],
    params:
        n = lambda w, input: len(input),
        pairs = lambda w: [f"{name}={meta}" for name, meta in _named_metadata_files(w)],
        id_field = config['strain_id_field'],
    output:
        metadata = "results/{species}/metadata.tsv"
    benchmark:
        "benchmarks/{species}/gather_metadata.txt"
    log:
        "logs/{species}/gather_metadata.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        if [[ {params.n} -eq 1 ]]; then
            augur read-file {input:q} > {output.metadata:q}
        else
            augur merge --metadata {params.pairs:q} \
                --metadata-id-columns {params.id_field:q} \
                --output-metadata {output.metadata:q}
        fi
        """

rule gather_sequences:
    """Produce a canonical (per-species) set of sequences from a single input or multiple inputs"""
    input:
        lambda w: [seqs for _name, seqs in _named_sequence_files(w)],
    params:
        n = lambda w, input: len(input),
        id_field = config['strain_id_field'],
    output:
        sequences = "results/{species}/sequences.fasta"
    benchmark:
        "benchmarks/{species}/gather_sequences.txt"
    log:
        "logs/{species}/gather_sequences.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        if [[ {params.n} -eq 1 ]]; then
            augur read-file {input:q} > {output.sequences:q}
        else
            augur merge --sequences {input:q} \
                --output-sequences {output.sequences:q}
        fi
        """
