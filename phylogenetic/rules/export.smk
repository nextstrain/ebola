
def _uses_sampling_year(wildcards):
    return bool(config['sampling_year_coloring'].get(f"{wildcards.species}/{wildcards.build}"))

def node_data_files(wildcards):
    build_pair = f"{wildcards.species}/{wildcards.build}"
    files = [
        f"results/{wildcards.species}/{wildcards.build}/branch_lengths.json",
    ]
    if config['ancestral'].get(build_pair, False):
        files.append(f"results/{wildcards.species}/{wildcards.build}/muts.json")
    if config['traits'].get(build_pair, False):
        files.append(f"results/{wildcards.species}/{wildcards.build}/traits.json")
    if _uses_sampling_year(wildcards):
        files.append(f"results/{wildcards.species}/{wildcards.build}/sampling-year.json")

    if config['label_outbreaks'].get(build_pair, False):
        files.append(f"results/{wildcards.species}/{wildcards.build}/outbreaks.json")

    # TODO: allow a way for configs to define custom rules which produce node-data JSONs
    # and have this function return the JSONs so the custom rule becomes part of the DAG
    #
    # if config.get['export'][build_pair].get('additional_node_data_jsons', False):
    #     raise Error("NOT YET IMPLEMENTED")

    return files


def _warning(wildcards):
    """
    Returns a list of arguments to be supplied to `augur export v2` for the warning text.
    A config-specified 'warning' key is expected to be a markdown string.
    A config-specified 'warning_file' key is expected to be a markdown file and the file
    is resolved (and thus must exist).
    The 'warning' key takes precidence.
    """
    block = config['export'][f"{wildcards.species}/{wildcards.build}"]
    if warning:=block.get('warning'):
        return ['--warning', warning]
    if warning_file:=block.get('warning_file'):
        return ['--warning', resolve_config_path(warning_file)({})]
    return []


def _description(wildcards):
    """
    Returns a list of arguments to be supplied to `augur export v2` for the description text.
    A config-specified 'description' key is expected to be a markdown file and the file
    is resolved (and thus must exist).
    """
    if fname:=config['export'][f"{wildcards.species}/{wildcards.build}"].get('description'):
        return ['--description', resolve_config_path(fname)({})]
    return []

def _colors(wildcards):
    """
    Returns a list of arguments to be supplied to `augur export v2` for custom, per-dataset
    colors (if defined in config).
    """
    if fname:=config['export'][f"{wildcards.species}/{wildcards.build}"].get('colors'):
        return ['--colors', resolve_config_path(fname)({})]
    return []


BASE_LAT_LONGS = os.path.join(workflow.basedir, 'defaults', 'lat_longs.tsv')

rule concatenate_lat_longs:
    """Config defined lat_longs TSV will be joined with the BASE_LAT_LONGS file if specified"""
    input:
        base = BASE_LAT_LONGS,
        user = lambda w: config['export'][f"{w.species}/{w.build}"]['lat_longs'], # relative to analysis directory
    output:
        lat_longs = "results/{species}/{build}/lat_longs.tsv",
    shell:
        """cat {input.base} {input.user} > {output.lat_longs}"""

def _auspice_configs(wildcards):
    """returns a list of JSON files for consumption by `augur export v2`. If the config defines
    'auspice_config_overlay' then this config section is written into its own config JSON
    and that file is part of the returned list of files.
    """
    build = config['export'][f"{wildcards.species}/{wildcards.build}"]
    jsons = [
        resolve_config_path(build['auspice_config'])({}),
    ]
    if _uses_sampling_year(wildcards):
        jsons.append(f"results/{wildcards.species}/{wildcards.build}/sampling-year.config.json")
    if overlay:=build.get('auspice_config_overlay'):
        if not isinstance(overlay, dict):
            raise InvalidConfigError(f"config.export.<build_pair>.auspice_config_overlay must be a dictionary; use auspice_config to provide the base JSON")
        import json
        fname = f"results/{wildcards.species}/{wildcards.build}/auspice_config_overlay.json"
        with open(fname, 'w') as fh:
            json.dump(overlay, fh, indent=2)
        jsons.append(fname)
    return jsons


rule export:
    """Exporting data files for for auspice"""
    input:
        tree = "results/{species}/{build}/tree.nwk",
        metadata = "results/{species}/{build}/metadata.tsv",
        node_data_jsons = node_data_files,
        lat_longs = lambda w: "results/{species}/{build}/lat_longs.tsv" if config['export'][f"{w.species}/{w.build}"].get('lat_longs') else BASE_LAT_LONGS,
        auspice_config = _auspice_configs,
    output:
        auspice_json = "auspice/ebola_{species}_{build}.json" # TODO XXX remap name to match URLs?
    params:
        id_field = config['strain_id_field'],
        warning = _warning,
        description = _description,
        colors = _colors,
    benchmark:
        "benchmarks/{species}/{build}/export.txt"
    log:
        "logs/{species}/{build}/export.txt"
    shell:
        r"""
        exec &> >(tee {log:q})

        augur export v2 \
            --auspice-config {input.auspice_config:q} \
            --tree {input.tree:q} \
            --metadata {input.metadata:q} \
            --metadata-id-columns {params.id_field:q} \
            --node-data {input.node_data_jsons:q} \
            --lat-longs {input.lat_longs:q} \
            --include-root-sequence-inline \
            {params.colors:q} \
            {params.warning:q} \
            {params.description:q} \
            --output {output.auspice_json:q}
        """
