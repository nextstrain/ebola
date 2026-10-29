from augur.subsample import get_referenced_files

rule process_bdbv_locations:
    input:
        metadata = "results/{species}/metadata_extended.tsv"
    output:
        metadata = "results/{species}/metadata_extended_updated_geo.tsv"
    log:
        log_file = "logs/{species}/process_locations.txt"
    run:
        import csv
        inferred_key = 'location_reconstructed'
        augmented_key = 'location_epicenter'

        epicenter_zones = ["Bunia", "Mongbwalu", "Rwampara"]
        epicenter_joint_name = "Bunia|Mongbwalu|Rwampara"
        epicenter_count = 0

        log_fh = open(log.log_file, 'w')

        with open(output.metadata, 'w') as output:
            with open(input.metadata,'r') as metadata:

                metadata_reader = csv.DictReader(metadata,delimiter="\t")
                header = metadata_reader.fieldnames + [inferred_key,augmented_key]

                writer = csv.DictWriter(output, fieldnames=header,delimiter="\t")
                writer.writeheader()

                for row in metadata_reader:
                    location = row.get('location')
                    if not location: # some samples do not have locations in the metadata
                        writer.writerow(row)
                        continue
                    else:
                        row[inferred_key] = row['location']
                        if row[inferred_key] in epicenter_zones:
                            row[augmented_key] = epicenter_joint_name
                            epicenter_count+=1
                        else:
                            row[augmented_key] = row[inferred_key]
                    writer.writerow(row)

        with open(log.log_file,'w') as l:
            l.write(f"{epicenter_count} Samples recorded as {augmented_key} = {epicenter_joint_name}")




rule subsample:
    input:
        config = "results/{species}/{build}/subsample_config.yaml",
        sequences = "results/{species}/alignment.fasta",
        metadata = lambda w: f"results/{w.species}/metadata_extended.tsv" \
        if w.species!='bdbv' \
        else f"results/{w.species}/metadata_extended_updated_geo.tsv" ,
        # note: get_referenced_files will use the env variable AUGUR_SEARCH_PATHS
        referenced_files = lambda w: get_referenced_files(f"results/{w.species}/{w.build}/subsample_config.yaml")
    output:
        sequences = "results/{species}/{build}/subsampled.fasta",
        metadata = "results/{species}/{build}/metadata.tsv",
        full_log = "logs/{species}/{build}/subsample-per-strain.tsv"
    log:
        "logs/{species}/{build}/subsample.txt",
    benchmark:
        "benchmarks/{species}/{build}/subsample.txt",
    shell:
        r"""
        exec &> >(tee {log:q})

        augur subsample \
            --config {input.config} \
            --sequences {input.sequences} \
            --metadata {input.metadata} \
            --output-sequences {output.sequences} \
            --output-metadata {output.metadata} \
            --output-log {output.full_log}
        """


def subsample_include_files(wildcards):
    """
    All `include` files referenced in the build's subsample config
    (under `defaults` and every entry in `samples`), resolved via AUGUR_SEARCH_PATHS
    """
    subsample_config = config["subsample"][f"{wildcards.species}/{wildcards.build}"]
    sections = [subsample_config.get("defaults", {}), *subsample_config.get("samples", {}).values()]
    paths = []
    for section in sections:
        files = section.get("include", [])
        paths.extend([files] if isinstance(files, str) else files)
    return [resolve_config_path(p)(wildcards) for p in dict.fromkeys(paths)]


rule keep_ids:
    """
    Combine the subsample include files so the force-included strains can be
    protected from clock filtering in `augur refine` via `--keep-ids`
    """
    input:
        include_files = subsample_include_files,
    output:
        keep_ids = "results/{species}/{build}/keep_ids.txt",
    shell:
        r"""
        # awk (unlike cat) adds a missing trailing newline to each file
        awk 1 /dev/null {input.include_files:q} > {output.keep_ids:q}
        """