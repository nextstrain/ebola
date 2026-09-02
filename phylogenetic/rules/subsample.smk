from augur.subsample import get_referenced_files

# # Merge Sota into Bunia in reconstructed_location 
# # Then merge So
# rule process_locations:
#     input:
rule process_bdbv_locations:
    input:
        metadata = "results/{species}/metadata_extended.tsv"
    output:
        metadata = "results/{species}/metadata_extended_updated_geo.tsv"
    log:
        log_file = "logs/{species}/process_locations.txt"
    run:
        import csv
        sota_samples = 0
        inferred_key = 'location_reconstructed'
        augmented_key = 'location_epicenter'

        epicenter_zones = ["Bunia", "Mongbwalu", "Rwampara"]

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
                        if row['location'] == "Sota":
                            row[inferred_key] = "Bunia"
                            sota_samples+=1
                        else:
                            row[inferred_key] = row['location']
                        if row[inferred_key] in epicenter_zones:
                            row[augmented_key] = "Bunia|Mongbwalu|Rwampara"
                        else:
                            row[augmented_key] = row[inferred_key]
                    writer.writerow(row)

        with open(log.log_file,'w') as l:
            l.write(f"{sota_samples} Samples from Sota recoded as Bunia in {inferred_key}")




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
    params:
        id_field = config['strain_id_field'],
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
            --metadata-id-columns {params.id_field} \
            --output-sequences {output.sequences} \
            --output-metadata {output.metadata}
        """