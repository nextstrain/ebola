"""
This part of the workflow handles transforming the data into standardized
formats and expects input file

    sequences_ndjson = "data/sequences.ndjson"

This will produce output files as

    metadata = "data/metadata.tsv"
    sequences = "data/sequences.fasta"

Parameters are expected to be defined in `config.transform`.
"""


rule fetch_general_geolocation_rules:
    output:
        general_geolocation_rules="data/general-geolocation-rules.tsv",
    params:
        geolocation_rules_url=config["transform"]["geolocation_rules_url"],
    shell:
        """
        curl {params.geolocation_rules_url} > {output.general_geolocation_rules}
        """


rule concat_geolocation_rules:
    input:
        general_geolocation_rules="data/general-geolocation-rules.tsv",
        local_geolocation_rules=config["transform"]["local_geolocation_rules"],
    output:
        all_geolocation_rules="data/all-geolocation-rules.tsv",
    shell:
        """
        cat {input.general_geolocation_rules} {input.local_geolocation_rules} >> {output.all_geolocation_rules}
        """


rule transform:
    input:
        sequences_ndjson="data/sequences_{serotype}.ndjson",
        all_geolocation_rules="data/all-geolocation-rules.tsv",
        annotations=config["transform"]["annotations"],
    output:
        metadata="data/raw_metadata_{serotype}.tsv",
        sequences="data/sequences_{serotype}.fasta",
    log:
        "logs/transform_{serotype}.txt",
    params:
        field_map=config["transform"]["field_map"],
        strain_regex=config["transform"]["strain_regex"],
        strain_backup_fields=config["transform"]["strain_backup_fields"],
        date_fields=config["transform"]["date_fields"],
        expected_date_formats=config["transform"]["expected_date_formats"],
        articles=config["transform"]["titlecase"]["articles"],
        abbreviations=config["transform"]["titlecase"]["abbreviations"],
        titlecase_fields=config["transform"]["titlecase"]["fields"],
        authors_field=config["transform"]["authors_field"],
        authors_default_value=config["transform"]["authors_default_value"],
        abbr_authors_field=config["transform"]["abbr_authors_field"],
        annotations_id=config["transform"]["annotations_id"],
        metadata_columns=config["transform"]["metadata_columns"],
        id_field=config["transform"]["id_field"],
        sequence_field=config["transform"]["sequence_field"],
        transform_field_names_url="https://raw.githubusercontent.com/nextstrain/monkeypox/master/ingest/bin/transform-field-names",
        transform_string_fields_url="https://raw.githubusercontent.com/nextstrain/monkeypox/master/ingest/bin/transform-string-fields",
        transform_strain_names_url="https://raw.githubusercontent.com/nextstrain/monkeypox/master/ingest/bin/transform-strain-names",
        transform_date_fields_url="https://raw.githubusercontent.com/nextstrain/monkeypox/master/ingest/bin/transform-date-fields",
        transform_genbank_location_url="https://raw.githubusercontent.com/nextstrain/monkeypox/master/ingest/bin/transform-genbank-location",
        transform_authors_url="https://raw.githubusercontent.com/nextstrain/monkeypox/master/ingest/bin/transform-authors",
        apply_geolocation_rules_url="https://raw.githubusercontent.com/nextstrain/monkeypox/master/ingest/bin/apply-geolocation-rules",
        merge_user_metadata_url="https://raw.githubusercontent.com/nextstrain/monkeypox/master/ingest/bin/merge-user-metadata",
        ndjson_to_tsv_and_fasta_url="https://raw.githubusercontent.com/nextstrain/monkeypox/master/ingest/bin/ndjson-to-tsv-and-fasta",
    shell:
        """
        if [[ ! -d bin ]]; then
          mkdir bin
        fi
        cd bin
        [[ -f transform-field-names ]]      || wget {params.transform_field_names_url}
        [[ -f transform-string-fields ]]    || wget {params.transform_string_fields_url}
        [[ -f transform-strain-names ]]     || wget {params.transform_strain_names_url}
        [[ -f transform-date-fields ]]      || wget {params.transform_date_fields_url}
        [[ -f transform-genbank-location ]] || wget {params.transform_genbank_location_url}
        [[ -f transform-authors ]]          || wget {params.transform_authors_url}
        [[ -f apply-geolocation-rules ]]    || wget {params.apply_geolocation_rules_url}
        [[ -f merge-user-metadata ]]        || wget {params.merge_user_metadata_url}
        [[ -f ndjson-to-tsv-and-fasta ]]    || wget {params.ndjson_to_tsv_and_fasta_url}
        chmod 755 *
        cd ..

        (cat {input.sequences_ndjson} \
            | ./bin/transform-field-names \
                --field-map {params.field_map} \
            | ./bin/transform-string-fields --normalize \
            | ./bin/transform-strain-names \
                --strain-regex {params.strain_regex} \
                --backup-fields {params.strain_backup_fields} \
            | ./bin/transform-date-fields \
                --date-fields {params.date_fields} \
                --expected-date-formats {params.expected_date_formats} \
            | ./bin/transform-genbank-location \
            | ./bin/transform-string-fields \
                --titlecase-fields {params.titlecase_fields} \
                --articles {params.articles} \
                --abbreviations {params.abbreviations} \
            | ./bin/transform-authors \
                --authors-field {params.authors_field} \
                --default-value {params.authors_default_value} \
                --abbr-authors-field {params.abbr_authors_field} \
            | ./bin/apply-geolocation-rules \
                --geolocation-rules {input.all_geolocation_rules} \
            | ./bin/merge-user-metadata \
                --annotations {input.annotations} \
                --id-field {params.annotations_id} \
            | ./bin/ndjson-to-tsv-and-fasta \
                --metadata-columns {params.metadata_columns} \
                --metadata {output.metadata} \
                --fasta {output.sequences} \
                --id-field {params.id_field} \
                --sequence-field {params.sequence_field} ) 2>> {log}
        """

rule post_process_metadata:
    input:
        metadata="data/raw_metadata_{serotype}.tsv",
    output:
        metadata="data/metadata_{serotype}.tsv",
    params:
        post_process_metadata_url="https://raw.githubusercontent.com/nextstrain/zika/ingest/ingest/bin/post_process_metadata.py",

    shell:
        """
        if [[ ! -d bin ]]; then
          mkdir bin
        fi
        cd bin
        [[ -f post_process_metadata.py ]] || wget {params.post_process_metadata_url}
        chmod 755 *
        cd ..

        ./bin/post_process_metadata.py --metadata {input.metadata} --outfile {output.metadata}
        """

rule compress:
    input:
        sequences="data/sequences_{serotype}.fasta",
        metadata="data/metadata_{serotype}.tsv",
    output:
        sequences="data/sequences_{serotype}.fasta.zst",
        metadata="data/metadata_{serotype}.tsv.zst",
    shell:
        """
        zstd -T0 -o {output.sequences} {input.sequences}
        zstd -T0 -o {output.metadata} {input.metadata}
        """