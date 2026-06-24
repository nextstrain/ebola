rule rename_jsons_to_reflect_urls:
    input:
        *rules.all.input
    output:
        touch("results/rename_jsons_to_reflect_urls.done"),
    run:
        import os
        import shutil
        dst_dir = os.path.join('auspice', 'nextstrain.org')
        os.makedirs(dst_dir, exist_ok=True)
        for build in config['builds']:
            name = os.path.join('auspice', f"ebola_{build.replace('/', '_')}.json")
            new_name = os.path.join(dst_dir, config['rename_jsons_to_reflect_urls'][build])
            shutil.copy(name, new_name)

rule deploy_all:
    input: "results/rename_jsons_to_reflect_urls.done"
    output: touch("results/deploy_all.done")
    params:
        deploy_url = config["deploy_url"]
    shell:
        """
        nextstrain remote upload {params.deploy_url} auspice/nextstrain.org/*
        """