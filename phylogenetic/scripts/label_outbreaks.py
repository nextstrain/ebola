
import argparse
from Bio import Phylo
from augur.io import read_metadata
import json
from get_year import colors
import re

# A mapping of nextclade clade names to geographic ones, following 
# <https://virological.org/t/the-16th-ebola-virus-disease-outbreak-in-bulape-health-zone-kasai-democratic-republic-of-the-congo-a-new-spillover-event-from-an-unknown-reservoir-host/1003>
# as well as other names used by INRB, e.g. "Nord-Kivu"
geo_names = {
    "Ebov-1976": {"name": "Yambuku 1976", "label": True},
    "Ebov-1994": {"name": "Gabon 1994", "label": True},
    "Ebov-1995": {"name": "Kikwit 1995", "label": True},
    "Ebov-1996a": {"name": "Gabon 1996 A", "label": True},
    "Ebov-1996b": {"name": "Gabon 1996 B", "label": True},
    "Ebov-2001a": {"name": "Gabon 2001", "label": True},
    "Ebov-2003": {"name": "Kelle 2003", "label": True}, # https://www.who.int/emergencies/disease-outbreak-news/item/2003DON158 (Kelle is in RC -- Republic of Congo)
    "Ebov-2007": {"name": "Luebo 2007", "label": True},
    "Ebov-2013": {"name": "West Africa 2013", "label": True},
    "Ebov-2013/r2021": {"name": "West Africa 2013", "label": False},
    "Ebov-2014": {"name": "Boende 2014", "label": True},
    "Ebov-2017": {"name": "Likati 2017", "label": True},
    "Ebov-2018a": {"name": "Tumba 2018", "label": True},
    "Ebov-2018b": {"name": "Nord-Kivu 2018", "label": True},
    "Ebov-2020": {"name": "Mbandaka 2020", "label": True},
    "Ebov-2025": {"name": "Bulape 2025", "label": True},
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tree", required=True, help="Newick")
    parser.add_argument("--metadata", required=True, help="Metadata TSV")
    parser.add_argument("--output", required=True, help="Node Data JSON")
    parser.add_argument("--id-columns", nargs="+", help="ID columns in Metadata TSV", default=['accession'])
    args = parser.parse_args()

    T = Phylo.read(args.tree, "newick")
    m = read_metadata(args.metadata, id_columns=args.id_columns)
    outbreaks = m.groupby('outbreak').apply(lambda g: g.index.tolist()).to_dict()
    nodes = {}
    branches = {}

    cas = {} # map of CA node to nextclade outbreak name
    for name,strains in outbreaks.items():
        ca = T.common_ancestor(strains)
        print(f"Outbreak {name} CA: {ca.name}, num outbreak strains: {len(strains)}, num descendants of CA: {len(ca.get_terminals())}")
        cas[ca] = name

    for start_node in T.find_clades():
        if outbreak_nextclade:=cas.get(start_node, False):
            outbreak_geo = geo_names[outbreak_nextclade]['name']

            # label all downstream nodes, as we're preorder so we'll overwrite
            for node in start_node.find_clades():
                if node.name in nodes:
                    print("...overwriting", nodes[node.name]['outbreak'], 'with', outbreak_nextclade)
                nodes[node.name] = {
                    'outbreak': outbreak_nextclade,
                    'outbreak_geo': outbreak_geo
                }

            # label CA branch with a branch label
            branches[start_node.name] = {'labels': {'outbreak': outbreak_nextclade}}
            if geo_names[outbreak_nextclade]['label']:
                branches[start_node.name]['labels']['outbreak_geo'] = outbreak_geo

    with open(args.output, 'w') as fh:
        json.dump({"nodes": nodes, "branches": branches}, fh)

    # Suggest colour scale for outbreaks config
    try:
        palette = colors[len(geo_names)]
        config = [
            {"key": "outbreak", "title": "Outbreak (Nextclade name)", "type": "categorical", "scale": []},
            {"key": "outbreak_geo", "title": "Outbreak (Geographic name)", "type": "categorical", "scale": []},
        ]
        for idx, outbreak_nextclade in enumerate(sorted(geo_names.keys(), key=lambda x: re.findall(r'\d{4}', x)[-1])):
            config[0]['scale'].append([outbreak_nextclade, palette[idx]])
            geo_data = geo_names[outbreak_nextclade]
            if geo_data['label']:
                config[1]['scale'].append([geo_data['name'], palette[idx]])
        print(f"\n\nSuggested auspice-config colors entries:")
        print("\t\t", json.dumps(config[0]), ",")
        print("\t\t", json.dumps(config[1]), ",")
    except Exception:
        print("ERROR: Colour suggestion failed")

