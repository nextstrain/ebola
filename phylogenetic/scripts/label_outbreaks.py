
import argparse
from Bio import Phylo
from augur.io import read_metadata
import json
from get_year import colors
import re

def geographic(nextclade_outbreak: str):
    """
    Given a nextclade outbreak name (e.g. `Ebov-2013`) returns a geographic name, largely following 
    <https://virological.org/t/the-16th-ebola-virus-disease-outbreak-in-bulape-health-zone-kasai-democratic-republic-of-the-congo-a-new-spillover-event-from-an-unknown-reservoir-host/1003>
    as well as other names used by INRB, e.g. "Nord-Kivu".

    NOTE: relapse outbreaks (e.g. `Ebov-2013/r2021`) are not given new geo names
    """
    relapse = '/r' in nextclade_outbreak
    key = nextclade_outbreak.split('/r')[0]
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
        "Ebov-2014": {"name": "Boende 2014", "label": True},
        "Ebov-2017": {"name": "Likati 2017", "label": True},
        "Ebov-2018a": {"name": "Tumba 2018", "label": True},
        "Ebov-2018b": {"name": "Nord-Kivu 2018", "label": True},
        "Ebov-2020": {"name": "Mbandaka 2020", "label": True},
        "Ebov-2022": {"name": "Mbandaka 2022", "label": True},
        "Ebov-2025": {"name": "Bulape 2025", "label": True},
        "unassigned": {"name": "unassigned", "label": False},
    }
    if key=='unassigned':
        return {'name': 'unassigned', 'label': False}
    if key in geo_names:
        base_info = {**geo_names[key]}
        if relapse:
            base_info['label'] = False
        return base_info
    print(f"\n[ERROR]Nextclade outbreak label {outbreak_nextclade} doesn't have a geographic name set in the `geo_names` dict\n\n")
    return {'name': nextclade_outbreak, 'label': False}


def suggest_colours(key: str, title: str, outbreaks: set[str]):
    palette = colors[len(outbreaks)]
    # Prune unassigned - it'll be dropped by Auspice anyways as 'unassigned' is a special-cased name
    assigned_outbreaks = [k for k in outbreaks if k!='unassigned']
    # sort outbreaks by year. For relapses it's the relapse year, not the original one
    def get_year(outbreak: str) -> int:
        return int(re.findall(r'\d{4}', outbreak)[-1])
    scale = [[name, palette[idx]] for idx,name in enumerate(sorted(assigned_outbreaks, key=get_year))]
    TAB = '  '
    print(f"{TAB}{TAB}{{")
    print(f"{TAB}{TAB}{TAB}\"key\": \"{key}\",")
    print(f"{TAB}{TAB}{TAB}\"title\": \"{title}\",")
    print(f"{TAB}{TAB}{TAB}\"type\": \"categorical\",")
    print(f"{TAB}{TAB}{TAB}\"scale\": {json.dumps(scale)}")
    print(f"{TAB}{TAB}}},")


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
    outbreaks_nextclade = set()
    outbreaks_geo = set()

    outbreak_mrcas = {} # map of MRCA node to nextclade outbreak name
    for name,strains in outbreaks.items():
        ca = T.common_ancestor(strains)
        print(f"Outbreak {name} CA: {ca.name}, num outbreak strains: {len(strains)}, num descendants of CA: {len(ca.get_terminals())}")
        outbreak_mrcas[ca] = name

    for start_node in T.find_clades():
        if outbreak_nextclade:=outbreak_mrcas.get(start_node, False):
            geo = geographic(outbreak_nextclade)
            outbreaks_nextclade.add(outbreak_nextclade)
            outbreaks_geo.add(geo['name'])

            # label all downstream nodes, as we're preorder so we'll overwrite
            for node in start_node.find_clades():
                if node.name in nodes:
                    print("...overwriting", nodes[node.name]['outbreak'], 'with', outbreak_nextclade)
                nodes[node.name] = {
                    'outbreak': outbreak_nextclade,
                    'outbreak_geo': geo['name']
                }

            # label CA branch with a branch label
            if outbreak_nextclade != 'unassigned':
                branches[start_node.name] = {'labels': {'outbreak': outbreak_nextclade}}
            if geo['label']:
                branches[start_node.name]['labels']['outbreak_geo'] = geo['name']

    with open(args.output, 'w') as fh:
        json.dump({"nodes": nodes, "branches": branches}, fh)

    # Suggest a colour scale for the outbreaks
    print("\n---- Suggested Auspice config colourings -----\n\n")
    try:
        suggest_colours("outbreak", "Outbreak (Nextclade name)", outbreaks_nextclade)
        suggest_colours("outbreak_geo", "Outbreak (Geographic name)", outbreaks_geo)
    except Exception as e:
        print("Unexpected error", e)
