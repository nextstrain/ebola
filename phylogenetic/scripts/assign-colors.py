
"""
Assign colors based on ordering.
Adapted from https://github.com/blab/cytb/blob/main/phylogenetic/scripts/assign-colors.py
"""

import argparse
import pandas as pd

# Forced colours MUST NOT appear in the ordering TSV
forced_colors = {
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Assign colors based on ordering",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('--ordering', type=str, required=True, help="input ordering file")
    parser.add_argument('--color-schemes', type=str, required=True, help="input color schemes file")
    parser.add_argument('--metadata', type=str, help="if provided, restrict colors to only those found in metadata")
    parser.add_argument('--output', type=str, required=True, help="output colors tsv")
    args = parser.parse_args()

    assignment = {}
    with open(args.ordering) as f:
        for line in f.readlines():
            array = line.lstrip().rstrip().split("\t")
            if len(array) == 2:
                name = array[0]
                trait = array[1]
                if name not in assignment:
                    assignment[name] = [trait]
                else:
                    assignment[name].append(trait)

    # if metadata supplied, go through and
    # 1. remove assignments that don't exist in metadata
    if args.metadata:
        metadata = pd.read_csv(args.metadata, delimiter='\t')
        # Use 'accession' if 'strain' column doesn't exist
        id_column = 'strain' if 'strain' in metadata.columns else 'accession'
        for name in list(assignment.keys()):
            if name in metadata.columns:
                subset_present = [x for x in assignment[name] if x in metadata[name].unique()]
                assignment[name] = subset_present

    schemes = {}
    counter = 0
    with open(args.color_schemes) as f:
        for line in f.readlines():
            counter += 1
            array = line.lstrip().rstrip().split("\t")
            schemes[counter] = array

    with open(args.output, 'w') as f:
        for trait_name, trait_array in assignment.items():
            if len(trait_array)==0:
                print(f"No traits found for {trait_name}")
                continue
            if len(schemes)<len(trait_array):
                print(f"WARNING: insufficient colours available for trait {trait_name} - reusing colours!")
                remain = len(trait_array)
                color_array = []
                while(remain>0):
                    if (remain>len(schemes)):
                        color_array = [*color_array, *schemes[len(schemes)]]
                        remain -= len(schemes)
                    else:
                        color_array = [*color_array, *schemes[remain]]
                        remain = 0
            else:
                color_array = schemes[len(trait_array)]
            extra_trait_values = list(forced_colors.get(trait_name, {}).keys())
            extra_color_values = list(forced_colors.get(trait_name, {}).values())

            zipped = list(zip(trait_array+extra_trait_values, color_array+extra_color_values))
            for trait_value, color in zipped:
                f.write(trait_name + "\t" + trait_value + "\t" + color + "\n")
            f.write("\n")
