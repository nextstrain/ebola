from augur.io import read_metadata
import json
import argparse

colors = [ # <https://github.com/nextstrain/auspice/blob/master/src/util/globals.js>
  [],
  ["#4C90C0"],
  ["#4C90C0", "#CBB742"],
  ["#4988C5", "#7EB876", "#CBB742"],
  ["#4580CA", "#6BB28D", "#AABD52", "#DFA43B"],
  ["#4377CD", "#61AB9D", "#94BD61", "#CDB642", "#E68133"],
  ["#416DCE", "#59A3AA", "#84BA6F", "#BBBC49", "#E29D39", "#E1502A"],
  ["#3F63CF", "#529AB6", "#75B681", "#A6BE55", "#D4B13F", "#E68133", "#DC2F24"],
  ["#3E58CF", "#4B8EC1", "#65AE96", "#8CBB69", "#B8BC4A", "#DCAB3C", "#E67932", "#DC2F24"],
  ["#3F4DCB", "#4681C9", "#5AA4A8", "#78B67E", "#9EBE5A", "#C5B945", "#E0A23A", "#E67231", "#DC2F24"],
  ["#4042C7", "#4274CE", "#5199B7", "#69B091", "#88BB6C", "#ADBD51", "#CEB541", "#E39B39", "#E56C2F", "#DC2F24"],
  ["#4137C2", "#4066CF", "#4B8DC2", "#5DA8A3", "#77B67F", "#96BD60", "#B8BC4B", "#D4B13F", "#E59638", "#E4672F", "#DC2F24"],
  ["#462EB9", "#3E58CF", "#4580CA", "#549DB2", "#69B091", "#83BA70", "#A2BE57", "#C1BA47", "#D9AD3D", "#E69136", "#E4632E", "#DC2F24"],
  ["#4B26B1", "#3F4ACA", "#4272CE", "#4D92BF", "#5DA8A3", "#74B583", "#8EBC66", "#ACBD51", "#C8B944", "#DDA93C", "#E68B35", "#E3602D", "#DC2F24"],
  ["#511EA8", "#403DC5", "#4063CF", "#4785C7", "#559EB1", "#67AF94", "#7EB877", "#98BD5E", "#B4BD4C", "#CDB642", "#DFA53B", "#E68735", "#E35D2D", "#DC2F24"],
  ["#511EA8", "#403AC4", "#3F5ED0", "#457FCB", "#5098B9", "#60AA9F", "#73B583", "#8BBB6A", "#A4BE56", "#BDBB48", "#D3B240", "#E19F3A", "#E68234", "#E25A2C", "#DC2F24"],
  ["#511EA8", "#4138C3", "#3E59CF", "#4379CD", "#4D92BE", "#5AA5A8", "#6BB18E", "#7FB975", "#96BD5F", "#AFBD4F", "#C5B945", "#D8AE3E", "#E39B39", "#E67D33", "#E2572B", "#DC2F24"],
  ["#511EA8", "#4236C1", "#3F55CE", "#4273CE", "#4A8CC2", "#569FAF", "#64AD98", "#76B680", "#8BBB6A", "#A1BE58", "#B7BC4B", "#CCB742", "#DCAB3C", "#E59638", "#E67932", "#E1552B", "#DC2F24"],
  ["#511EA8", "#4335BF", "#3F51CC", "#416ECE", "#4887C6", "#529BB6", "#5FA9A0", "#6EB389", "#81B973", "#95BD61", "#AABD52", "#BFBB48", "#D1B340", "#DEA63B", "#E69237", "#E67531", "#E1522A", "#DC2F24"],
  ["#511EA8", "#4333BE", "#3F4ECB", "#4169CF", "#4682C9", "#4F96BB", "#5AA5A8", "#68AF92", "#78B77D", "#8BBB6A", "#9EBE59", "#B3BD4D", "#C5B945", "#D5B03F", "#E0A23A", "#E68D36", "#E67231", "#E1502A", "#DC2F24"],
  ["#511EA8", "#4432BD", "#3F4BCA", "#4065CF", "#447ECC", "#4C91BF", "#56A0AE", "#63AC9A", "#71B486", "#81BA72", "#94BD62", "#A7BE54", "#BABC4A", "#CBB742", "#D9AE3E", "#E29E39", "#E68935", "#E56E30", "#E14F2A", "#DC2F24"],
  ["#511EA8", "#4531BC", "#3F48C9", "#3F61D0", "#4379CD", "#4A8CC2", "#539CB4", "#5EA9A2", "#6BB18E", "#7AB77B", "#8BBB6A", "#9CBE5B", "#AFBD4F", "#C0BA47", "#CFB541", "#DCAB3C", "#E39B39", "#E68534", "#E56B2F", "#E04D29", "#DC2F24"],
  ["#511EA8", "#4530BB", "#3F46C8", "#3F5ED0", "#4375CD", "#4988C5", "#5098B9", "#5AA5A8", "#66AE95", "#73B583", "#82BA71", "#93BC62", "#A4BE56", "#B5BD4C", "#C5B945", "#D3B240", "#DEA73B", "#E59738", "#E68234", "#E4682F", "#E04C29", "#DC2F24"],
  ["#511EA8", "#462FBA", "#3F44C8", "#3E5BD0", "#4270CE", "#4784C8", "#4E95BD", "#57A1AD", "#61AB9C", "#6DB38A", "#7BB879", "#8BBB6A", "#9BBE5C", "#ABBD51", "#BBBC49", "#CBB843", "#D6AF3E", "#DFA43B", "#E69537", "#E67F33", "#E4662E", "#E04A29", "#DC2F24"],
  ["#511EA8", "#462EB9", "#4042C7", "#3E58CF", "#416DCE", "#4580CA", "#4C90C0", "#549DB2", "#5DA8A3", "#69B091", "#75B681", "#83BA70", "#92BC63", "#A2BE57", "#B2BD4D", "#C1BA47", "#CEB541", "#D9AD3D", "#E1A03A", "#E69136", "#E67C32", "#E4632E", "#E04929", "#DC2F24"],
  ["#511EA8", "#462EB9", "#4040C6", "#3F55CE", "#4169CF", "#447DCC", "#4A8CC2", "#529AB7", "#5AA5A8", "#64AD98", "#70B487", "#7DB878", "#8BBB6A", "#99BD5D", "#A9BD53", "#B7BC4B", "#C5B945", "#D1B340", "#DCAB3C", "#E29D39", "#E68D36", "#E67932", "#E3612D", "#E04828", "#DC2F24"],
  ["#511EA8", "#472DB8", "#403EC6", "#3F53CD", "#4066CF", "#4379CD", "#4989C5", "#4F97BB", "#57A1AD", "#61AA9E", "#6BB18E", "#77B67F", "#84BA70", "#92BC64", "#A0BE58", "#AFBD4F", "#BCBB49", "#CAB843", "#D4B13F", "#DEA83C", "#E39B39", "#E68A35", "#E67732", "#E35F2D", "#DF4728", "#DC2F24"],
  ["#511EA8", "#472CB7", "#403DC5", "#3F50CC", "#4063CF", "#4375CD", "#4785C7", "#4D93BE", "#559EB1", "#5DA8A3", "#67AF94", "#72B485", "#7EB877", "#8BBB6A", "#98BD5E", "#A6BE55", "#B4BD4C", "#C1BA47", "#CDB642", "#D7AF3E", "#DFA53B", "#E49838", "#E68735", "#E67431", "#E35D2D", "#DF4628", "#DC2F24"],
  ["#511EA8", "#482CB7", "#403BC5", "#3F4ECB", "#3F61D0", "#4272CE", "#4682C9", "#4C90C0", "#529BB5", "#5AA5A8", "#63AC9A", "#6DB28B", "#78B77D", "#84BA6F", "#91BC64", "#9EBE59", "#ACBD51", "#B9BC4A", "#C5B945", "#D0B441", "#DAAD3D", "#E0A23A", "#E59637", "#E68434", "#E67231", "#E35C2C", "#DF4528", "#DC2F24"],
  ["#511EA8", "#482BB6", "#403AC4", "#3F4CCB", "#3F5ED0", "#426FCE", "#457FCB", "#4A8CC2", "#5098B9", "#58A2AC", "#60AA9F", "#69B091", "#73B583", "#7FB976", "#8BBB6A", "#97BD5F", "#A4BE56", "#B1BD4E", "#BDBB48", "#C9B843", "#D3B240", "#DCAB3C", "#E19F3A", "#E69337", "#E68234", "#E67030", "#E25A2C", "#DF4428", "#DC2F24"],
  ["#511EA8", "#482BB6", "#4039C3", "#3F4ACA", "#3E5CD0", "#416CCE", "#447CCD", "#4989C4", "#4E96BC", "#559FB0", "#5DA8A4", "#66AE96", "#6FB388", "#7AB77C", "#85BA6F", "#91BC64", "#9DBE5A", "#AABD53", "#B6BD4B", "#C2BA46", "#CDB642", "#D6B03F", "#DDA83C", "#E29D39", "#E69036", "#E67F33", "#E56D30", "#E2592C", "#DF4428", "#DC2F24"],
  ["#511EA8", "#482AB5", "#4138C3", "#3F48C9", "#3E59CF", "#4169CF", "#4379CD", "#4886C6", "#4D92BE", "#539CB4", "#5AA5A8", "#62AB9B", "#6BB18E", "#75B581", "#7FB975", "#8BBB6A", "#96BD5F", "#A2BE57", "#AFBD4F", "#BABC4A", "#C5B945", "#CFB541", "#D8AE3E", "#DFA63B", "#E39B39", "#E68D36", "#E67D33", "#E56B2F", "#E2572B", "#DF4328", "#DC2F24"],
  ["#511EA8", "#492AB5", "#4137C2", "#3F47C9", "#3E57CE", "#4067CF", "#4376CD", "#4783C8", "#4C8FC0", "#519AB7", "#58A2AC", "#5FA9A0", "#68AF93", "#70B486", "#7BB77A", "#85BA6F", "#90BC65", "#9CBE5B", "#A8BE54", "#B3BD4D", "#BEBB48", "#C9B843", "#D2B340", "#DAAD3D", "#E0A33B", "#E49838", "#E68B35", "#E67B32", "#E5692F", "#E2562B", "#DF4227", "#DC2F24"],
  ["#511EA8", "#492AB5", "#4236C1", "#3F45C8", "#3F55CE", "#4064CF", "#4273CE", "#4681CA", "#4A8CC2", "#4F97BA", "#569FAF", "#5CA7A4", "#64AD98", "#6DB28B", "#76B680", "#80B974", "#8BBB6A", "#96BD60", "#A1BE58", "#ACBD51", "#B7BC4B", "#C2BA46", "#CCB742", "#D4B13F", "#DCAB3C", "#E1A13A", "#E59638", "#E68835", "#E67932", "#E4672F", "#E1552B", "#DF4227", "#DC2F24"],
  ["#511EA8", "#4929B4", "#4235C0", "#3F44C8", "#3F53CD", "#3F62CF", "#4270CE", "#457ECB", "#4989C4", "#4E95BD", "#549DB3", "#5AA5A8", "#61AB9C", "#69B090", "#72B485", "#7BB879", "#85BA6E", "#90BC65", "#9BBE5C", "#A6BE55", "#B1BD4E", "#BBBC49", "#C5B945", "#CEB541", "#D6AF3E", "#DDA93C", "#E29F39", "#E69537", "#E68634", "#E67732", "#E4662E", "#E1532B", "#DF4127", "#DC2F24"],
  ["#511EA8", "#4929B4", "#4335BF", "#3F42C7", "#3F51CC", "#3F60D0", "#416ECE", "#447CCD", "#4887C6", "#4D92BF", "#529BB6", "#58A2AB", "#5FA9A0", "#66AE95", "#6EB389", "#77B67E", "#81B973", "#8BBB6A", "#95BD61", "#A0BE59", "#AABD52", "#B5BD4C", "#BFBB48", "#C9B843", "#D1B340", "#D8AE3E", "#DEA63B", "#E29C39", "#E69237", "#E68434", "#E67531", "#E4642E", "#E1522A", "#DF4127", "#DC2F24"],
  ["#511EA8", "#4928B4", "#4334BF", "#4041C7", "#3F50CC", "#3F5ED0", "#416CCE", "#4379CD", "#4784C7", "#4B8FC1", "#5098B9", "#56A0AF", "#5CA7A4", "#63AC99", "#6BB18E", "#73B583", "#7CB878", "#86BB6E", "#90BC65", "#9ABD5C", "#A4BE56", "#AFBD4F", "#B9BC4A", "#C2BA46", "#CCB742", "#D3B240", "#DAAC3D", "#DFA43B", "#E39B39", "#E68F36", "#E68234", "#E67431", "#E4632E", "#E1512A", "#DF4027", "#DC2F24"]
]


def suggest_colors(years):
    # A categorical scale looks better and helps understand the different outbreaks (IMO)
    # cf a continuous scale, although that would be more technically accurate
    c = colors[len(years)] # colors is 1-indexed

    config = {
        "key": "year",
        "title": "Sampling Year",
        "type": "categorical",
        "scale": [[year, c[idx]] for idx,year in enumerate(years)]
    }

    print("Suggested auspice-config colors entry:")
    print(json.dumps(config))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", required=True, help="Metadata TSV")
    parser.add_argument("--id-columns", nargs="+", help="ID columns in Metadata TSV", default=['accession'])
    parser.add_argument("--output", required=True, help="Node Data JSON output")
    args = parser.parse_args()

    m = read_metadata(args.metadata, id_columns=args.id_columns)
    nodes = {name: {'year': date.split('-')[0]} for name,date in zip(m.index, m['date']) if date and not date.startswith('X')}
    with open(args.output, 'w') as fh:
        json.dump({"nodes": nodes}, fh)

    try:
        suggest_colors(sorted(set([x['year'] for x in nodes.values()])))
    except KeyError:
        print("Failed to suggest colours for the auspice config")
