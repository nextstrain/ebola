#! /usr/bin/env python3

"""
Labels strains as `is_lab_host=True` based on certain metadata and a
hardcoded list of checks in this script.

For ad-hoc labels we can use the `annotations.tsv`
"""

import argparse
import pandas as pd
from collections import defaultdict

# Set of NCBI titles via `jq -r '.title' data/ncbi_entrez.ndjson | sort -u`
LAB_TITLES = set([
    'A replication inhibitor of Filoviridae virus',
    'An upstream open reading frame modulates ebola virus polymerase translation and virus replication',
    'Fluorescent and bioluminescent reporter mouse-adapted Ebola viruses maintain pathogenicity and can be visualized in vivo',
    "Characterization of the L gene and 5' trailer region of Ebola virus",
    'Chimeric filovirus glycoprotein',
    'Chimeric Filoviruses for Identification and Characterization of Monoclonal Antibodies',
    'CHIMERIC VSV VIRUS COMPOSITIONS AND METHODS OF USE THEREOF FOR TREATMENT OF CANCER',
    'COMPOSITIONS AND METHODS FOR DETECTING AN RNA VIRUS',
    'CRISPR SYSTEM BASED DROPLET DIAGNOSTIC SYSTEMS AND METHODS',
    'Development of a reverse genetics system to generate a recombinant Ebola virus Makona expressing a green fluorescent protein',
    'Diagnostic reverse-transcription polymerase chain reaction kit for filoviruses based on the strain collections of all European biosafety level 4 laboratories',
    'DNA vaccines expressing either the GP or NP genes of Ebola virus protect mice from lethal challenge',
    'Ebola virus seed stock sequencing',
    'Ebolavirus chimerization for development of a mouse model for screening of bundibugyo-specific antibodies in vivo',
    'Evaluation of Signature Erosion in Ebola Virus Due to Genomic Drift and Its Impact on the Performance of Diagnostic Assays',
    'Evidence for replication absent genetic diversification and high concentration of Ebola virus in semen of a patient recovering from severe disease',
    'FDA dAtabase for Regulatory Grade micrObial Sequences (FDA-ARGOS): Supporting development and validation of Infectious Disease Dx tests',
    'Fruit bats as reservoirs of Ebola virus',
    'Generation and Characterization of a Mouse-Adapted Makona Variant of Ebola Virus',
    'GP mRNA of Ebola virus is edited by the Ebola virus polymerase and by T7 and vaccinia virus polymerases',
    'HUMAN EBOLA VIRUS SPECIES AND COMPOSITIONS AND METHODS THEREOF',
    'Implementation of a non-human primate model of Ebola disease: Infection of Mauritian cynomolgus macaques and analysis of virus populations',
    "Informing the Historical Record of Experimental Nonhuman Primate Infections with Ebola Virus: Genomic Characterization of USAMRIID Ebola Virus/H.sapiens-tc/COD/1995/Kikwit-9510621 Challenge Stock 'R4368' and Its Replacement 'R4415'",
    'METHODS AND DEVICES FOR REAL-TIME DIAGNOSTIC TESTING (RDT) FOR EBOLA AND OTHER INFECTIOUS DISEASES',
    'METHODS OF DETECTING EBOLA',
    'Monoclonal antibodies and vaccines against epitopes on the ebola virus glycoprotein',
    'Mouse adapted variant of Ebola virus subtype Zaire strain Mayinga complete genome',
    'Genetic factors of Ebola virus virulence in guinea pigs',
    'Mutations In Ebola Virus-Makona Genome Do Not Seem To Alter Pathogenicity In Animal Models',
    'PRIMER SET FOR DETECTION OF ZAIRE EBOLA VIRUS, ASSAY KIT, AND AMPLIFICATION METHOD',
    'Primer set, Assay kit and Detecting method for Zaire Ebolavirus',
    'Rapid detection of all known ebolavirus species by reverse transcription-loop-mediated isothermal amplification (RT-LAMP)',
    'RAPID VACCINE PLATFORM',
    'Recombinant Ebola virus nucleoprotein and glycoprotein (Gabon 94 strain) provide new tools for the detection of human infections',
    'Screen for inhibitors of filovirus and uses therefor',
    'The nucleoprotein gene of Ebola virus: cloning, sequencing, and in vitro expression',
    'The virion glycoproteins of Ebola viruses are encoded in two reading frames and are expressed through transcriptional editing',
    'The VP35 and VP40 proteins of filoviruses. Homology between Marburg and Ebola viruses',
    'Therapeutic efficacy of the small molecule GS-5734 against Ebola virus in rhesus monkeys',
    'Uncovering the Etiology of Fevers of Unknown Origin: a laboratory-based observational study in patients with suspected Ebola, Guinea, 2014',
    'Recombinant biologically contained filovirus',
    'Preliminary Evaluation of the Effect of Investigational Ebola Virus Disease Treatments on Viral Genome Sequences',
    'Effect of Experimental Treatment on Imported Ebola Genome Sequences',
])

NOTES = set([
    'harvest date: 01-May-2015; passaged 3x in cell culture (parent stock: SAMN05859699)',
    'harvest date: 06-MAR-2015; passaged 3x in cell culture following isolation (parent stock: SAMN05755726)',
    'harvest date: 07-May-2015; passaged 3x in cell culture (parent stock: SAMN04488486)',
    'harvest date: 09-Jun-2015; passaged 3x in cell culture (parent stock: SAMN04488486)',
    'harvest date: 15-Apr-2015; passaged 3x in cell culture (parent stock: SAMN04488486)',
    'harvest date: 15-Jul-2015; passaged 2x in cell culture',
    'harvest date: 16-Jan-2015; passaged 2x in cell culture',
    'harvest date: 16-Oct-2014; passaged 2x in cell culture',
    'harvest date: 17-Jul-2015; passaged 4x in cell culture',
    'harvest date: 21-Jan-2016; passaged 4x (parent stock: SAMN05859700)',
    'harvest date: 21-Nov-2014; passaged 3x in cell culture (parent stock: SAMN04488486)',
    'harvest date: 25-Oct-2014; passaged 3x in cell culture (parent stock: SAMN04488486)',
    'harvest date: 28-Aug-2014; passaged 5x in cell culture',
    'harvest date: 30-Dec-2014; passaged 2x in cell culture following isolation',
    'harvest date: Aug-2015; passaged 3x in cell culture (parent stock: SAMN05860094)',
    'chimeric molecule between Ebola virus Glycoprotein 1 and Ebola virus Glycoprotein 2',
    'mouse-adapted in Balb/c mice',
    'mouse-adapted, recombinant Ebola virus',
    'mouse-adapted, recombinant Ebola virus, expressing fused ZsG and nLuc',
    'mouse-adapted, recombinant Ebola virus, expressing nLuc',
    'mouse-adapted, recombinant Ebola virus, expressing nLuc and ZsG',
    'mouse-adapted, recombinant Ebola virus, expressing ZsG',
    'mouse-adapted; harvest date: 19-Jun-2015; passaged 2x in cell culture following isolation and plaque-picking',
    'mouse-adapted; harvest date: 19-Jun-2015; passaged 3x in cell culture following isolation and plaque-picking (parent stock: SAMN05859701)',
    'passage details: 5 passages on VeroE6',
    'passage details: P0',
    'passage details: P2',
    'passage details: passaged in VERO E-6 cells',
    'passage details: passaged in Vero E-6 cells and havested on 15-Jan-2015',
    'passage details: passaged using clone R4414 as a seed',
    'passage details: propagated in Vero E6 cells and harvested on 12-Aug-2014 after 13 days in culture',
    'passage details: Total 2 passages CDC (Vero E6 (1)/ PHE Porton Vero E6 (1))',
    'passage details: Total 3 passages CDC (Vero E6 (1)/ PHE Porton Vero E6 (2))',
    'passaged twice in cell culture',
    'recombinant virus derived from Zaire ebolavirus isolate Ebola virus/H.sapiens-wt/LBR/2014/Makona-201403007 (KP178538)',
    'recombinant virus derived from Zaire ebolavirus isolate Ebola virus/H.sapiens-wt/LBR/2014/Makona-201403007 (KP178538); sequence coding for ZsGreen-P2A-VP40 fusion is inserted in VP40 gene',
    'recombinant virus obtained by reverse genetics',
    'recombinant virus obtained by reverse genetics; contains marker gene fusion',
    'recombinant virus; sequence based on INSDC accession AF086833.2; four silent mutations (genetic markers) compared to AF086833.2: c2149g, a11043g, c13194g, c15639g',
    'USAMRIID_ID#16502; Vero E6 passage #3',
    'mAb114-post-treatment',
])


excluded = {
    "title": defaultdict(list),
    "note": defaultdict(list),
}

def is_lab_host(row):
    """
    Apply strain preference hierarchy for rows that have a match.
    Preference: ncbi.strain > ncbi.isolate > ppx.strain
    """
    if row['is_lab_host'] is True:
        return True
    if row['title'] in LAB_TITLES:
        excluded['title'][row['title']].append(row)
        return True
    if row['note'] in NOTES:
        excluded['note'][row['note']].append(row)
        return True
    return ''

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", required=True, help="Input metadata TSV file")
    parser.add_argument("--output", required=True, help="Output metadata TSV file")
    args = parser.parse_args()

    metadata = pd.read_csv(args.metadata, sep='\t')

    n = 0
    metadata['is_lab_host'] = metadata.apply(is_lab_host, axis=1)
    for reason in excluded.keys():
        for value,rows in excluded[reason].items():
            print(f"{len(rows)} strains set as 'is_lab_host=True' due to \"{reason}\"=\"{value}\"")
            for row in rows:
                print(f"\t{row['accession']} ({row['strain']})")
                n+=1
    print('-'*80 + f"\nMarked {n} strains as lab host due to metadata matches\n" + '-'*80)

    metadata.to_csv(args.output, sep='\t', index=False)



