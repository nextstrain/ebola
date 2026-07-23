# 2026 DRC & Uganda BDBV case-count dataset

These scripts use the reported cases in the [INRB-UMIE/Ebola_DRC_2026](https://github.com/INRB-UMIE/Ebola_DRC_2026) GitHub repo to generate a Nextstrain dataset. The primary purpose is to see cases over time on the map. We use a default 7-day time window prior to the case reporting data for visualisation purposes.

**Prerequisites**

You must clone and keep up-to-date the github.com/INRB-UMIE/Ebola_DRC_2026 repo.

**How to run**

```sh
mkdir -p data
./collect-cases.py --repo <PATH_TO_REPO> --output data/cases.tsv
./make-tree.py --cases data/cases.tsv --output ../../auspice/ebola_bdbv_drc-uganda-2026-cases.json
```


### Case definitions

The canonical counts are processed upstream in the `INRB-UMIE/Ebola_DRC_2026` repo, we simply parse them here. That repo states:

> If a new SitRep disagrees with the previously reported values (i.e. reported cases decrease from report to report), we will report values exactly as is... While health zone level metrics may disagree with national values, we will report the tabular data verbatim."

* `new_suspected_cases` are suspected cases reported for that sitrep date in that zone. 
* `new_confirmed_cases` are new lab-confirmed BDBV cases reported for that sitrep date in that zone. These may have been previously reported as a suspected case.
* `cumulative_confirmed_cases` is either taken from the sitrep or derived as the previous cumulative value + new value. These may jump up due to backfilling, or may decrease due to corrections.
* `cumulative_suspected_cases` 

We add a `cumulative_confirmed_cases_clamped` column which is largely the same as `cumulative_confirmed_cases` but prevents situations where a count decreases over time by decreasing a timepoint's count if the subsequent timepoint is lower.