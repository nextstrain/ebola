#! /usr/bin/env python3
"""
Custom script to extract information from strain names.
"""

import json
import re
from sys import stdin, stderr


def main():
    for line in stdin:
        record = json.loads(line)
        record = extract_info(record)
        print(json.dumps(record))


patterns = (
    re.compile(r'^.*/cynomolgus macaque-wt/[A-Z]{3}/(?P<year>[0-9]{4})/.*$'),
    # Example: https://www.ncbi.nlm.nih.gov/nuccore/KY471113

    re.compile(r'^.*/H\.sapiens-(tc|wt)/[A-Z]{3}/(?P<year>[0-9]{4})/.*$'),
    # Example: https://www.ncbi.nlm.nih.gov/nuccore/KT582109
)


def extract_info(record: dict[str, str]):
    """
    Extract information from the strain name.
    """
    for pattern in patterns:
        if match := pattern.search(record['strain']):
            groups = match.groupdict()

            if year := groups.get('year'):
                record = apply_year_match(record, year)

    return record


def apply_year_match(record: dict[str, str], year: str):
    """
    Update the date based on the extracted year if it is an improvement over the
    current date.
    """
    current_date = record['date']
    new_date = f'{year}-XX-XX'

    # The extracted year is an improvement over the current date only if there
    # is a change and (1) the current date has a different year or (2) the
    # current date already has no month or day information.
    if current_date != new_date and (not current_date.startswith(year) or current_date.endswith('XX-XX')):
        print(f"{record['accession']!r}: date {current_date!r} → {new_date!r}", file=stderr)
        record['date'] = new_date

    return record


if __name__ == "__main__":
    main()
