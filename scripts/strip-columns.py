#!/usr/bin/env python3
"""Strip problematic columns from exported CSVs before re-import.
Applies the recipe's excludeFields that weren't present at export time."""

import csv
import sys
import os

STRIP_MAP = {
    "01-Account.csv": ["Upsell_Opportunity_Products__c"],
    "07-OpportunityLineItem.csv": ["TotalPrice"],
    "08-Contract.csv": ["ActivatedDate", "ActivatedById"],
    "09-Order.csv": ["StatusCode", "ActivatedDate", "ActivatedById"],
}

tier_dir = sys.argv[1] if len(sys.argv) > 1 else "exports/prod-closed-won-20260310/tier-0"

for filename, columns_to_strip in STRIP_MAP.items():
    filepath = os.path.join(tier_dir, filename)
    if not os.path.exists(filepath):
        print(f"SKIP: {filepath} not found")
        continue

    with open(filepath, "r", encoding="utf-8-sig") as f:
        content = f.read()

    rows = list(csv.reader(content.splitlines()))
    if not rows:
        print(f"SKIP: {filepath} is empty")
        continue

    headers = rows[0]
    strip_indices = set()
    for col in columns_to_strip:
        for i, h in enumerate(headers):
            if h.strip('"') == col:
                strip_indices.add(i)
                break

    if not strip_indices:
        print(f"SKIP: {filename} - columns not found: {columns_to_strip}")
        continue

    new_headers = [h for i, h in enumerate(headers) if i not in strip_indices]
    new_rows = []
    for row in rows[1:]:
        new_rows.append([v for i, v in enumerate(row) if i not in strip_indices])

    with open(filepath, "w", encoding="utf-8", newline="") as f:
        f.write("\ufeff")
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(new_headers)
        writer.writerows(new_rows)

    stripped_names = [headers[i] for i in sorted(strip_indices)]
    print(f"OK: {filename} - stripped {stripped_names} ({len(new_rows)} rows)")
