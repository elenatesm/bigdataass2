#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        doc_id, title, text = line.split('\t', 2)
    except ValueError:
        continue

    words = text.strip().split()
    for word in words:
        print(f"{doc_id}\t{word}\t1")
