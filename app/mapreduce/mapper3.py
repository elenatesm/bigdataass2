#!/usr/bin/env python3
import sys

for line in sys.stdin:
    try:
        doc_id, title, text = line.strip().split("\t", 2)
        words = text.strip().split()
        dl = len(words)
        print(f"{doc_id}\t{dl}")
    except ValueError:
        continue
