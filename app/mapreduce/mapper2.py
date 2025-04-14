import sys

for line in sys.stdin:
    try:
        doc_id, title, text = line.strip().split("\t", 2)
        words = set(text.lower().split())
        for word in words:
            print(f"{word}\t{doc_id}")
    except ValueError:
        continue
