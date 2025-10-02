This script provides a query interface for searching through chunked and compressed JSONL (JSON Lines) datasets, specifically designed for academic publication data.

## What it does:

The script helps you search and analyze a dataset containing:
- **Authors** (with their information and publication counts)
- **Publications** (with metadata like titles, years, PDFs, etc.)

It handles data that's been split into multiple compressed chunk files and lets you query across all of them transparently.

## Key capabilities:

- **Search authors** by name or ID
- **Search publications** by title, year, year range, or author
- **Filter publications** that have PDFs downloaded
- **Filter publications** mentioning "Stony Brook" in their content
- **Get statistics** on the entire dataset
- **Export filtered subsets** to new files
- **Combine chunks** back into single files

## Can you get all Stony Brook authors?

**Not directly with a single method**, but you can do it in two steps:

1. Get all publications with Stony Brook mentions using `get_publications_with_stonybrook_mentions()`
2. Extract the unique author IDs from those publications
3. Look up those authors using `get_author_by_id()`

The script has `get_publications_with_stonybrook_mentions()` but doesn't have a corresponding `get_authors_with_stonybrook_mentions()` method. However, you could easily add one or write a small script that:

```python
query = DatasetQuery("./output/run_XXXXX")

# Get all Stony Brook author IDs
stonybrook_author_ids = set()
for pub in query.get_publications_with_stonybrook_mentions():
    author_id = pub.get('author_id')
    if author_id:
        stonybrook_author_ids.add(author_id)

# Get author details
for author_id in stonybrook_author_ids:
    author = query.get_author_by_id(author_id)
    print(author)
```

Would you like me to create a helper script or modify this one to add that functionality?

<br>
