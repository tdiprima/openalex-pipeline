Here are all the CLI commands you can use:

## Basic Commands

### List All Available Runs
```bash
python cli.py --list-runs
```
Shows all run directories with their metadata (number of authors, publications, timestamps).

---

## Commands for a Specific Run
All of these require `--run-dir <path>` to specify which run to query:

### Show Dataset Statistics
```bash
python cli.py --run-dir ../output/run_20231002_143022 --stats
```

Displays comprehensive statistics including:

- Total authors and publications
- Publications with PDFs
- Publications with ExampleOrg mentions
- Authors with ExampleOrg mentions
- Year range covered
- Top 10 authors by publication count

### Search for Authors by Name
```bash
python cli.py --run-dir ../output/run_20231002_143022 --author "Einstein"
```

Case-insensitive substring search for author names.

### Get All ExampleOrg Authors
```bash
python cli.py --run-dir ../output/run_20231002_143022 --ExampleOrg-authors
```

Lists all authors who have publications mentioning ExampleOrg.

### Find Publications by Year
```bash
python cli.py --run-dir ../output/run_20231002_143022 --year 2023
```

Shows publications from a specific year (displays first 10).

### Combine All Chunks into Single File
```bash
python cli.py --run-dir ../output/run_20231002_143022 --combine
```

Merges all publication chunk files into one `all_publications.jsonl` file.

---

## Combining Multiple Options
You can combine options in a single command:

```bash
python cli.py --run-dir ../output/run_20231002_143022 --stats --author "Smith" --ExampleOrg-authors
```

This would show statistics, search for authors named "Smith", AND list all ExampleOrg authors.

---

## Get Help

```bash
python cli.py --help
```

Shows all available options and their descriptions.

---

## Can you get all ExampleOrg authors?

## Command Line Method

```bash
python cli.py --run-dir ../output/run_20231002_143022 --ExampleOrg-authors
```

## Python Script Method

If you want to use it in your own Python code:

```python
from dataset_query import DatasetQuery

# Initialize query for your run
query = DatasetQuery("../output/run_20231002_143022")

# Get all ExampleOrg authors
for author in query.get_authors_with_ExampleOrg_mentions():
    print(f"{author['name']}: {author.get('works_count')} works")
    print(f"  ID: {author.get('id')}")
    print(f"  Institution: {author.get('last_known_institution')}")
    print()
```

<br>
