# For all files from today
TODAY=$(date +%Y%m%d)
# uv run scripts/submit.py --file-path=src/create_tables.py
# uv run scripts/submit.py --file-path=src/generate_legal_docs.py

# For all files from today
TODAY=$(date +%Y%m%d)
uv run scripts/submit.py --file-path=src/insert.py --args="--file-path=s3a://data/docs/legal/*/$TODAY/ --table-name=legal.documents --mode=batch"