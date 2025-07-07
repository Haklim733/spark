# For all files from today (use UTC to match Python code)
# with spark connect
uv run -m src.create_tables
# uv run -m src.generate_legal_docs --bucket=raw --key=/docs/legal --num-docs=100
TODAY=$(date -u +%Y%m%d) 
uv run -m src.process_legal --file-dir="s3a://raw/docs/legal/*/$TODAY/**/*" --table-name=legal.documents --mode=batch


# without spark connect 

# uv run scripts/submit.py --file-path=src/create_tables.py
# uv run scripts/submit.py --file-path=src/generate_legal_docs.py

# For all files from today (use UTC to match Python code)
# TODAY=$(date -u +%Y%m%d)
# uv run scripts/submit.py --file-path=src/insert_legal.py --args="--file-path=s3a://data/docs/legal/*/$TODAY/ --table-name=legal.documents --mode=batch"
