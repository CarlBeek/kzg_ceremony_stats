# Statistics for the KZG Ceremony

A repo for statistics around the KZG ceremony. Primarily around analysing how many bots there are and whether they can be detected.

## Setup

```bash
python3 -m venv venv
. venv/bin/activate
pip install -r requirements.txt
```

## Downloading the data

Download the latest transcript to and fetch all the nonce data from a connected (archival) RPC endpoint: (Takes a handful of hours depending on your RPC node.)

```bash
python cli.py load_new_data --fetch_new_transcript --patch_missing_data
```

## Statistics

Calculate some stats and print them + save some plots to the `./plots` directory.

```bash
python cli.py calculate-stats
```
