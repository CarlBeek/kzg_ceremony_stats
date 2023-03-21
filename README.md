# Statistics for the KZG Ceremony

A repo for statistics around the KZG ceremony. Primarily around analysing how many bots there are and whether they can be detected.

<iframe width="100%" height="500" src="https://htmlpreview.github.io/?https://raw.githubusercontent.com/CarlBeek/kzg_ceremony_stats/master/plots/nonce_ecdf.html" frameborder="0"></iframe>

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
