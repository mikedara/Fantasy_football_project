"""
Pulls historical NFL / fantasy football data from nflverse and lands it
in BigQuery as raw tables, ready for dbt to build staging/mart models on top of.

Run manually with:
    python ingest_nflverse.py

Requires:
    pip install nflreadpy pyarrow google-cloud-bigquery

Auth: uses Application Default Credentials, i.e. whatever you set up with
    gcloud auth application-default login
No service account key needed.
"""

import nflreadpy as nfl
from google.cloud import bigquery
import polars as pl

# --- config -----------------------------------------------------------
PROJECT_ID = "fantasy-football-db-506621"
DATASET = "raw_nflverse"
LOCATION = "US"
# ------------------------------------------------------------------------

client = bigquery.Client(project=PROJECT_ID)


def ensure_dataset() -> None:
    """Create the raw dataset if it doesn't already exist."""
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET}")
    dataset_ref.location = LOCATION
    client.create_dataset(dataset_ref, exists_ok=True)
    print(f"Dataset {PROJECT_ID}.{DATASET} ready.")


def write_table(df: pl.DataFrame, table_name: str) -> None:
    """Full-refresh load a polars DataFrame into a BigQuery table."""
    if df.is_empty():
        print(f"  Skipping {table_name} — no rows returned.")
        return

    pandas_df = df.to_pandas()
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # full refresh each run
        autodetect=True,
    )
    job = client.load_table_from_dataframe(pandas_df, table_id, job_config=job_config)
    job.result()  # wait for completion
    print(f"  Loaded {len(pandas_df):,} rows -> {table_id}")


def main() -> None:
    ensure_dataset()

    print("\nLoading player_stats (all seasons, 1999+)...")
    player_stats = nfl.load_player_stats(seasons=True)
    write_table(player_stats, "player_stats")

    print("\nLoading players (player dimension)...")
    players = nfl.load_players()
    write_table(players, "players")

    print("\nLoading rosters_weekly (all seasons)...")
    rosters_weekly = nfl.load_rosters_weekly(seasons=True)
    write_table(rosters_weekly, "rosters_weekly")

    print("\nLoading ff_rankings (2019+ ADP/ranking archives)...")
    try:
        ff_rankings = nfl.load_ff_rankings()
        write_table(ff_rankings, "ff_rankings")
    except Exception as e:
        print(f"  Could not load ff_rankings: {e}")

    print("\nDone. All tables landed in raw_nflverse.")


if __name__ == "__main__":
    main()