
# price_tracker

## Data Scrapping

A scrapping bot to check price changing with Kenya heading to a general election and the soaring prices of household items.

HOW TO:

- Install packages using `pip install --no-cache-dir -r requirements.txt`
- To run the script you only need navigate to the root of the repo and run using:
  - `python run.py`  to run all existing working crawlers
  - `python run.py --crawlers jumia --crawlers carrefour` to run only these two crawlers

## Dockerization

- For the scraper use these commands:
  - using only dokcer file `docker build -t price_tracker . -f Dockerfile.scraper`
  - using docker compose `docker build -t price_tracker . -f Dockerfile.scraper`
  
- To run the scrapper from docker run it with the commands:
  - `docker run --rm -v datasets:/datasets price_tracker /bin/bash -c "python run.py"`

- All containers networked under one network `price_tracker` created via command `docker network create price_tracker`

## Data Modelling

- Added in a data modelling solution using dbt in the directory `dbt_price_tracker`.
- This should be able to help use develop data marts in a more repeatable and version controlled way.
- Currently we have managed to finish the initial prototype for jumia data mart based on the data we have.
- Next steps would be to refine the logic and add carrefour as the next data source.
- Also look in to a possibility of having both data in one fact table.  

## Batch Processing

- Added both Pandas and PySpark batch processing into the existing Postgres database
- The scripts have prefix indicating tool used (`pandas_inges.py`, `pyspark_ingest.py`)

## Ongoing

- Data Marting Solution using DBT dbt_price_tracker
- Dockerization of batch processing and dbt data marting
- Added Apache Superset Business Intelligence Tool and connected it to the existing database.

## TO DO

- Add cloud storage pipeline
- Add reporting pipeline (batch processing with PySpark staged on cloud serverless database e.g Athena)
- Add analysis solution.
