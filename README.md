
# price_tracker

A scrapping bot to check price changing with Kenya heading to a general election and the soaring prices of household items.

HOW TO:

- Install packages using `pip install --no-cache-dir -r requirements.txt`
- To run the script you only need navigate to the root of the repo and run using:
  - `python run.py`  to run all existing working crawlers
  - `python run.py --crawlers jumia --crawlers carrefour` to run only these two crawlers

## Buld docker image

- For the scraper use these commands:
  - using only dokcer file `docker build -t price_tracker . -f Dockerfile.scraper`
  - using docker compose `docker build -t price_tracker . -f Dockerfile.scraper`
  
- To run the scrapper from docker run it with the commands:
  - `docker run --rm -v datasets:/datasets price_tracker /bin/bash -c "python run.py"`

## TO DO

- Dockerize the app
- Add cloud storage pipeline
- Add database pipeline
- Add reporting pipeline
- Add data marting solution
- Add analysis solution.
