import dlt
import argparse
from settings import *
from helpers import *

GAME_URL = BASE_URL + NHL_SLUG + '/summary' #?event=
SCOREBOARD_URL = BASE_URL + NHL_SLUG + '/scoreboard' #?dates=YYYYMMDD
PIPELINE_NAME = 'headcount'
TARGET = 'snowflake'

# The games URL takes dates in YYYYMMDD format and returns the events that occured
# on that day. This generates a list of all the days to process to better help with paralellization
# for fetching the games on that date.
@dlt.resource(merge_key='season_day', write_disposition='merge',parallelized=True)
def season_days():
        for year in years:
            yield calendar_dates(year, SCOREBOARD_URL)

# Games is a transformer, just like season_days. It takes season_day as an input and 
# then makes the request for that date.
@dlt.transformer(write_disposition='merge',merge_key='id',data_from=season_days,parallelized=True)
def games(day_record):
    for day in day_record:
        yield fetch_games(day, SCOREBOARD_URL)

# Game details fetches EVERYTHING from the game summary endpoint.
# dlt does a great job of normalizing this data and breaking it out
# into nested tables. Next step would be to clean up using dbt within the project.
@dlt.transformer(write_disposition='merge',merge_key='id',data_from=games,parallelized=True)
def game_details(game_record):
    for game in game_record:
        yield fetch_game_details(game['id'], GAME_URL)

# Make PICKCENTER Separate because we want to track those changes.
@dlt.transformer(write_disposition={"disposition": "merge", "strategy": "scd2"},merge_key='id',data_from=games,parallelized=True)
def picks(game_record):
    for game in game_record:
        yield fetch_picks(game['id'],GAME_URL)

# Pipelines build sources - return the above tagged functions. dlt does the rest.
@dlt.source(name='headcount')
def headcount_source():
    return [season_days,games,game_details,picks]

pipeline = dlt.pipeline(
    pipeline_name=PIPELINE_NAME,
    progress='enlighten',
    destination=TARGET,
    dataset_name=PIPELINE_NAME
)

# You can get away with __main__, but this allows you to call the pipeline with some
# arguments from the command line. While this is writing to duckdb, you could easily
# have it write to parquet and save to your GitHub account and leverage GitHub Actions
# To automate that.
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a list of years.")
    parser.add_argument("--start_year", type=int, help="Start year (default: 2014)")
    parser.add_argument("--end_year", type=int, help="End year (default: current year)")
    parser.add_argument("--years_to_fill", type=int, help="Number of years to fill")
    parser.add_argument('--load_year', type=int, help="Load year")
    
    args = parser.parse_args()
    
    years = generate_years_list(args.start_year, args.end_year, args.years_to_fill,args.load_year)
    print("Loading the following years: ") 
    print(years)
    source = headcount_source()
    load_info = pipeline.run(source)
    print(load_info)