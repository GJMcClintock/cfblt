import datetime
from dlt.sources.helpers import requests
import json
import dlt
import argparse

GAME_URL = 'https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary' #?event=
SCOREBOARD_URL = 'https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard' #?dates=YYYYMMDD

keys_to_pop = ['pickcenter', 'lastFiveGames', 'news', 'ticketsinfo', 'meta', 'standings']

def generate_years_list(start_year=None, end_year=None, years_to_fill=None,load_year=None):
    current_year = datetime.datetime.now().year
    
    if load_year is not None:
        return [load_year]
    # Set default start_year if not provided
    if start_year is None:
        start_year = DEFAULT_START_YEAR
    
    # Determine end_year
    if end_year is None:
        if years_to_fill is not None:
            end_year = min(start_year + years_to_fill - 1, current_year)
        else:
            end_year = current_year
    
    # Ensure end_year is not greater than current_year
    end_year = min(end_year, current_year)
    
    # Generate and return the list of years
    return list(range(start_year, end_year + 1))

# The games URL takes dates in YYYYMMDD format and returns the events that occured
# on that day. This generates a list of all the days to process to better help with paralellization
# for fetching the games on that date.
@dlt.resource(merge_key='season_day', write_disposition='merge',parallelized=True)
def season_days():
        dates = []
        for year in years:

            params = {'dates': str(year)+'1001'}
            req = json.loads(requests.get(url = SCOREBOARD_URL, params=params).text)
            cal = req["leagues"][0]["calendar"]
            for item in cal:
                date = datetime.datetime.strptime(item, "%Y-%m-%dT%H:%MZ")
                formatted_date = date.strftime('%Y%m%d')
                dates.append(
                    {   
                        'season' : year,
                        'date': date,
                        'season_day': formatted_date
                    }
                )
        yield dates

# Games is a transformer, just like season_days. It takes season_day as an input and 
# then makes the request for that date.
@dlt.transformer(write_disposition='merge',merge_key='id',data_from=season_days,parallelized=True)
def games(day_record):
    for day in day_record:
        params = {'dates': day['season_day']}
        req  = json.loads(requests.get(url  = SCOREBOARD_URL, params=params).text)
        if "events" in req and req['events']:
            for event in req['events']:
                if 'id' in event and event['id']:
                    yield event

# Game details fetches EVERYTHING from the game summary endpoint.
# dlt does a great job of normalizing this data and breaking it out
# into nested tables. Next step would be to clean up using dbt within the project.
@dlt.transformer(write_disposition='merge',merge_key='id',data_from=games,parallelized=True)
def game_details(game_record):
    params = { 'event': game_record['id'] }
    try:
        req   = json.loads(requests.get(url= GAME_URL, params=params).text)
        if 'header' in req and 'id' in req['header']:
            req['id'] = req['header']['id']
            for key in keys_to_pop:
                if key in req:
                    req.pop(key)
            yield req
    except:
        pass

# Make PICKCENTER Separate because we want to track those changes.
@dlt.transformer(write_disposition={"disposition": "merge", "strategy": "scd2"},merge_key='id',data_from=games,parallelized=True)
def picks(game_record):
    params = { 'event': game_record['id'] }
    try:
        req   = json.loads(requests.get(url= GAME_URL, params=params).text)
        if 'header' in req and 'id' in req['header']:
            pick = req['pickcenter'] if 'pickcenter' in req else None
            pick = {'pickcenter': pick}
            pick['id'] = req['header']['id']
            yield pick
    except:
        pass



# Pipelines build sources - return the above tagged functions. dlt does the rest.
@dlt.source(name='metrics')
def metrics_source():
    return [season_days,games,game_details,picks]

pipeline = dlt.pipeline(
      pipeline_name='metrics',
      progress='enlighten',
      destination='snowflake',
      dataset_name="metrics"
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
    load_info = pipeline.run(metrics_source())
    print(load_info)