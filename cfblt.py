import datetime
import requests
import json
import dlt
import argparse

GAME_URL = 'https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary' #?event=
SCOREBOARD_URL = 'https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard' #?dates=YYYYMMDD

def generate_years_list(start_year=None, end_year=None, years_to_fill=None,load_year=None):
    current_year = datetime.datetime.now().year
    
    if load_year is not None:
        return [load_year]
    # Set default start_year if not provided
    if start_year is None:
        start_year = 2014
    
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

years = generate_years_list()
    
@dlt.resource(primary_key='start_date',write_disposition='append',parallelized=True)
def seasons():
    for year in years:

        params = {'dates': str(year)+'1001'}
        req = json.loads(requests.get(url = SCOREBOARD_URL, params=params).text)
        cal = req["leagues"][0]["calendar"]
        for item in cal:
                if 'entries' in item:
                    del item['entries']
        yield cal

@dlt.resource(primary_key='start_date',write_disposition='merge',parallelized=True)
def weeks():
    for year in years:

        params = {'dates': str(year)+'1001'}
        req = json.loads(requests.get(url = SCOREBOARD_URL, params=params).text)
        cal = req["leagues"][0]["calendar"]
        for item in cal:
            if 'entries' in item:
                weeks = item['entries']
                yield weeks


@dlt.transformer(primary_key='season_day', write_disposition='merge', data_from=seasons,parallelized=True)
def season_days(season_record):
    for season in season_record:
        date_cursor = datetime.datetime.strptime(season['startDate'], "%Y-%m-%dT%H:%MZ")
        cursor_end = datetime.datetime.strptime(season['endDate'], "%Y-%m-%dT%H:%MZ")
        while date_cursor.date() <= cursor_end.date():
            yield { 'season_day' : date_cursor.strftime('%Y%m%d')}
            date_cursor += datetime.timedelta(days=1)

@dlt.transformer(write_disposition='merge',primary_key='id',data_from=season_days,parallelized=True)
def games(day_record):
    params = {'dates': day_record['season_day']}
    req  = json.loads(requests.get(url  = SCOREBOARD_URL, params=params).text)
    if "events" in req and req['events']:
        for event in req['events']:
            if 'id' in event and event['id']:
                yield event

@dlt.transformer(write_disposition='merge',primary_key='id',data_from=games,parallelized=True)
def game_details(game_record):
    params = { 'event': game_record['id'] }
    req   = json.loads(requests.get(url= GAME_URL, params=params).text)
    if 'header' in req and 'id' in req['header']:
        req['id'] = req['header']['id']
        yield req

@dlt.source(name='cfblt')
def cfblt_source():
    return [seasons,weeks,season_days,games,game_details]

pipeline = dlt.pipeline(
      pipeline_name='cfblt',
      destination='duckdb',
      progress='enlighten',
      dataset_name='raw'
      )

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
    load_info = pipeline.run(cfblt_source())
    print(load_info)