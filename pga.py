import datetime
from dlt.sources.helpers import requests
import json
import dlt
import argparse
import pandas as pd

SCOREBOARD_URL = 'https://site.api.espn.com/apis/site/v2/sports/golf/pga/scoreboard' #?dates=YYYYMMDD

pipeline = dlt.pipeline(
      pipeline_name='growth',
      progress='enlighten',
      destination='snowflake',
      dataset_name="growth"
      )


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
        # Generate and return the list of years

    return list(range(start_year, end_year + 1))

def generate_dates_list(start_year=None, end_year=None, years_to_fill=None,load_year=None):
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
    # Initialize list to store dates
    dates_list = []

    # Create a date range from start_year to end_year (inclusive)
    for year in range(start_year, end_year + 1):
        # Start from January 1st of the year
        current_date = datetime.date(year, 1, 1)
        
        # Continue until we're in the next year
        while current_date.year == year and current_date <= datetime.date.today():
            # If it's a Sunday (weekday 6 in Python's datetime)
            if current_date.weekday() == 6:
                # Format as YYYYMMDD and add to the list
                date_str = current_date.strftime("%Y%m%d")
                dates_list.append(int(date_str))
            
            # Move to next day
            current_date += datetime.timedelta(days=1)

    return dates_list

@dlt.defer
def fetch_golf_data(date):
    params = {'dates': str(date)}
    try:
        req = json.loads(requests.get(url=SCOREBOARD_URL, params=params).text)
        if 'events' in req:
            tournaments = pd.DataFrame(req['events'])
            tournaments['date'] = date
            # Drop rows where id is null or NaN
            tournaments = tournaments.dropna(subset=['id'])
            return tournaments.to_dict(orient='records')
    except Exception as e:
        pass

@dlt.resource(
    standalone=True,
    merge_key = 'id',
    write_disposition='merge'
)
def get_growth():
    for date in date_list:
        tournaments = fetch_golf_data(date)
        if tournaments:
            yield tournaments




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
    date_list = generate_dates_list(args.start_year, args.end_year, args.years_to_fill,args.load_year)
    load_info = pipeline.run(get_growth())
    print(load_info)