from settings import *
from helpers import *
import dlt
import argparse
import pandas as pd

SCOREBOARD_URL = BASE_URL + PGA_SLUG + '/scoreboard' #?dates=YYYYMMDD
PIPELINE_NAME = 'growth'
TARGET = 'snowflake'
pipeline = dlt.pipeline(
      pipeline_name=PIPELINE_NAME,
      progress='enlighten',
      destination=TARGET,
      dataset_name=PIPELINE_NAME
    )

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