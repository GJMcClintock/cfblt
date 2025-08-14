import datetime
from settings import *
from dlt.sources.helpers import requests
import json

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

def fetch_game_details(game_id, game_url):
    params = {'event': game_id}
    try:
        req = json.loads(requests.get(url=game_url, params=params).text)
        if 'header' in req and 'id' in req['header']:
            req['id'] = req['header']['id']
            for key in KEYS_TO_POP:
                if key in req:
                    req.pop(key)
            return req
    except:
        pass