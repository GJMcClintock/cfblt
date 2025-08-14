import datetime
from settings import *
from dlt.sources.helpers import requests
import dlt
import json
import pandas as pd

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


def generate_dates_list(start_year=None, end_year=None, years_to_fill=None,load_year=None):
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
def make_date_range(season):
        dates = []
        date_cursor = datetime.datetime.strptime(season['startDate'], "%Y-%m-%dT%H:%MZ")
        cursor_end = datetime.datetime.strptime(season['endDate'], "%Y-%m-%dT%H:%MZ")
        while date_cursor.date() <= cursor_end.date():
            dates.append({ 'season_day' : date_cursor.strftime('%Y%m%d')})
            date_cursor += datetime.timedelta(days=1)
        return dates

@dlt.defer
def calendar_dates(year, scoreboard_url):
    dates = []
    params = {'dates': str(year)+'1001'}
    req = json.loads(requests.get(url = scoreboard_url, params=params).text)
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
    return dates

@dlt.defer
def baseball_dates(year, scoreboard_url):
    dates = []
    params = {'dates': str(year)+'0601'}
    req = json.loads(requests.get(url = scoreboard_url, params=params).text)
    season = req["leagues"][0]
    date_cursor = datetime.datetime.strptime(season['calendarStartDate'], "%Y-%m-%dT%H:%MZ")
    cursor_end = datetime.datetime.strptime(season['calendarEndDate'], "%Y-%m-%dT%H:%MZ")
    while date_cursor.date() <= cursor_end.date():
            dates.append({ 'season_day' : date_cursor.strftime('%Y%m%d')})
            date_cursor += datetime.timedelta(days=1)
    return dates

@dlt.defer
def fetch_football_seasons(year, scoreboard_url):
        params = {'dates': str(year)+'1001'}
        req = json.loads(requests.get(url = scoreboard_url, params=params).text)
        cal = req["leagues"][0]["calendar"]
        for item in cal:
                if 'entries' in item:
                    del item['entries']
        return cal

@dlt.defer
def fetch_football_weeks(year, scoreboard_url):
        params = {'dates': str(year)+'1001'}
        req = json.loads(requests.get(url = scoreboard_url, params=params).text)
        cal = req["leagues"][0]["calendar"]
        for item in cal:
            if 'entries' in item:
                weeks = item['entries']
                return weeks

@dlt.defer
def fetch_games(day_record, scoreboard_url):
    games = []
    params = {'dates': day_record['season_day']}
    req = json.loads(requests.get(url=scoreboard_url, params=params).text)
    if "events" in req and req['events']:
        for event in req['events']:
            if 'id' in event and event['id']:
                games.append(event)
    return games

@dlt.defer
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

@dlt.defer
def fetch_picks(game_id, game_url):
    picks = []
    params = {'event': game_id}
    try:
        req = json.loads(requests.get(url=game_url, params=params).text)
        if 'header' in req and 'id' in req['header']:
            pick = req['pickcenter'] if 'pickcenter' in req else None
            pick = {'pickcenter': pick}
            pick['id'] = req['header']['id']
            picks.append(pick)
    except:
        pass
    return picks

@dlt.defer
def fetch_golf_data(date, scoreboard_url):
    params = {'dates': str(date)}
    try:
        req = json.loads(requests.get(url=scoreboard_url, params=params).text)
        if 'events' in req:
            tournaments = pd.DataFrame(req['events'])
            tournaments['date'] = date
            # Drop rows where id is null or NaN
            tournaments = tournaments.dropna(subset=['id'])
            return tournaments.to_dict(orient='records')
    except Exception as e:
        pass