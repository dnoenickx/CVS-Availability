from states import get_state_abbreviation
from datetime import datetime
from time import sleep
import urllib.request
import pandas as pd
import json
import time
import os


# Obtained from: https://www.cvs.com/immunizations/covid-19-vaccine
# On March 26, 2021 at 5:00 AM EST
SUPPORTED_STATES = [
    'Alabama', 'Arizona', 'Arkansas', 'California', 'Colorado',
    'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Illinois',
    'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maryland',
    'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri',
    'Montana', 'Nevada', 'New Jersey', 'New York', 'North Carolina',
    'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania',
    'Puerto Rico', 'Rhode Island', 'South Carolina', 'Texas', 'Utah',
    'Vermont', 'Virginia']


def filename(state):
    """Returns the filename for the given state's data"""
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return f'{dir_path}/data/cvs_availability_{state.lower()}.csv'


def time_string():
    """Returns a formatted UTC timestamp"""
    return f"{datetime.utcnow():%Y-%m-%d %H:%M} UTC"


def fetch_data(state, verbose=False):
    """Fetches, from CVS website, vaccine appointment availability for the 
       given state. Returns data in a pandas dataframe. Can optionally print
       out the names of the locations with availability. """
    new_data = []
    url = "https://www.cvs.com/immunizations/covid-19-vaccine/immunizations/" \
          f"covid-19-vaccine.vaccine-status.{state.lower()}.json?vaccineinfo"
    with urllib.request.urlopen(url) as page:
        data = json.loads(page.read().decode())
        avail_count = 0
        for site in data['responsePayloadData']['data'][state.upper()]:
            city = site['city'].title()
            status = site['status'] == 'Available'
            new_data.append([city, status])
            if verbose and status:
                print(f"  • {city}")
                avail_count += 1
        if avail_count == 0:
            print('    (None)')
        elif not verbose:
            print('    {avail_count} sites with availability!')
    df = pd.DataFrame(new_data, columns=['City', time_string()])
    df.set_index('City', inplace=True)
    return df


def load_data(state):
    """Loads data for a given state. Catches if there is no prior data to load."""
    try:
        data = pd.read_csv(filename(state))
        data.set_index('City', inplace=True)
    except FileNotFoundError:
        print(f"    (no data to load for {state.upper()})")
        data = pd.DataFrame()
    return data


def update(state, verbose=False):
    """ Fetches and appends new data to the given states csv. """
    print(state.title())
    try:
        state = get_state_abbreviation(state)
    except KeyError:
        return
    old_data = load_data(state)
    new_data = fetch_data(state, verbose=verbose)
    
    merged_data = old_data.merge(new_data, how='outer', 
                                 left_index=True, right_index=True)
    merged_data.to_csv(filename(state), index_label='City')


def update_all(delay):
    """ Updates csv for every state listed in SUPPORTED_STATES. """
    for state in SUPPORTED_STATES:
        sleep(delay)
        update(state, verbose=True)


while True:
    update_all(1)
    time.sleep(600)

