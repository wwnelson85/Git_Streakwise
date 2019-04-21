import numpy as np
import pandas as pd
import requests
import datetime
import warnings
import io

def validate_datestring(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")

def sanitize_input(start_dt, end_dt):
    # if no dates are supplied, assume they want yesterday's data
    # send a warning in case they wanted to specify
    if start_dt is None and end_dt is None:
        today = datetime.datetime.today()
        start_dt = (today - datetime.timedelta(1)).strftime("%Y-%m-%d")
        end_dt = today.strftime("%Y-%m-%d")
        print("Warning: no date range supplied. Returning yesterday's Statcast data. For a different date range, try get_statcast(start_dt, end_dt).")
    #if only one date is supplied, assume they only want that day's stats
    #query in this case is from date 1 to date 1
    if start_dt is None:
        start_dt = end_dt
    if end_dt is None:
        end_dt = start_dt
    # now that both dates are not None, make sure they are valid date strings
    validate_datestring(start_dt)
    validate_datestring(end_dt)
    return start_dt, end_dt

def single_game_request(game_pk):

    url = "https://baseballsavant.mlb.com/statcast_search/csv?all=true&type=details&game_pk={game_pk}".format(game_pk=game_pk)
    s=requests.get(url, timeout=None).content
    data = pd.read_csv(io.StringIO(s.decode('utf-8')))#, error_bad_lines=False) # skips 'bad lines' breaking scrapes. still testing this.
    return data

def small_request(start_dt,end_dt):
    url = "https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=R%7CPO%7CS%7C=&hfSea=&hfSit=&player_type=pitcher&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={}&game_date_lt={}&team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0&type=details&".format(start_dt, end_dt)
    s=requests.get(url, timeout=None).content
    data = pd.read_csv(io.StringIO(s.decode('utf-8')))#, error_bad_lines=False) # skips 'bad lines' breaking scrapes. still testing this.
    return data

def postprocessing(data, team):
    #replace empty entries and 'null' strings with np.NaN
    data.replace(r'^\s*$', np.nan, regex=True, inplace = True)
    data.replace(r'^null$', np.nan, regex=True, inplace = True)

    # convert columns to numeric
    not_numeric = ['sv_id', 'umpire', 'type', 'inning_topbot', 'bb_type', 'away_team', 'home_team', 'p_throws', 
                   'stand', 'game_type', 'des', 'description', 'events', 'player_name', 'game_date', 'pitch_type', 'pitch_name']

    numeric_cols = ['release_speed','release_pos_x','release_pos_z','batter','pitcher','zone','hit_location','balls',
                    'strikes','game_year','pfx_x','pfx_z','plate_x','plate_z','on_3b','on_2b','on_1b','outs_when_up','inning',
                    'hc_x','hc_y','fielder_2','vx0','vy0','vz0','ax','ay','az','sz_top','sz_bot',
                    'hit_distance_sc','launch_speed','launch_angle','effective_speed','release_spin_rate','release_extension',
                    'game_pk','pitcher.1','fielder_2.1','fielder_3','fielder_4','fielder_5',
                    'fielder_6','fielder_7','fielder_8','fielder_9','release_pos_y',
                    'estimated_ba_using_speedangle','estimated_woba_using_speedangle','woba_value','woba_denom','babip_value',
                    'iso_value','launch_speed_angle','at_bat_number','pitch_number','home_score','away_score','bat_score',
                    'fld_score','post_away_score','post_home_score','post_bat_score','post_fld_score']

    data[numeric_cols] = data[numeric_cols].astype(float)

    # convert date col to datetime data type and sort so that this returns in an order that makes sense (by date and game)
    data['game_date'] = pd.to_datetime(data['game_date'], format='%Y-%m-%d')
    data = data.sort_values(['game_date', 'game_pk', 'at_bat_number', 'pitch_number'], ascending=False)

    #select only pitches from a particular team
    valid_teams = ['MIN', 'PHI', 'BAL', 'NYY', 'LAD', 'OAK', 'SEA', 'TB', 'MIL', 'MIA',
       'KC', 'TEX', 'CHC', 'ATL', 'COL', 'HOU', 'CIN', 'LAA', 'DET', 'TOR',
       'PIT', 'NYM', 'CLE', 'CWS', 'STL', 'WSH', 'SF', 'SD', 'BOS','ARI','ANA','WAS']

    if(team in valid_teams):
        data = data.loc[(data['home_team']==team)|(data['away_team']==team)]
    elif(team != None):
        raise ValueError('Error: invalid team abbreviation. Valid team names are: {}'.format(valid_teams))
    data = data.reset_index()
    return data

def statcast(start_dt=None, end_dt=None, team=None, verbose=True):
    """
    Pulls statcast play-level data from Baseball Savant for a given date range.
    INPUTS:
    start_dt: YYYY-MM-DD : the first date for which you want statcast data
    end_dt: YYYY-MM-DD : the last date for which you want statcast data
    team: optional (defaults to None) : city abbreviation of the team you want data for (e.g. SEA or BOS)
    If no arguments are provided, this will return yesterday's statcast data. If one date is provided, it will return that date's statcast data.
    """


    start_dt, end_dt = sanitize_input(start_dt, end_dt)
    # 3 days or less -> a quick one-shot request. Greater than 3 days -> break it into multiple smaller queries
    small_query_threshold = 5
    # inputs are valid if either both or zero dates are supplied. Not valid of only one given.


    if start_dt and end_dt:
        # how many days worth of data are needed?
        date_format = "%Y-%m-%d"
        d1 = datetime.datetime.strptime(start_dt, date_format)
        d2 = datetime.datetime.strptime(end_dt, date_format)
        days_in_query = (d2 - d1).days
        if days_in_query <= small_query_threshold:
            data = small_request(start_dt,end_dt)
        else:
            data = large_request(start_dt,end_dt,d1,d2,step=small_query_threshold,verbose=verbose)

        data = postprocessing(data, team)
        
        return data
    
def statcast_single_game(game_pk, team=None):
    """
    Pulls statcast play-level data from Baseball Savant for a single game,
    identified by its MLB game ID (game_pk in statcast data)
    INPUTS:
    game_pk : 6-digit integer MLB game ID to retrieve
    """
    data = single_game_request(game_pk)
    data = postprocessing(data, team)

def get_lookup_table():
    print('Gathering player lookup table. This may take a moment.')
    url = "https://raw.githubusercontent.com/chadwickbureau/register/master/data/people.csv"
    s=requests.get(url).content
    table = pd.read_csv(io.StringIO(s.decode('utf-8')), dtype={'key_sr_nfl': object, 'key_sr_nba': object, 'key_sr_nhl': object})
    #subset columns
    cols_to_keep = ['name_last','name_first','key_mlbam', 'key_retro', 'key_bbref', 'key_fangraphs', 'mlb_played_first','mlb_played_last']
    table = table[cols_to_keep]
    #make these lowercase to avoid capitalization mistakes when searching
    table['name_last'] = table['name_last'].str.lower()
    table['name_first'] = table['name_first'].str.lower()
    # Pandas cannot handle NaNs in integer columns. We need IDs to be ints for successful queries in statcast, etc. 
    # Workaround: replace ID NaNs with -1, then convert columns to integers. User will have to understand that -1 is not a valid ID.
    table[['key_mlbam', 'key_fangraphs']] = table[['key_mlbam', 'key_fangraphs']].fillna(-1)
    table[['key_mlbam', 'key_fangraphs']] = table[['key_mlbam', 'key_fangraphs']].astype(int) # originally returned as floats which is wrong
    return table


''' START OF WWN CODE '''

player_id = get_lookup_table()

import datetime as dt
import pandas as pd
import os

# parameters
today = dt.date.today()
start_date = '2015-01-01'
end_date = today

upload = 'on'

person = 'willy'
directory_willy = '/Users/williamnelson/Documents/streakwise/statcast_raw/'
directory_sonny = '/Users/sonnynelson/Documents/PyBaseball/'

if person == 'willy':
    directory = directory_willy
else:  
    directory = directory_sonny

filename = 'statcast_history_1.03.csv'

# get a datestring to use to cycle through to collect data
date_string = pd.date_range(start_date, end_date, freq='D')


for day in date_string:

    loop_start_date = day.strftime("%Y-%m-%d")

    data_collect = statcast(start_dt=loop_start_date, end_dt=loop_start_date, verbose=True)
    data_collect = data_collect.rename(columns={'player_name':'pitcher_name','batter':'batter_id','pitcher':'pitcher_id'})
   
    data_collect = pd.merge(data_collect,player_id[['name_last','name_first','key_mlbam']],how='left',left_on=['batter_id'],right_on=['key_mlbam'])
    data_collect['batter_name'] = data_collect['name_first'] + ' ' + data_collect['name_last']
    data_collect['batter_name'] = data_collect.batter_name.str.title()
    
    if upload == 'on':
        
        # if file does not exist write header 
        if not os.path.isfile(directory + filename):
           data_collect.to_csv(directory + filename, header='column_names', sep='|')
        else: # else it exists so append without writing the header
           data_collect.to_csv(directory + filename, mode='a', header=False, sep='|')
    
    if data_collect.shape[0] == 0:
        print(day.strftime("%Y-%m-%d") + ' n/a')
    else:
        print(day.strftime("%Y-%m-%d"))