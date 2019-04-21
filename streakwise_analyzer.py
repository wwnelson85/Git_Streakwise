'''BOX SCORE WRANGLER'''
# this script complies game-by-game box score data and stores in a csv

import datetime as dt
import pandas as pd
import os
from pybaseball import batting_stats_range
from pybaseball import pitching_stats_range
import csv


''' PARAMETERS '''
upload = 'on'
update = 'off'
filename = 'box_scores_v1.00.csv'
person = 'willy'

'''DIRECTORY'''
# establish directory location based on person parameter
directory_willy = '/Users/williamnelson/Documents/streakwise/statcast_raw/'
directory_sonny = '/Users/sonnynelson/Documents/PyBaseball/'
if person == 'willy':
    directory = directory_willy
else:  
    directory = directory_sonny


'''DATE WORK'''
start_date = '2018-01-01'
end_date = '2018-12-31'

# this part finds the most recent date saved in the datafile. used to update.
if (update == 'on') & (upload == 'on'):
    recent_cvs_date = pd.read_csv(directory + filename,sep='|')
    recent_cvs_date = recent_cvs_date[['date']].drop_duplicates().max()[0]
    start_date = recent_cvs_date + pd.DateOffset(1)
    today = dt.date.today()
    end_date = today

# get a datestring to use to cycle through to collect data
date_string = pd.date_range(start_date, end_date, freq='D')


'''SRART COMPILING'''
#loop through all the days
for active_date in date_string:
    
    #change the format of 'date' to a 'string' since that what the website needs
    if (active_date.month > 2) & (active_date.month < 12):
        loop_date = active_date.strftime("%Y-%m-%d")
    
        #try to scrape data, on error go to next day and try again
        try:
            #get stats
            batting = batting_stats_range(loop_date,)
            pitching = pitching_stats_range(loop_date,)
            
            #now clean up column names by adding 'p_' and 'b_' to pitching and batting stats
            batting_specific_columns = [(i,'b_'+i) for i in batting.iloc[:, 6:].columns.values]
            batting.rename(columns = dict(batting_specific_columns), inplace=True)
    
            pitching_specific_columns = [(i,'p_'+i) for i in pitching.iloc[:, 6:].columns.values]
            pitching.rename(columns = dict(pitching_specific_columns), inplace=True)
            
            #now merge the batting and pitching datasets together
            data_collect = pd.concat([batting,pitching], axis=0, ignore_index=True)
            
            #add date column and re-order column order
            data_collect['date'] = loop_date
            data_collect = data_collect[['date', 'Name', 'Tm', 'Age', 'G', 'Lev', 'b_2B', 'b_3B', 'b_AB', 'b_BA', 'b_BB', 'b_CS', 'b_GDP', 'b_H', 'b_HBP', 'b_HR', 'b_IBB', 'b_OBP', 'b_OPS', 'b_PA', 'b_R', 'b_RBI', 'b_SB', 'b_SF', 'b_SH','b_SLG','b_SO', 'p_2B', 'p_3B', 'p_AB', 'p_BAbip', 'p_BB', 'p_BF', 'p_CS', 'p_ER', 'p_ERA', 'p_GB/FB','p_GDP', 'p_GS', 'p_H', 'p_HBP', 'p_HR', 'p_IBB', 'p_IP', 'p_L', 'p_LD', 'p_PO', 'p_PU', 'p_Pit', 'p_R', 'p_SB', 'p_SF', 'p_SO', 'p_SO/W', 'p_SO9', 'p_SV', 'p_StL', 'p_StS', 'p_Str', 'p_W', 'p_WHIP']]
            
            #upload to csv in upload is set to on        
            if upload == 'on':
                
                # if file does not exist write header 
                if not os.path.isfile(directory + filename):
                   data_collect.to_csv(directory + filename, header='column_names',sep='|',index=False)
                else: # else it exists so append without writing the header
                   data_collect.to_csv(directory + filename, mode='a',header=False,sep='|',index=False)
            
            #if there are no rows in the dataframe (no data) print an 'n/a', else print the date
            if data_collect.shape[0] == 0:
                print(loop_date + ' n/a')
            else:
                print(loop_date)
        
        # if no data was found print 'n/a', assuming this is offseason
        except:
            print(loop_date + ' n/a')