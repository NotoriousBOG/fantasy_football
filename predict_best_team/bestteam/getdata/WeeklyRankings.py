# python WeeklyRankings.py YYYY W; where YYYY is the year, W is the Week
# Pulls from https://www.fantasypros.com/nfl/rankings/qb.php?week=1
#
from bs4 import BeautifulSoup
import urllib2
import numpy as np
import sys
import psycopg2
from psycopg2.extensions import AsIs
import traceback
import csv

def write_to_db(data):
    try:
        conn = psycopg2.connect("dbname='test_db' user='bogdan'")
    except psycopg2.Error as e:
        print "I am unable to connect to the database"
        print e
        print e.pgcode
        print e.pgerror
        print traceback.format_exc()
    cur = conn.cursor()
    for player in data:
        columns = player.keys()
        values = [player[column] for column in columns]
        insert_statement = 'insert into weekly_rankings (%s) values %s'

        cur.execute(insert_statement, (AsIs(','.join(columns)), tuple(values)))

    cur.execute("""DELETE FROM weekly_rankings a
                    WHERE a.ctid <> (SELECT min(b.ctid)
                        FROM weekly_rankings b
                        WHERE (a.name = b.name and a.team = b.team
                        and a.season = b.season
                        and a.week = b.week));""")
    conn.commit()


def get_source(url):
    
    username = 'notoriousbog'
    password = 'TBD'
    # a great password

    passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
    # this creates a password manager
    passman.add_password(None, url, username, password)
    # because we have put None at the start it will always
    # use this username/password combination for  urls
    # for which `url` is a super-url

    authhandler = urllib2.HTTPBasicAuthHandler(passman)
    # create the AuthHandler
    
    opener = urllib2.build_opener(authhandler)
    
    urllib2.install_opener(opener)
    # All calls to urllib2.urlopen will now use our handler
    # Make sure not to include the protocol in with the URL, or
    # HTTPPasswordMgrWithDefaultRealm will be very confused.
    # You must (of course) use it when fetching the page though.
    
    data = urllib2.urlopen(url).read()
    # authentication is now handled automatically for us
    
    return BeautifulSoup(data, "html.parser")


def generate_all_urls(week):
    position_dict = {'1': 'qb', '2': 'rb', '3': 'wr', '4': 'te'}
    all_urls = []
    for key, value in position_dict.iteritems():
        all_urls.append("https://www.fantasypros.com/nfl/rankings/{0}.php?week={1}".format(value,week))
    return all_urls


#csvfile = open('WeeklyLeaders.csv','a')
#fieldnames= ['name','team','position','season','week','opponent','at_home',
#                 'won_game','team_score','opponent_score','passing_completed',
#                 'passing_attempted','passing_yds','passing_td','passing_int',
#                 'rushing_attempts','rushing_yds','rushing_td',
#                 'receiving_receptions','receiving_yds','receiving_td','receiving_targets',
#                 'two_point_conv','fumbles','total_returned_tds','total_points']
#writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
#writer.writeheader()  #because of the append method, currently it is written for every page it downloads
#csvfile.close()
    
def get_data_from_source(source, season, week):
    table = []
#    csvfile = open('WeeklyProjections.csv','a')
#    fieldnames= ['name','team','position','season','week','opponent','at_home',
#                 'won_game','team_score','opponent_score','passing_completed',
#                 'passing_attempted','passing_yds','passing_td','passing_int',
#                 'rushing_attempts','rushing_yds','rushing_td',
#                 'receiving_receptions','receiving_yds','receiving_td','receiving_targets',
#                 'two_point_conv','fumbles','total_returned_tds','total_points']
#    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
    #writer.writeheader()  #because of the append method, currently it is written for every page it downloads

    teams = ['Ari','Atl','Bal','Buf','Car','Chi','Cin','Cle','Dal','Den','Det','GB','Hou','Ind','Jax','KC','LA','Mia','Min','NE','NO','NYG','NYJ','Oak','Phi','Pit','SD','Sea','SF','TB','Ten','Wsh']

    for tr in source.find_all('tr')[1:]: # looking for rows in a table; probably skipping to the 1st row; source is probably something returned from BeautifulSoup
        
        tds = tr.find_all('td') # finds individual cells and adds them to some list or array
        length_tds = len(tds)
     
        if length_tds == 7:     # tds should have 7 columns for each row. For some reason the last row is not 7 and doesn't contain any relevant data so ignore it
            player_dict = {}    # starting a blank dictionary
            try:
                player_info = tds[1].text.split(" ")
                       
                if any(player_info[3]==x for x in teams)==True:     # check to see if the 4th position is a team instead of something else; If so then the players name is 
                    player_dict['name'] = player_info[0].strip() + ' ' + player_info[1].strip() + ' ' +player_info[2]
                    player_dict['team'] = player_info[3]
                else:
                    player_dict['team'] = player_info[2].strip()
                    player_dict['name'] = player_info[0].strip() + ' ' + player_info[1].strip()
            except:
                print 'exception'
                #player_info = tds[1].text.split(" ")
                #player_dict['name'] = player_info[1]
                #player_dict['team'] = player_info[1]
                #player_dict['position'] = "D"
    
    
            player_dict['season'] = season
            player_dict['week'] = week
            
            # Ranks
            player_dict['rank'] = int(tds[0].text)
            player_dict['best'] = int(tds[3].text)
            player_dict['worst'] = int(tds[4].text)
            player_dict['rank_avg'] = float(tds[5].text)
            player_dict['rank_stdv'] = float(tds[6].text)
             
            table.append(player_dict)
        else:
            continue        # if the length of the row is not 7 then just ignore it and continue to the next iteration

        #writer.writerow(player_dict)
        #print table
    #csvfile.close()
    return table


def main():
    season = int(sys.argv[1])
    week = sys.argv[2]
    all_urls = generate_all_urls(week)
    all_tables = []
    for url in all_urls:
        print url
        soup = get_source(url)
        table = get_data_from_source(soup, season, week)
        all_tables = all_tables + table
    write_to_db(tuple(all_tables))
    print("Finished!")
    
if __name__ == '__main__':
    main()
