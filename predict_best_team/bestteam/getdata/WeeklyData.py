# python WeeklyData.py YYYY W P; where YYYY is the year, W is the Week, and P is the number of Pages to pull.
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
        insert_statement = 'insert into scoring_leaders_weekly (%s) values %s'

        cur.execute(insert_statement, (AsIs(','.join(columns)), tuple(values)))

    cur.execute("""DELETE FROM scoring_leaders_weekly a
                    WHERE a.ctid <> (SELECT min(b.ctid)
                        FROM scoring_leaders_weekly b
                        WHERE (a.name = b.name and a.team = b.team
                        and a.position = b.position and a.season = b.season
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


def generate_all_urls(season, week, n_pages, page_size=50):
    start_values = np.arange(0, page_size * n_pages, page_size)
    all_urls = []
    for i in start_values:
        all_urls.append("http://games.espn.go.com/ffl/leaders?&startIndex={1}&scoringPeriodId={2}&seasonId={0}"
                        .format(season, i, week))
    return all_urls

csvfile = open('WeeklyLeaders.csv','a')
fieldnames= ['name','team','position','season','week','opponent','at_home',
                 'won_game','team_score','opponent_score','passing_completed',
                 'passing_attempted','passing_yds','passing_td','passing_int',
                 'rushing_attempts','rushing_yds','rushing_td',
                 'receiving_receptions','receiving_yds','receiving_td','receiving_targets',
                 'two_point_conv','fumbles','total_returned_tds','total_points']
writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
writer.writeheader()  #because of the append method, currently it is written for every page it downloads
csvfile.close()
    
def get_data_from_source(source, season, week):
    table = []
    csvfile = open('WeeklyLeaders.csv','a')
    fieldnames= ['name','team','position','season','week','opponent','at_home',
                 'won_game','team_score','opponent_score','passing_completed',
                 'passing_attempted','passing_yds','passing_td','passing_int',
                 'rushing_attempts','rushing_yds','rushing_td',
                 'receiving_receptions','receiving_yds','receiving_td','receiving_targets',
                 'two_point_conv','fumbles','total_returned_tds','total_points']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
    #writer.writeheader()  #because of the append method, currently it is written for every page it downloads
    for tr in source.find_all('tr')[3:]: # looking for rows in a table; probably skipping to the third row; source is probably something returned from BeautifulSoup
        tds = tr.find_all('td') # finds individual cells and adds them to some list or array

        player_dict = {}    # starting a blank dictionary
        try:
            player_info = tds[0].text.split(",")
            player_dict['name'] = player_info[0]
            player_dict['team'] = player_info[1].split(u'\xa0')[0].strip()
            player_dict['position'] = player_info[1].split(u'\xa0')[1].strip()
        except:
            player_info = tds[0].text.split(" ")
            player_dict['name'] = player_info[0]
            player_dict['team'] = player_info[0]
            player_dict['position'] = "D"

        player_dict['season'] = season
        player_dict['week'] = week
        
        
        if tds[2].text == "** BYE **":
            player_dict['won_game'] = 2
            player_dict['team_score'] = 0
            player_dict['opponent_score'] = 0
            player_dict['opponent'] = "NA"
            player_dict['at_home'] = 1
            start_index = 3
        else: 
            start_index = 4
            opponent_text = tds[2].text
            if "@" in opponent_text:
                player_dict['opponent'] = opponent_text[1:]
                player_dict['at_home'] = 0
            else:
                player_dict['opponent'] = opponent_text
                player_dict['at_home'] = 1
            status_text = tds[3].text
            won_loss_score = status_text.split(" ")
            won_loss = won_loss_score[0].strip()
            if won_loss == "W":
                player_dict['won_game'] = 1
            else:
                player_dict['won_game'] = 0
            scores = won_loss_score[1].split("-")
            player_dict['team_score'] = int(scores[0].strip())
            player_dict['opponent_score'] = int(scores[1].strip())


        # passing
        cmp_atmp = tds[start_index+1].text.split("/")
        player_dict['passing_completed'] = int(cmp_atmp[0])
        player_dict['passing_attempted'] = int(cmp_atmp[1])
        player_dict['passing_yds'] = int(tds[start_index+2].text)
        player_dict['passing_td'] = int(tds[start_index+3].text)
        player_dict['passing_int'] = int(tds[start_index+4].text)

        # rushing)
        player_dict['rushing_attempts'] = int(tds[start_index+6].text)
        player_dict['rushing_yds'] = int(tds[start_index+7].text)
        player_dict['rushing_td'] = int(tds[start_index+8].text)

        # receiving)
        player_dict['receiving_receptions'] = int(tds[start_index+10].text)
        player_dict['receiving_yds'] = int(tds[start_index+11].text)
        player_dict['receiving_td'] = int(tds[start_index+12].text)
        player_dict['receiving_targets'] = int(tds[start_index+13].text)

        # misc)
        player_dict['two_point_conv'] = int(tds[start_index+15].text)
        player_dict['fumbles'] = int(tds[start_index+16].text)
        player_dict['total_returned_tds'] = int(tds[start_index+17].text)

        player_dict['total_points'] = int(tds[start_index+19].text)

        table.append(player_dict)
        writer.writerow(player_dict)
        # print table
    csvfile.close()
    return table


def main():
    season = int(sys.argv[1])
    week = sys.argv[2]
    n_pages = int(sys.argv[3])
    all_urls = generate_all_urls(season, week, n_pages)
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
