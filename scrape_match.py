from pathlib import Path
import os
import datetime
import time
import sys

sys.path.append('.')

from classes.MatchScraper import MatchScraper
from classes.CalendarScraper import CalendarScraper

from multiprocessing import Pool


root = Path('./data')




def makes_dir(url, root):
    match = url.split('/')[5]
    teams = match.split('-at-')[0]
    date = match.split('-on-')[1]

    p = root.joinpath(date)
    if not os.path.isdir(p):
        os.makedirs(p)

    p = p.joinpath(teams)
    if not os.path.isdir(p):
        os.makedirs(p)

    return p, teams, date


def scrape_match(url):
    ms = MatchScraper(url)
    p, match, date = makes_dir(url, root)
    # logging.basicConfig(filename=str(p.joinpath('log.txt')), encoding='utf-8', level=logging.DEBUG)

    match_running = True
    valid_request_counter = 0
    last_valid_minute = 0
    local_valid_request_counter = 0
    while match_running:
        valid_request_counter += 1
        df, team_stats = ms.get_match_snapshot()
        minute = ms.get_minute_of_play()


        if len(df) > 0:

            if minute.replace('\'', '') != str(last_valid_minute):
                local_valid_request_counter = 0 


            df.to_csv(p.joinpath('%d_%s_ps.csv' % (local_valid_request_counter, minute)))
            team_stats.to_csv(p.joinpath('%d_%s_ts.csv' % (local_valid_request_counter, minute)))
            if 'FT' in df.minute.tolist() or valid_request_counter > 150:
                match_running = False
                print('Match %s over' % match)
            print('%s at %s' % (match, minute))
        else:
            print('Data unavailable for %s - %s'% (match, minute), datetime.datetime.now())

        time.sleep(60)
    return



def job_that_executes_once(link):
    with open('file.txt', 'w') as f: f.write('diofa ' + str(datetime.datetime.today()))
    print('# Do some work that only needs to happen once...')
    print(link)
    print()
    return


if __name__ == '__main__':

    # url = sys.argv[1]
    # url = 'https://www.rugbypass.com/live/pro-14/zebre-vs-glasgow-warriors-at-stadio-sergio-lanfranchi-on-06032021/2020-2021/stats/'
    # team_stats = scrape_match(url)



    url_live = 'https://www.rugbypass.com/live/'
    cs = CalendarScraper(url_live)
    games = cs.get_games_df()
    mydate = datetime.date.today()
    today = games[games.date_utc.dt.date == mydate]

    pool = Pool(processes=len(today))
    pool.map(scrape_match, today.link.tolist())











