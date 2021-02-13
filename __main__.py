from pathlib import Path
import os
import datetime
import time
import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from classes.MatchScraper import MatchScraper
from classes.CalendarScraper import CalendarScraper


sched = BlockingScheduler()

root = Path('./Data')




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
    logging.basicConfig(filename=p.joinpath('log.txt'), encoding='utf-8', level=logging.DEBUG)

    match_running = True
    while match_running:

        df = ms.get_match_snapshot()
        minute = ms.get_minute_of_play()
        if len(df) > 0:
            df.to_csv(p.joinpath('%s.csv' % minute))
            if 'FT' in df.minute.tolist():
                match_running = False
                print('Match %s over' % match)
                logging.info('Match %s over' % match)
            print('%s at %s' % (match, minute))
            logging.info('%s at %s' % (match, minute))
        else:
            print('Data unavailable for %s - %s'% (match, minute))

        time.sleep(1)



if __name__ == '__main__':

    url_live = 'https://www.rugbypass.com/live/'
    cs = CalendarScraper(url_live)
    games = cs.get_games_df()
    mydate = datetime.date.today()
    today = games[games.date_utc.dt.date == mydate]
    test_date = datetime.datetime.today() + datetime.timedelta(0,3)



    if len(today) > 0:
        for index, row in today.iterrows():
            try:
                print(row['home'], row['away'], 'on', row['date'],'added')
                sched.add_job(scrape_match,
                              'date',
                              run_date=row['date'],
                              args=[row['link']])
            except IndexError:
                print('No data available for this %s - %s ' % (row['home'], row['away']))
        sched.start()
    else:
        print('no matches')








