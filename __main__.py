from pathlib import Path
import os
import datetime
import time
import logging

from classes.MatchScraper import MatchScraper
from classes.CalendarScraper import CalendarScraper

from plan import Plan



if __name__ == '__main__':

    url_live = 'https://www.rugbypass.com/live/'
    cs = CalendarScraper(url_live)
    games = cs.get_games_df()
    mydate = datetime.date.today()
    today = games[games.date_utc.dt.date == mydate]

    curr_date = datetime.datetime.today()
    run_date =  curr_date + datetime.timedelta(minutes=1)

    print(datetime.datetime.today())
    print(run_date)
    print(run_date.strftime('%H:%M'))

    min = run_date.minute
    hour = run_date.hour
    month = run_date.month
    dom = run_date.day
    dow = run_date.weekday() + 1
    year = run_date.year

    print(min, hour, month, dom, dow)


    cron = Plan()
    command3 = '/usr/local/bin/python3 /Users/mc/Desktop/rugby-pass-scraper/scrape_match.py'
    every = '%s %s %s %s %s' % (min, hour, dom, month, dow,)
    cron.command(command3, every=every, at=str(hour)+':'+str(min))
    cron.command(command3, every=every, at=str(hour)+':'+str(min+1))
    cron.run('write')








