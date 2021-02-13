import urllib
import pandas as pd
from bs4 import BeautifulSoup
import datetime
import re
from dateutil.tz import tzlocal
import pytz

from bs4 import BeautifulSoup
from .Scraper import Scraper

class CalendarScraper(Scraper):

    def __init__(self, url):
        super().__init__(url)
        self.LOCAL_TIMEZONE = datetime.datetime.now(datetime.timezone(datetime.timedelta(0))).astimezone().tzname()

        self.games_html = None
        self.games_dict = {}
        self.games = None


    def get_games_divs(self):
        soup = BeautifulSoup(self.html, "html.parser" )
        games = soup.find_all("div", {"class": "games-competition"})

        self.games_html= games
        return games



    def parse_game_match(self, game_html):
        games_dict = {}
        soup = BeautifulSoup(str(game_html), "html.parser" )
        header = str(list(soup.find_all("div"))[0])

        soup = BeautifulSoup(header, "html.parser" )

        game_competition_and_date = str(soup.find('span', {'class': 'comp-date'}))
        for (line, k) in zip(game_competition_and_date.split('<br/>'), ['competition', 'date']):
            games_dict[k] = self.cleanhtml(line).strip()

        date_time_str = games_dict['date']
        date_time_obj = datetime.datetime.strptime(date_time_str, '%a %d %B %Y, %I:%M%p')
        games_dict['date'] = date_time_obj
        timezone = pytz.timezone(self.LOCAL_TIMEZONE)
        timezone_date_time_obj = timezone.localize(date_time_obj)
        timezone_date_time_obj = timezone_date_time_obj.astimezone(pytz.UTC)
        games_dict['date_utc'] = timezone_date_time_obj

        teams = soup.find_all('a', {'class': 'name team-link'})
        for team, where in zip(teams, ['home', 'away']):
            games_dict[where] = self.cleanhtml(str(team))

        try:
            scrape_link = str(soup.find('a', {'class': 'link-box'}).get('href'))
            games_dict['link'] = scrape_link + 'stats/'

            score_home = self.cleanhtml(str(soup.find('span', {'class': 'score home'})))
            games_dict['score_home'] = score_home

            score_away = self.cleanhtml(str(soup.find('span', {'class': 'score away'})))
            games_dict['score_away'] = score_away

        except AttributeError:
            print(games_dict['home'], games_dict['away'], games_dict['date'])

        self.games_dict = games_dict
        return games_dict


    def get_games_df(self):
        self.get_html(self.url)
        self.get_games_divs()
        games = []
        for game_html in self.games_html:
            game = self.parse_game_match(game_html)
            games.append(game)

        df = pd.DataFrame(games)
        return df



