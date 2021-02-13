import pandas as pd
import datetime
import numpy as np

from bs4 import BeautifulSoup
from .Scraper import Scraper

class MatchScraper(Scraper):

    def __init__(self, url):
        super().__init__(url)
        self.stats_indeces = {'attack':0,
                            'defence':1,
                            'kicking':2,
                            'set plays':3,
                            'discipline':4}
        self.minute = None
        self.tables = None
        self.perf_df = pd.DataFrame()
        self.final_df = None

    def get_minute_of_play(self):

        soup = BeautifulSoup(self.html, "html.parser" )
        self.minute = soup.find("span", {"id": "match-game-time"})
        if self.minute == None:
            return None
        else:
            self.minute = self.cleanhtml(str(self.minute))
            return self.minute

    def get_html_tables(self):
        tables = {}
        html_original = self.html
        for team in ['home', 'away']:
            pivot = '<div class="match-centre-stats-page-team %s full-player-stats">' % team
            index_start = html_original.find(pivot) + len(pivot)
            html = html_original[index_start:].strip()

            index_end = html.find('</div>')
            html = html[:index_end]

            tables[team] = html
        self.tables = tables
        return tables

    def html_table_2_df(self, table, index):
        html  = table
        if index == 0:
            pivot = '<table class="player-stats selected" data-index="%d">' % index
        else:
            pivot = '<table class="player-stats " data-index="%d">' % index
        index_start = html.find(pivot) + len(pivot)
        table = html[index_start:]

        index_end = html.find('</table>')
        table = table[:index_end]
        table = '<table>' + table + "</table>"

        try:
            df = pd.read_html(table)[0]
            df = df.dropna(axis=0)
            df = df.rename(columns={'Unnamed: 0': 'position'})
            df = df.set_index('position')
        except IndexError:
            df = pd.DataFrame()

        return df

    def concat_all_dfs(self, table, data_indeces):
        dfs = []
        for x in data_indeces.keys():
            dfs.append(self.html_table_2_df(table, data_indeces[x]))
        try:
            df = pd.concat(dfs, axis=1)
            team = df.columns[0]
            df['players'] = df[team].values[:, 1]
            df = df.drop(team, axis=1)
            df['team'] = team
        except IndexError:
            df = pd.DataFrame()

        self.final_df = df
        return df

    def get_match_snapshot(self):
        html = self.get_html(self.url)
        minute = self.get_minute_of_play()

        if html != np.nan:

            self.get_html_tables()

            team = 'home'
            df1 = self.concat_all_dfs(self.tables[team], self.stats_indeces)

            team = 'away'
            df2 = self.concat_all_dfs(self.tables[team], self.stats_indeces)

            df = pd.concat([df1, df2], axis=0)
            df['timestamp'] = datetime.datetime.utcnow()
            df['timezone'] = 'UTC'
            df['minute'] = minute


            return df
        else:
            print('Error in html parsing')
            return pd.DataFrame()


