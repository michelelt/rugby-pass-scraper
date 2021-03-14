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

    def standardize_columns_names(self, stat_label):
        new_columns={}
        new_columns['attack'] = {
            'T': 'try_scored', 'M': 'ball_carry_meters', 'C': 'ball_carry', 'BD': 'beat_defender', 'CB': 'line_break', 'P': 'passes',
            'O': 'offloads', 'TC': 'total_turnovers_conceded', 'TA': 'try_assist', 'Pts': 'points',
        }

        new_columns['defence'] = {
            'T': 'tackle_made', 'MT': 'tackle_missed','TW': 'total_turnovers_gained',
        }

        new_columns['kicking'] = {
            'K': 'kicks_from_hand', 'C': 'conversion_succesful', 'PG': 'penalty_goal_succesful','DG': 'drop_kick_succesful',
        }

        new_columns['set plays'] = {
            'TW': 'lineout_won_own_throws', 'LS': 'lineout_won_steal'
        }

        new_columns['discipline']={
            'PC': 'penalties_conceded', 'RC': 'red_card', 'YC': 'yellow_card'
        }

        return new_columns[stat_label]


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

    def get_team_stats(self):
        html = self.html
        soup = BeautifulSoup(str(html), 'html.parser')
        div = soup.find_all('script')
        team_stats = {'home':{}, 'away':{}}
        for el in list(div):
            el = str(el)
            if 'possession' in el.lower():
                team_stats['home']['possession'] = el.split(',')[2]
                team_stats['away']['possession'] = el.split(',')[3]

            elif 'rucks won' in el.lower():
                won = el.split(';')[0]
                lost = el.split(';')[2]
                team_stats['home']['rucks_won'] = won.split(',')[2]
                team_stats['away']['rucks_won'] = won.split(',')[3]
                team_stats['home']['rucks_lost'] = lost.split(',')[2]
                team_stats['away']['rucks_lost'] = lost.split(',')[3]
            else:
                pass

        soup = BeautifulSoup(str(html), "html.parser")
        div = soup.find('div', {'data-id': 'team-breakdown'}).find_all('div', {'class': 'home'})
        team_stats['home']['maul_won_home'] = self.cleanhtml(str(div).split(',')[2]).strip()
        div = soup.find('div', {'data-id': 'team-breakdown'}).find_all('div', {'class': 'away'})
        team_stats['away']['maul_won_away'] = self.cleanhtml(str(div).split(',')[2]).strip()

        soup = BeautifulSoup(str(html), "html.parser")
        div = soup.find('div', {'data-id': 'team-set-plays'}).find_all('div', {'class': 'home'})
        team_stats['home']['scrum_won'] = self.cleanhtml(str(div).split(',')[2]).strip()
        team_stats['home']['scrum_lost'] = self.cleanhtml(str(div).split(',')[4]).strip()

        div = soup.find('div', {'data-id': 'team-set-plays'}).find_all('div', {'class': 'away'})
        team_stats['away']['scrum_won'] = self.cleanhtml(str(div).split(',')[2]).strip()
        team_stats['away']['scrum_lost'] = self.cleanhtml(str(div).split(',')[4]).strip()

        return team_stats

    def concat_all_dfs(self, table, data_indeces):
        dfs = []
        for x in data_indeces.keys():
            tmp = self.html_table_2_df(table, data_indeces[x])
            tmp = tmp.rename(columns=self.standardize_columns_names(x))
            dfs.append(tmp)
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
        # print(html)
        if 'Sorry no stats available' not in self.html:
        # if 'Sorry no stats available' not in html:
            self.get_html_tables()

            team = 'home'
            df1 = self.concat_all_dfs(self.tables[team], self.stats_indeces)
            df1['is_home'] = True

            team = 'away'
            df2 = self.concat_all_dfs(self.tables[team], self.stats_indeces)
            df2['is_home'] = False

            df = pd.concat([df1, df2], axis=0)
            df['timestamp'] = datetime.datetime.utcnow()
            df['timezone'] = 'UTC'
            df['minute'] = minute


            team_stats = self.get_team_stats()
            team_stats['minute'] = minute
            team_stats['home']['name'] = df1.team.unique()
            team_stats['away']['name'] = df2.team.unique()

            team_stats = pd.DataFrame(team_stats)


            return df, team_stats
        else:
            return pd.DataFrame(), pd.DataFrame()


