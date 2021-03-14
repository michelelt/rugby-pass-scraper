from pathlib import Path
import os
import datetime
import time
import logging
import json
import pickle


from classes.MatchScraper import MatchScraper
from classes.CalendarScraper import CalendarScraper


def get_players_from_match(url):
    ms = MatchScraper(url)
    df, team_stats = ms.get_match_snapshot()

    return df, team_stats


def download_entry_dataset(games):
    teams_with_players = {}

    for i, row in games.iterrows():
        print(row.home, row.away, row.competition)
        try:
            ps, ts = get_players_from_match(row.link)

            if len(ps) > 0:
                grouped = ps.groupby(['team', 'players']).count().index.tolist()
                if len(ps.players.unique()) != len(ps.players):
                    print(row.home, row.away, row.competition, 'hasa omonymus')
                    break

                for team_player_pair in grouped:
                    team = team_player_pair[0]
                    player = team_player_pair[1]

                    if team not in teams_with_players.keys():
                        teams_with_players[team] = [player]
                    else:

                        if player not in teams_with_players[team]:
                            teams_with_players[team].append(player)

        except AttributeError:
            print(row.link)

    return teams_with_players

if __name__ == '__main__':

    url_live = 'https://www.rugbypass.com/live/'
    cs = CalendarScraper(url_live)
    games = cs.get_games_df()
    games = games[games.date < datetime.datetime.now()]

    teams_with_players = download_entry_dataset(games)
    
    output = open('./Data/players_per_team.pickle', 'wb')
    pickle.dump(teams_with_players, output)
    output.close()


    file = open("./Data/players_per_team.pickle", 'rb')
    teams_with_players = dict(pickle.load(file))
    file.close()


    file = open("./Data/team_club_national.csv", "w+")
    file.write('team,type\n')
    for i,team in enumerate(teams_with_players.keys()):
        print(team, str(i)+'/'+str(len(teams_with_players.keys())))
        a = input()
        file.write(team +','+a+'\n')
    file.close()




