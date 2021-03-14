import psycopg2
from configparser import ConfigParser

class DatabaseProxy:
    def __init__(self, filename, section):
        self.filename = filename,
        self.section = section

    def config(self):
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(self.filename)

        db = {}
        if parser.has_section(self.section):
            params = parser.items(self.section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(self.section, self.filename))

        return db


    def check_connection(self):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = self.config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)

            # create a cursor
            cur = conn.cursor()

            # execute a statement
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)

            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

    def select_query(self, query):
        """ query data from the vendors table """
        conn = None
        try:
            params = self.config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute(query)
            print("The number of parts: ", cur.rowcount)
            row = cur.fetchone()

            while row is not None:
                yield row
                row = cur.fetchone()

            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def insert_query(self, query):
        """ query data from the vendors table """
        conn = None

        params = self.config()
        conn = psycopg2.connect(**params)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        print('Record Inserted')
        conn.close()

    def get_team_id(self, team_str):
        query ='''
            SELECT * FROM team
            WHERE team.team_name %% \'%s\'; 
        '''

        results = dict(self.exec_query(query % team_str) )
        return results


    def get_player_id(self, player_name):
        query = '''
            SELECT * FROM player
            WHERE player.player_name %% \'%s\'; 
        '''

        results = dict(self.exec_query(query % player_name))
        return results


    def insert_palyer_play_in_team(self, players_list, team):
        team_id = self.get_team_id(team)
        players_id_list = []
        for player in players_list:
            players_id_list.append(self.get_team_id()[0])



filename='database.ini'
section='postgresql'

dbp = DatabaseProxy(filename, section)
var = dbp.insert_player({'player_name':'paolo rossi'})


# START TRANSACTION
#
# DECLARE placeKey int
#
# INSERT INTO places (place_name)
# VALUES ('XYZ')
#
# SET placeKey = LAST_INSERT_ID()
#
# DECLARE tripKey int
#
# INSERT INTO trips (trip_name)
# VALUES ('MyTrip')
#
# SET tripKey = LAST_INSERT_ID()
#
# INSERT INTO trips_places_asc(trip_id, place_id)
# VALUES (tripKey, placeKey)
#
# COMMIT
