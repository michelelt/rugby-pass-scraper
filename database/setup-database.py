import psycopg2
from configparser import ConfigParser

def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


def check_connection():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

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


def create_tables():
    commands = (
        """
        CREATE TABLE Team (
            team_id SERIAL PRIMARY KEY,
            team_hash VARCHAR UNIQUE 
            team_name VARCHAR(255) NOT NULL
        )
        """,


        """
        CREATE TABLE Player (
            player_id SERIAL PRIMARY KEY,
            player_hash VARCHAR UNIQUE 
            player_name VARCHAR(255) NOT NULL
        )
        """,

        """
        CREATE TABLE Plays(
            player_id INTEGER,
            team_id INTEGER,
            yyss VARCHAR (256) NOT NULL
            PRIMARY KEY (player_id, team_id, yyss),

            FOREIGN KEY(player_id)
                REFERENCES Player (player_id)
                ON UPDATE CASCADE ON DELETE CASCADE,

            FOREIGN KEY(team_id)
                REFERENCES Team (team_id)
                ON UPDATE CASCADE ON DELETE CASCADE
        )
        """,

        """
        CREATE TABLE Match_game (
            match_id SERIAL PRIMARY KEY,
            home_team INTEGER,
            away_team INTEGER,
            kick_off TIMESTAMP WITH TIME ZONE,
            place VARCHAR(255) NOT NULL,

            FOREIGN KEY (home_team)
                REFERENCES Team (team_id)
                ON UPDATE CASCADE ON DELETE CASCADE,

            FOREIGN KEY (away_team)
                REFERENCES Team (team_id)
                ON UPDATE CASCADE ON DELETE CASCADE
        )
        """,

        """
        CREATE TABLE Player_stats(
            ps_id SERIAL PRIMARY KEY,
            match_id INTEGER,
            player_id INTEGER,
            team_id INTEGER,
            position INTEGER, try_scored INTEGER, ball_carry_meters INTEGER, ball_carry INTEGER, beat_defender INTEGER, 
            line_break INTEGER, passes INTEGER, offloads INTEGER, total_turnovers_conceded INTEGER, try_assist INTEGER, points INTEGER,
            tackle_made INTEGER, tackle_missed INTEGER, total_turnovers_gained INTEGER,
            kicks_from_hand INTEGER, conversion_succesful INTEGER, penalty_goal_succesful INTEGER, drop_kick_succesful INTEGER,
            lineout_won_own_throws INTEGER, lineout_won_steal INTEGER, 
            penalties_conceded INTEGER, red_card INTEGER, yellow_card INTEGER,
            minute INTEGER, 
            
        FOREIGN KEY(match_id)
            REFERENCES Match_game (match_id)
            ON UPDATE CASCADE ON DELETE CASCADE,
                
        FOREIGN KEY(player_id)
            REFERENCES Player (player_id)
            ON UPDATE CASCADE ON DELETE CASCADE,
            
        FOREIGN KEY(team_id)
            REFERENCES Team (team_id)
            ON UPDATE CASCADE ON DELETE CASCADE       
        )
        """,

        """
        CREATE TABLE Match_stat(
            ms_id SERIAL PRIMARY KEY,
            match_id INTEGER,
            team_id INTEGER,
            possession FLOAT,
            rucks_won INTEGER,
            rucks_lost INTEGER,
            maul_won INTEGER,
            scrum_won INTEGER,
            scrum_lost INTEGER,
            minute INTEGER,

        FOREIGN KEY(match_id)
            REFERENCES Match_game (match_id)
            ON UPDATE CASCADE ON DELETE CASCADE,

        FOREIGN KEY(team_id)
            REFERENCES Team (team_id)
            ON UPDATE CASCADE ON DELETE CASCADE

        )
        """
    )

    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            print(command)
            cur.execute(command)
            print('created')
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()



create_tables()
