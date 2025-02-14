from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = 'postgres_conn_id'  # Connection ID for PostgreSQL

def create_raw_data_table(table_name: str, columns: list):
    """ Creates a table in PostgreSQL """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    
    try:
        column_definitions = ', '.join(columns)
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions})"
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        cur.close()
        conn.close()  

def insert_raw_data(ti):
    """ Inserts data into PostgreSQL, pulling from XCom """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    data = ti.xcom_pull(task_ids='scrape_wikipedia')

    if not data:
        print("No data found in XCom for insertion!")
        return

    try:
        placeholders = ', '.join(['%s'] * len(data[0])) 
        query = f"INSERT INTO labs.raw_stadium_data VALUES ({placeholders})"
        # print(f"Query: {query}")
        # print(f"Data: {data[0]}")
        cur.executemany(query, [tuple(row) for row in data])
        conn.commit()
    except Exception as e:
        print(f"Error inserting data: {e}")
        raise Exception(f"Error inserting data: {e}")
    finally:
        cur.close()
        conn.close()  

def validate_data(table_name: str):
    """ Validates the data in the table """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    try:
        conn = hook.get_conn()
        cur = conn.cursor()

        query = f"SELECT COUNT(*) FROM {table_name}"
        cur.execute(query)

        count = cur.fetchone()[0]
        
        if count != 382:
            raise ValueError(f"Data validation failed! Expected 382 rows, got {count} rows.")
        print("Data validation passed!")
    except Exception as e:
        print(f"Error validating data: {e}")
    finally:
        cur.close()
        conn.close()

def create_tables_for_normalized_data():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    try:
        conn = hook.get_conn()
        cur = conn.cursor()

        # region table query
        query = """
        CREATE TABLE IF NOT EXISTS labs.region (
            region_id SERIAL PRIMARY KEY,
            region_name TEXT UNIQUE
        )
        """
        cur.execute(query)
        conn.commit()

        # country table query
        query = """
        CREATE TABLE IF NOT EXISTS labs.country (
            country_id SERIAL PRIMARY KEY,
            country_name TEXT,
            flag_url TEXT,
            region_id INT,
            FOREIGN KEY (region_id) REFERENCES labs.region(region_id)
        )
        """
        cur.execute(query)
        conn.commit()


        # stadium table query
        query = """
        CREATE TABLE IF NOT EXISTS labs.stadium (
            stadium_id SERIAL PRIMARY KEY,
            rank INT,
            stadium_name TEXT,
            seating_capacity TEXT,
            city TEXT,
            stadium_image_url TEXT,
            country_id INT,
            FOREIGN KEY (country_id) REFERENCES labs.country(country_id)
        )
        """
        cur.execute(query)
        conn.commit()

        # team table query
        query = """
        CREATE TABLE IF NOT EXISTS labs.team (
            team_id SERIAL PRIMARY KEY,
            team_name TEXT UNIQUE,
            stadium_id INT,
            FOREIGN KEY (stadium_id) REFERENCES labs.stadium(stadium_id)
        )
        """
        cur.execute(query)
        conn.commit()   
    except Exception as e:
        print(f"Error creating tables: {e}")
        raise Exception(f"Error creating tables: {e}")
    finally:
        cur.close()
        conn.close()

def insert_normalized_data():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    # op_args=['labs.raw_stadium_data', ['rank INT', 'stadium TEXT', 'seating_capacity TEXT', 'region TEXT', 'country TEXT', 'city TEXT', 'images TEXT', 'home_teams TEXT', 'flag TEXT']],
    try:
        # Insert data into region table
        query = """
        INSERT INTO labs.region (region_name)
        SELECT DISTINCT region FROM labs.raw_stadium_data
        """
        cur.execute(query)
        conn.commit()
        # Insert data into country table
        query = """
        INSERT INTO labs.country (country_name, flag_url, region_id)
        SELECT DISTINCT country, flag, region_id FROM labs.raw_stadium_data s
        JOIN labs.region r ON s.region = r.region_name 
        """
        cur.execute(query)
        conn.commit()


        # Insert data into stadium table
        query = """
        INSERT INTO labs.stadium (stadium_name, seating_capacity, city, region_id, stadium_image_url)
        SELECT stadium, seating_capacity, city, region_id, images
        FROM labs.raw_stadium_data
        JOIN labs.region
        ON labs.raw_stadium_data.region = labs.region.region_name
        """
        cur.execute(query)
        conn.commit()

        # Insert data into team table
        # pay more attention to this since the home_teams column has multiple values
        query = """
                SELECT home_teams, stadium_id
                FROM labs.raw_stadium_data
                JOIN labs.stadium
                ON labs.raw_stadium_data.stadium = labs.stadium.stadium_name
            """
        cur.execute(query)
        rows = cur.fetchall()

        for row in rows:
            home_teams = row[0]
            
            if home_teams: 
                teams = [team.strip() for team in home_teams.split(",")]  # Handle inconsistent spacing

                for team in teams:
                    insert_query = """
                    INSERT INTO labs.team (team_name, stadium_id)
                    VALUES (%s, %s)
                    """
                    cur.execute(insert_query, (team, row[1]))

        conn.commit()  

    except Exception as e:
        print(f"Error inserting normalized data: {e}")
        delete_all_tables()
        raise Exception(f"Error inserting normalized data: {e}")
    finally:
        cur.close()
        conn.close()

def delete_all_tables():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        tables = ['labs.raw_stadium_data', 'labs.country', 'labs.region', 'labs.stadium', 'labs.team']
        for table in tables:
            query = f"DROP TABLE IF EXISTS {table}"
            cur.execute(query)
        conn.commit()
    except Exception as e:
        print(f"Error deleting tables: {e}")
        raise Exception(f"Error deleting tables: {e}")
    finally:
        cur.close()
        conn.close()