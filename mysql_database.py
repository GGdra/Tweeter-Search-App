import pymysql
from pymysql import Error
import json
from datetime import datetime
from config import *

# Function to create a connection to the MySQL database
def create_server_connection(host_name, user_name, user_password, db_name=None):
    connection = None
    try:
        connection = pymysql.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
        print("MySQL Database connection successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

# Function to create a new database
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Database created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

# Function to create a table
def create_table(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Table created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

# Helper function to convert Twitter date format to MySQL datetime format
def convert_twitter_date_to_sql_date(twitter_date):
    try:
        # Parse the Twitter date format
        return datetime.strptime(twitter_date, '%a %b %d %H:%M:%S %z %Y').strftime('%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        print(f"Date conversion error: {e}")
        return None

# Function to create or update a user in the database
def insert_or_update_user(connection, user_data):
    cursor = connection.cursor()
    sql = """
    INSERT INTO users
    (user_id, name, screen_name, location, url, geo, place, followers_count, description, favourites_count, statuses_count, created_at) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    name=VALUES(name), screen_name=VALUES(screen_name), location=VALUES(location), url=VALUES(url),
    geo=VALUES(geo), place=VALUES(place), followers_count=VALUES(followers_count), description=VALUES(description),
    favourites_count=VALUES(favourites_count), statuses_count=VALUES(statuses_count), created_at=VALUES(created_at);
    """
    # Convert the Twitter datetime to SQL datetime
    created_at = convert_twitter_date_to_sql_date(user_data['created_at'])

    if created_at is not None:
        user_values = (
            user_data['id_str'],  # Use 'id_str' to ensure the ID is captured accurately
            user_data['name'],
            user_data['screen_name'],
            user_data['location'],
            user_data['url'],
            user_data.get('geo', None),  # Assuming geo is optional
            user_data.get('place', None),  # Assuming place is optional
            user_data['followers_count'],
            user_data['description'],
            user_data.get('favourites_count', 0),  # Defaulting to 0 if not present
            user_data.get('statuses_count', 0),  # Defaulting to 0 if not present
            created_at  # Use the converted datetime
        )
        try:
            cursor.execute(sql, user_values)
            connection.commit()
        except Error as e:
            print(f"Failed to insert/update user {user_data['id_str']}: {e}")
    else:
        print(f"Skipping user {user_data['id_str']} due to invalid date format")

def execute_sql(connection, sql):
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        connection.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()

# Processing the dataset
def process_dataset(file_path, db_connection):
    with open(file_path, "r") as file:
        for line in file:
            try:
                data = json.loads(line)
                user_data = data['user']
                insert_or_update_user(db_connection, user_data)
            except json.JSONDecodeError:
                continue  # Skip lines that can't be decoded
            except KeyError:
                continue  # Skip lines missing expected keys

if __name__ == '__main__':
    # Establish the server connection
    server_connection = create_server_connection(mysql_config['host'], mysql_config['user'], mysql_config['password'])

    # Create a new Twitter database
    create_database_query = "CREATE DATABASE IF NOT EXISTS TwitterData;"
    create_database(server_connection, create_database_query)

    # Close the initial connection to reconnect with the specific database
    server_connection.close()

    # Establish a connection to the specific database
    db_connection = create_server_connection(mysql_config['host'], mysql_config['user'], mysql_config['password'], mysql_config['db'])

    # Define and create the users table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT PRIMARY KEY,
        name VARCHAR(255),
        screen_name VARCHAR(255),
        location VARCHAR(255),
        url VARCHAR(255),
        geo VARCHAR(255),
        place VARCHAR(255),
        followers_count INT,
        description TEXT,
        favourites_count INT,
        statuses_count INT,
        created_at DATETIME
    );
    """
    execute_sql(db_connection, create_table_sql)

    # Create an index on `screen_name`
    create_index_screen_name = "CREATE INDEX idx_screen_name ON users (screen_name);"
    execute_sql(db_connection, create_index_screen_name)

    # Create a B-tree index on `followers_count`
    create_index_followers_count = "CREATE INDEX idx_followers_count ON users (followers_count);"
    execute_sql(db_connection, create_index_followers_count)

    # Create an index on `created_at`
    create_index_created_at = "CREATE INDEX idx_created_at ON users (created_at);"
    execute_sql(db_connection, create_index_created_at)

    process_dataset(data_path, db_connection)
    if db_connection:
        db_connection.close()


