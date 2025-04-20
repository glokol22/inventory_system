import mysql.connector
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Get credentials from the environment
db_host = os.getenv('db_host')
db_user = os.getenv('db_user')
db_password = os.getenv('db_password')

# Connect to MySQL using environment variables
connection = mysql.connector.connect(
    host=db_host,
    user=db_user,
    password=db_password
)

cursor = connection.cursor()


# Creating the database if it doesn't exist
cursor.execute("CREATE DATABASE IF NOT EXISTS java_house_inventory;")

# Switching to the newly created database
cursor.execute("USE java_house_inventory;")