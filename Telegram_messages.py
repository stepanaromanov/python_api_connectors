!pip install telethon
!pip install pandas
!pip install psycopg2

import pandas as pd
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel
import psycopg2
from psycopg2 import sql, IntegrityError, OperationalError

def parse_message(text):
    # Function to parse the message text into a dictionary
    lines = text.strip().split("\n")
    parsed_data = {}

    for line in lines:
        # Check for English terms
        if line.startswith("Company:") or line.startswith("Компания:"):
            parsed_data['Company'] = line.split(":", 1)[1].strip()
        elif line.startswith("Period of publication:") or line.startswith("Период размещения:"):
            parsed_data['Period of Publication'] = line.split(":", 1)[1].strip()
        elif line.startswith("Region:") or line.startswith("Регион:"):
            parsed_data['Region'] = line.split(":", 1)[1].strip()
        elif line.startswith("Salary offered:") or line.startswith("Предлагаемая зарплата"):
            parsed_data['Salary Offered'] = line.split(":", 1)[1].strip() if ":" in line else "Negotiable"
        elif line.startswith("Vacancy:") or line.startswith("Вакансия:"):
            parsed_data['Vacancy'] = line.split(":", 1)[1].strip()
        else:
            # Assuming the last line is the URL
            parsed_data['URL'] = line.strip()

    return parsed_data

# Your API credentials (get from my.telegram.org)
api_id = 'your_api_id' 
api_hash = 'your_api_hash' 
phone_number = 'your_phone_number'

# Channel username or ID
channel_username = "@UzjobsUz"

# Specify limit messages
limit_messages = 1000

# Initialize the Telegram client
client = TelegramClient('session_name', api_id, api_hash)

# PostgreSQL connection parameters
db_params = {
    'dbname': 'your_database_name',
    'user': 'your_username',
    'password': 'your_password',
    'host': 'localhost',  # or your database host
    'port': '5432'  # Default PostgreSQL port
}

def insert_or_update_data(df):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        for index, row in df.iterrows():
            # Define the SQL command for insertion or update
            upsert_query = sql.SQL("""
                INSERT INTO your_table_name (company, period_of_publication, region, salary_offered, vacancy, url)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (company, period_of_publication) DO UPDATE
                SET region = EXCLUDED.region,
                    salary_offered = EXCLUDED.salary_offered,
                    vacancy = EXCLUDED.vacancy,
                    url = EXCLUDED.url;
            """)

            # Execute the query
            cursor.execute(upsert_query, (row['Company'], row['Period of Publication'], row['Region'], 
                                           row['Salary Offered'], row['Vacancy'], row['URL']))
        
        # Commit the transaction
        conn.commit()

    except (OperationalError, IntegrityError) as e:
        print(f"Database error: {e}")
        conn.rollback()  # Rollback in case of error
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

async def fetch_channel_messages(base_df):
    # Replace with your actual channel username or ID
    channel = await client.get_entity(channel_username)
    async for message in client.iter_messages(channel, limit=limit_messages):
        if message.message:  # Ensure the message is not None
            # Parse the message and create a DataFrame
            data = parse_message(message.message)
            df = pd.DataFrame([data])
            # Concatenate the new DataFrame with the existing one
            base_df = pd.concat([base_df, df], ignore_index=True)
    
    return base_df  # Return the updated DataFrame

async def main():
    base_df = pd.DataFrame(columns=['Company', 'Period of Publication', 'Region', 'Salary Offered', 'Vacancy', 'URL'])
    async with client:
        base_df = await fetch_channel_messages(base_df)  # Pass base_df to the function

    insert_or_update_data(base_df)  # Insert or update data in PostgreSQL

    return base_df  # Return the final DataFrame

# Running the async function in an environment where the event loop is already running
result_df = await main()
display(result_df)

