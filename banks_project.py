import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
table_name = "Largest_banks"
exchange_rate_path = "exchange_rate.csv"
output_path = "data/Largest_banks_data.csv"
database_name = "database/Banks.db"
log_file = "loggs/code_log.txt"


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')


log_progress("Preliminaries complete. Initiating ETL process")


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('tbody')
    trows = tables[0].find_all('tr')[1:]
    # df = pd.DataFrame(columns=table_attribs)
    data = []
    for row in trows:
        cells = row.find_all('td')

        # Check if row is not empty
        # append row :
        row_dict = {"Name": cells[1].text.strip(), "MC_USD_Billion": cells[2].text.strip()}
        data.append(row_dict)

    df = pd.DataFrame(data, columns=table_attribs)

    return df


# print(extract(url, table_attribs))


def transform(df, exchange_rate_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_df = pd.read_csv(exchange_rate_path)
    exchange_rate = dict(zip(exchange_df["Currency"], exchange_df["Rate"]))
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype('float')
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    q_output = pd.read_sql(query_statement, sql_connection)
    print(q_output)


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

df = extract(url, table_attribs)
print("extracted data : ")
print(df)
log_progress("Data extraction complete. Initiating Transformation process")
df = transform(df, exchange_rate_path)
print("data after transformation : ")
print(df)
print("quiz question 2 : ", df['MC_EUR_Billion'][4])
log_progress("Data transformation complete. Initiating Loading process")
load_to_csv(df, output_path)
log_progress("Data saved to CSV file")
sql_connection = sqlite3.connect(database_name)
log_progress("SQL Connection initiated")
load_to_db(df, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")
query_statement = "SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)
query_statement = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)
query_statement = "SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)
log_progress("Process Complete")
sql_connection.close()
log_progress("Server Connection closed")