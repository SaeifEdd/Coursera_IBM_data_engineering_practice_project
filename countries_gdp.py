import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

# initialize:
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_millions']
db_name = "database/World_Economies.db"
table_name = "Countries_by_GDP"
csv_path = "data/Countries_by_GDP.csv"


# Code for ETL operations on Country-GDP data


# Importing the required libraries

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    trows = tables[2].find_all('tr')[3:]
    df = pd.DataFrame(columns=table_attribs)
    data = []
    for row in trows:
        cells = row.find_all('td')

        # Check if row is not empty
        if not cells or not cells[0].find('a') or cells[2].text.strip() == '—':
            continue
        else:
            # append row :
            row_dict = {'Country': cells[0].text.strip(), 'GDP_USD_millions': cells[2].text.strip()}
            data.append(row_dict)

    df = pd.DataFrame(data, columns=table_attribs)

    return df


def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    gdp = list(df['GDP_USD_millions'])
    gdp = [float("".join(v.split(','))) for v in gdp]
    gdp = [np.round(v / 1000, 2) for v in gdp]
    df['GDP_USD_millions'] = gdp
    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})

    return df


def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("loggs/etl_project_log.txt", 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')


''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress("Preliminaries complete. Initiating ETL process.")

df = extract(url, table_attribs)

log_progress("Data extraction complete. Initiating Transformation process.")

df = transform(df)

log_progress("Data transformation complete. Initiating loading process.")

load_to_csv(df, csv_path)

log_progress("Data saved to CSV file.")

sql_connection = sqlite3.connect(db_name)

log_progress("SQL Connection initiated.")

load_to_db(df, sql_connection, table_name)

log_progress("Data loaded to Database as table. Running the query.")

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

log_progress("Process Complete.")

sql_connection.close()