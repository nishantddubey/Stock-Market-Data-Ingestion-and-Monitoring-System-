import yfinance as yf
import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Define the list of 20 stock tickers
tickers = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA',
    'META', 'NVDA', 'NFLX', 'ADBE', 'INTC',
    'PYPL', 'CSCO', 'PEP', 'AVGO', 'COST',
    'TM', 'NKE', 'V', 'MA', 'JPM'
]

# Fetch stock data using yfinance
def fetch_stock_data(tickers):
    stock_data = {}
    for ticker in tickers:
        stock = yf.Ticker(ticker)
        data = stock.history(period="1y")  # Get data for the last year
        stock_data[ticker] = data
    return stock_data

# Database connection details
config = {
    'user': 'nishant',
    'password': 'Nishant@123',
    'host': 'localhost',
    'database': 'STOCK_DATA'
}

# Create a connection to the MySQL database
def create_db_connection(config):
    try:
        conn = mysql.connector.connect(**config)
        return conn
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        return None

# Create the stock data table
def create_stock_data_table(cursor):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stocks_reports (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ticker VARCHAR(10),
        date DATE,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume BIGINT
    )
    """
    try:
        cursor.execute(create_table_query)
    except mysql.connector.Error as err:
        print(f"Failed creating table: {err}")
        return False
    return True

# Insert stock data into the database
def insert_stock_data(cursor, stock_data):
    insert_query = """
    INSERT INTO stocks_reports (ticker, date, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    for ticker, data in stock_data.items():
        for date, row in data.iterrows():
            cursor.execute(insert_query, (
                ticker,
                date,
                row['Open'],
                row['High'],
                row['Low'],
                row['Close'],
                row['Volume']
            ))

# Generate KPI reports
def generate_kpi_reports(stock_data):
    reports = {}

    for ticker, data in stock_data.items():
        # Daily Closing Price
        data['Daily Closing Price'] = data['Close']

        # Price Change Percentage
        data['Price Change Percentage (24h)'] = data['Close'].pct_change(periods=1) * 100
        data['Price Change Percentage (30d)'] = data['Close'].pct_change(periods=30) * 100
        data['Price Change Percentage (1y)'] = data['Close'].pct_change(periods=252) * 100

        reports[ticker] = data

    return reports

# Generate reports and save to files
def save_reports(reports, period):
    for ticker, data in reports.items():
        if period == 'daily':
            report = data.tail(1)
        elif period == 'weekly':
            report = data.tail(7)
        elif period == 'monthly':
            report = data.tail(30)
        
        report.to_csv(f'{ticker}_{period}_report.csv')

        # Plotting Closing Price
        plt.figure(figsize=(10, 5))
        plt.plot(report.index, report['Close'], marker='o')
        plt.title(f'{ticker} {period.capitalize()} Closing Price')
        plt.xlabel('Date')
        plt.ylabel('Closing Price')
        plt.grid(True)
        plt.savefig(f'{ticker}_{period}_closing_price.png')
        plt.close()

def main():
    # Fetch stock data
    stock_data = fetch_stock_data(tickers)

    # Connect to MySQL database
    conn = create_db_connection(config)
    if conn is None:
        return

    cursor = conn.cursor()

    # Create stocks table
    if create_stock_data_table(cursor):
        # Insert stock data into the table
        insert_stock_data(cursor, stock_data)
        conn.commit()
    
    cursor.close()
    conn.close()

    # Generate KPI reports
    reports = generate_kpi_reports(stock_data)

    # Save reports to files
    save_reports(reports, 'daily')
    save_reports(reports, 'weekly')
    save_reports(reports, 'monthly')

if __name__ == "__main__":
    main()
