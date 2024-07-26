import yfinance as yf
import mysql.connector
from mysql.connector import errorcode

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
        data = stock.history(period="1mo")  # Get data for the last month
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
    CREATE TABLE IF NOT EXISTS monthly_stocks (
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
    INSERT INTO monthly_stocks (ticker, date, open, high, low, close, volume)
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

if __name__ == "__main__":
    main()
