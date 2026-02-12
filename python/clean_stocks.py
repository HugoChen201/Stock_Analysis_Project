#Step 2: Clean and preprocess the downloaded stock data
import pandas as pd

#List of stock tickers
tickers = [
    "AAPL",
    "TSLA",
    "NVDA",
    "MSFT",
    "GOOGL",
    "AMZN",
    "META",
    "JPM",
    "DIS",
    "NFLX",
    "PYPL",
]

#Clean data for each ticker
for ticker in tickers:
    #Read the CSV file 
    df = pd.read_csv(f'../data/{ticker}_historical_data.csv')
    
    #Handle multi-level headers from yfinance
    #Drop the header row that contains ticker symbols (usually row 0 after reading)
    if df.iloc[0].astype(str).str.contains('Ticker').any():
        df = df.drop(index=0).reset_index(drop=True)
    
    #Flatten multi-level columns if they exist
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if col[1] == '' else col[1] for col in df.columns.values]

    #"Price" column is actually the date column.
    if 'Price' in df.columns and 'Date' not in df.columns:
        df.rename(columns={'Price': 'Date'}, inplace=True)
    
    #Rename columns to match expected format (case-insensitive comparison)
    column_mapping = {
        'close': 'Close',
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'volume': 'Volume',
        'date': 'Date',
        'index': 'Date'  # In case date is the index
    }
    
    #Apply case-insensitive renaming
    df.rename(columns={col: column_mapping.get(col.lower(), col) for col in df.columns}, inplace=True)
    
    #If Date is the index, reset it
    if df.index.name and df.index.name.lower() == 'date':
        df = df.reset_index()
    
    #Ensure Date column exists (error testing)
    if 'Date' not in df.columns:
        raise ValueError(
            f"'Date' column not found for {ticker}. "
            f"Columns are: {df.columns.tolist()}"
        )
    
    #If the first Date cell literally equals "Date", drop that row
    if str(df.loc[0, 'Date']).strip().lower() == 'date':
        df = df.drop(index=0).reset_index(drop=True)

    #Convert Date to datetime format
    df['Date'] = pd.to_datetime(df['Date'])
    
    #Convert numeric columns to proper data types
    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    #Add daily return (percentage change in Close price)
    df['Daily Return'] = df['Close'].pct_change()
    
    #Add ticker column at the beginning
    df['ticker'] = ticker
    
    #Reorder columns to match SQL table structure
    df = df[['ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Daily Return']]
    
    #Remove rows where essential values are null
    df.dropna(subset=['Open', 'High', 'Low', 'Close'], inplace=True)
    
    #Format Date column as string (YYYY-MM-DD) for better CSV compatibility
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    
    #Save cleaned CSV
    df.to_csv(f'../data/cleaned/{ticker}_cleaned_data.csv', index=False)
    print(f'Cleaned and saved: {ticker}_cleaned_data.csv')


print("All stock data cleaned and saved.")