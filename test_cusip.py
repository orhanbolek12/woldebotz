import yfinance as yf

cusips = ["060505682"] # BAC Series L
for cusip in cusips:
    try:
        # Some trick to search by CUSIP or find the ticker
        # yfinance doesn't directly support CUSIP lookup in the main Ticker class usually
        # but let's see what info we can get or if we can find it via search
        print(f"Searching for CUSIP: {cusip}")
        # Search for the string on Yahoo Finance via yfinance search
        search = yf.Search(cusip)
        print("Search results:")
        for quote in search.quotes:
            print(f"Ticker: {quote['symbol']}, Name: {quote['shortname']}")
    except Exception as e:
        print(f"Error: {e}")
