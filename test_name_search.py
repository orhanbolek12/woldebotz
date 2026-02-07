import yfinance as yf

holdings = [
    {"name": "BANK OF AMERICA CORP", "price": 1250.57},
    {"name": "NEXTERA ENERGY UNITS INC", "price": 56.32},
    {"name": "JPMORGAN CHASE & CO", "price": 25.09}
]

for h in holdings:
    print(f"Searching for: {h['name']} around ${h['price']}")
    search = yf.Search(h['name'])
    for quote in search.quotes:
        # Check if it's a preferred stock or has a related price
        # (This is just a diagnostic to see what yfinance returns for these generic names)
        print(f"  Result: {quote.get('symbol')} - {quote.get('shortname')}")
