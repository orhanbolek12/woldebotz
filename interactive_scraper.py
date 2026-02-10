from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd
import os

def interactive_scrape():
    print("Initializing Chrome Driver...")
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless') # Visible
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    url = "https://www.ishares.com/us/products/239826/ishares-us-preferred-stock-etf"
    print(f"Navigating to {url}...")
    driver.get(url)
    
    print("Waiting for user to filter for 'Arbor'...")
    
    max_retries = 60 # Wait up to 3 minutes (60 * 3s)
    found = False
    
    for i in range(max_retries):
        try:
            # Check for table rows
            rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
            
            # Check content of first few rows
            arbor_found = False
            data = []
            
            for row in rows:
                text = row.text
                if "Arbor" in text or "ARBOR" in text or "Abr" in text:
                    arbor_found = True
                    # Scrape this row
                    cols = row.find_elements(By.TAG_NAME, "td")
                    # Ticker, Name, Sector, Asset Class, Mkt Val, Wgt, Notional, Shares, CUSIP, ISIN, SEDOL, Date
                    row_data = [c.text for c in cols]
                    data.append(row_data)
            
            if arbor_found and len(data) > 0:
                print(f"Detected 'Arbor' in table! Found {len(data)} rows.")
                
                # Save to CSV
                df = pd.DataFrame(data)
                # Add headers based on our knowledge (Index 8 is CUSIP, 5 is Weight)
                # Ticker, Name, Sector, Asset, MV, Wgt, Notional, Shares, CUSIP, ISIN, SEDOL, Date
                headers = ["Ticker", "Name", "Sector", "Asset Class", "Market Value", "Weight (%)", "Notional Value", "Shares", "CUSIP", "ISIN", "SEDOL", "Date"]
                # Adjust if column count mismatches
                if df.shape[1] == len(headers):
                    df.columns = headers
                
                df.to_csv("arbor_holdings.csv", index=False)
                print("Saved to arbor_holdings.csv")
                found = True
                break
            
            if i % 5 == 0:
                print(f"Scanning... ({i*3}s)")
                
            time.sleep(3)
            
        except Exception as e:
            print(f"Error scanning: {e}")
            time.sleep(3)
            
    if not found:
        print("Timeout: 'Arbor' not found in table.")
    
    driver.quit()

if __name__ == "__main__":
    interactive_scrape()
