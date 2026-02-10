from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import pandas as pd

def dump_table():
    print("Initializing Chrome Driver...")
    options = webdriver.ChromeOptions()
    # options.add_argument('--headless') # Run visible for user to see
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    url = "https://www.ishares.com/us/products/239826/ishares-us-preferred-stock-etf"
    print(f"Navigating to {url}...")
    driver.get(url)
    
    try:
        # Wait for "Holdings" tab/link and click it if necessary
        print("Waiting for page load...")
        time.sleep(5)
        
        # Click "Holdings" tab if it exists
        try:
            # Try multiple selectors for the tab
            holdings_tab = driver.find_element(By.XPATH, "//a[contains(text(), 'Holdings')] | //li[contains(text(), 'Holdings')]")
            driver.execute_script("arguments[0].click();", holdings_tab) # Force click
            print("Clicked 'Holdings' tab.")
            time.sleep(5)
        except:
            print("Holdings tab not found or already active.")
            
        # Target the Search Box in the Holdings section
        print("Searching for 'Arbor'...")
        try:
            # Try multiple selectors
            search_box = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[placeholder*='Search'], input[aria-label*='Search'], input[id*='filter']"))
            )
            
            # Scroll into view
            driver.execute_script("arguments[0].scrollIntoView(true);", search_box)
            time.sleep(1)
            
            # Use JS to set value if standard send_keys fails
            try:
                search_box.clear()
                search_box.send_keys("Arbor")
            except:
                driver.execute_script("arguments[0].value = 'Arbor';", search_box)
                # Dispatch input event to trigger framework listeners
                driver.execute_script("arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", search_box)
                driver.execute_script("arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", search_box)
            
            search_box.send_keys("\n") # Enter key
            time.sleep(5) # Wait for filter to apply
        except Exception as e:
            print(f"Could not interact with search box: {e}")

        # Wait for table with specific headers
        print("Waiting for Holdings table...")
        # Look for a table that contains "Ticker" and "Name"
        table = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, "//table[contains(., 'Ticker') and contains(., 'Name')]"))
        )
        
        # Get all rows
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        
        print(f"Found {len(rows)} rows after search. Dumping...")
        
        data = []
        for i, row in enumerate(rows): # Dump ALL matching rows
            cols = row.find_elements(By.TAG_NAME, "td")
            row_data = [c.text for c in cols]
            data.append(row_data)
        
        if len(data) == 0:
            print("No rows found! Search might have failed.")
            
        # Save to file
        with open("ishares_table_dump.txt", "w", encoding="utf-8") as f:
            for row in data:
                f.write(" | ".join(row) + "\n")
                
        print("Dump saved to ishares_table_dump.txt")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    dump_table()
