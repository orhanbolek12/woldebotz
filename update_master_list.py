import openpyxl
import json
import os

# Paths
xlsx_path = r"c:\Users\orhan.bolek\Desktop\Updated Master List.xlsx"
tickers_path = "tickers.txt"
sector_map_path = "sector_map.json"

def update_master_list():
    if not os.path.exists(xlsx_path):
        print(f"[!] Excel file not found: {xlsx_path}")
        return

    print(f"[*] Loading Master List from {xlsx_path}...")
    # data_only=True to get values instead of formulas
    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    sheet = wb["Alpha"]
    
    header = [str(cell.value).strip() if cell.value else "" for cell in sheet[1]]
    
    try:
        ticker_idx = next(i for i, v in enumerate(header) if v == 'Ticker')
        yield_idx = next(i for i, v in enumerate(header) if v == 'Current Yield')
        sector_idx = next(i for i, v in enumerate(header) if v == 'Sector')
    except StopIteration:
        print("[!] Required columns (Ticker, Current Yield, Sector) not found in 'Alpha' sheet.")
        return

    new_tickers = []
    new_sector_map = {}
    
    # Load existing sector map to preserve any tickers not in the spreadsheet (optional, but user said sync)
    # The user said "Update with Updated Master List", so we might want to replace or merge.
    # Given the request, replacing seems more like a true "update" to the latest list.
    
    red_count = 0
    added_count = 0

    for row in sheet.iter_rows(min_row=2):
        ticker = row[ticker_idx].value
        if not ticker or not str(ticker).strip():
            continue
        
        ticker = str(ticker).strip().upper()
        sector = str(row[sector_idx].value).strip() if row[sector_idx].value else "Other"
        yield_cell = row[yield_idx]
        
        is_red = False
        if yield_cell.fill and yield_cell.fill.start_color and yield_cell.fill.start_color.type == 'rgb':
            if yield_cell.fill.start_color.rgb == 'FFFF0000':
                is_red = True
        
        if is_red:
            red_count += 1
            # print(f"[-] Skipping {ticker} (Red Yield)")
            continue
            
        new_tickers.append(ticker)
        new_sector_map[ticker] = sector
        added_count += 1

    # Unique and sorted
    unique_tickers = sorted(list(set(new_tickers)))
    
    # Save tickers.txt (Comma separated)
    with open(tickers_path, 'w') as f:
        f.write(", ".join(unique_tickers))
    
    # Save sector_map.json (Sorted keys for clean diff)
    with open(sector_map_path, 'w') as f:
        json.dump(dict(sorted(new_sector_map.items())), f, indent=4)

    print(f"\n[+] Update Complete:")
    print(f"    - Processed {added_count + red_count} rows with tickers.")
    print(f"    - Skipped {red_count} non-trading (red) tickers.")
    print(f"    - Saved {len(unique_tickers)} tickers to {tickers_path}.")
    print(f"    - Updated {len(new_sector_map)} mappings in {sector_map_path}.")

if __name__ == "__main__":
    update_master_list()
