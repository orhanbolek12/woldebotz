"""
Script to create holdings Excel reports for multiple CEF tickers
"""
import pandas as pd
from openpyxl.styles import Font
import time

# List of CEF tickers to process
tickers = ['BMEZ', 'THW', 'HQL', 'GRX', 'HQH', 'BME']

def create_holdings_excel(ticker, holdings_data, output_filename):
    """
    Create an Excel file with holdings data
    
    Args:
        ticker: CEF ticker symbol
        holdings_data: List of tuples (company_name, value_usd, portfolio_weight_pct)
        output_filename: Name of the output Excel file
    """
    # Create DataFrame
    df = pd.DataFrame(holdings_data, columns=['Company Name', 'Value (USD)', 'Portfolio Weight (%)'])
    
    # Add formatted columns
    df['Value (Millions USD)'] = df['Value (USD)'] / 1_000_000
    df['Value (Formatted)'] = df['Value (USD)'].apply(lambda x: f'${x:,.0f}')
    
    # Reorder columns
    df = df[['Company Name', 'Value (Millions USD)', 'Value (Formatted)', 'Portfolio Weight (%)']]
    
    # Sort by portfolio weight descending
    df = df.sort_values('Portfolio Weight (%)', ascending=False).reset_index(drop=True)
    
    # Create Excel file with formatting
    with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name=f'{ticker} Holdings', index=False)
        
        # Get workbook and worksheet
        workbook = writer.book
        worksheet = writer.sheets[f'{ticker} Holdings']
        
        # Format columns
        worksheet.column_dimensions['A'].width = 40
        worksheet.column_dimensions['B'].width = 20
        worksheet.column_dimensions['C'].width = 20
        worksheet.column_dimensions['D'].width = 20
        
        # Add summary row
        total_value = df['Value (Millions USD)'].sum()
        total_weight = df['Portfolio Weight (%)'].sum()
        
        summary_row = worksheet.max_row + 2
        worksheet[f'A{summary_row}'] = 'TOTAL'
        worksheet[f'B{summary_row}'] = total_value
        worksheet[f'D{summary_row}'] = total_weight
        
        # Bold the header and summary
        for cell in worksheet[1]:
            cell.font = Font(bold=True)
        for cell in worksheet[summary_row]:
            cell.font = Font(bold=True)
    
    print(f"Created: {output_filename} ({len(df)} holdings)")
    return len(df), total_value, total_weight

if __name__ == '__main__':
    print("CEF Holdings Excel Generator")
    print("=" * 50)
    print(f"Processing {len(tickers)} CEF tickers...")
    print()
    
    # This script will be populated with actual data from web scraping
    # For now, it's a template
    print("Ready to process tickers:", ', '.join(tickers))
