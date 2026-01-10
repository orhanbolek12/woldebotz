import pandas as pd

# BMEZ Holdings Data (Top 20 from SEC N-PORT-P filing as of 30 Sep 2025)
# Total Net Assets: ~$1,003,565,195 USD
holdings_data = [
    ('Alnylam Pharmaceuticals Inc', 53815296, 5.36),
    ('Veeva Systems Inc', 34693000, 3.46),
    ('Insmed Inc', 25992000, 2.59),
    ('Abbott Laboratories', 24732000, 2.46),
    ('Wuxi Biologics Cayman Inc', 23985000, 2.39),
    ('BlackRock Liquidity Funds', 23854000, 2.38),
    ('PsiQuantum Corp (Private)', 23478000, 2.34),
    ('Dexcom Inc', 20757000, 2.07),
    ('Galderma Group AG', 19869000, 1.98),
    ('Rhythm Pharmaceuticals Inc', 18750000, 1.87),
    ('Insulet Corp', 18164000, 1.81),
    ('Edwards Lifesciences Corp', 16860000, 1.68),
    ('Johnson & Johnson', 16043000, 1.60),
    ('Repligen Corp', 15968000, 1.59),
    ('Medtronic PLC', 15751000, 1.57),
    ('Genmab A/S', 15504000, 1.54),
    ('Exact Sciences Corp', 14450000, 1.44),
    ('Tenet Healthcare Corp', 14341000, 1.43),
    ('Guardant Health Inc', 13444000, 1.34),
    ('Neurocrine Biosciences Inc', 12845000, 1.28),
]

# Create DataFrame
df = pd.DataFrame(holdings_data, columns=['Company Name', 'Value (USD)', 'Portfolio Weight (%)'])

# Add formatted columns
df['Value (Millions USD)'] = df['Value (USD)'] / 1_000_000
df['Value (Formatted)'] = df['Value (USD)'].apply(lambda x: f'${x:,.0f}')

# Reorder columns
df = df[['Company Name', 'Value (Millions USD)', 'Value (Formatted)', 'Portfolio Weight (%)']]

# Sort by portfolio weight descending
df = df.sort_values('Portfolio Weight (%)', ascending=False).reset_index(drop=True)

# Calculate summary
total_value = df['Value (Millions USD)'].sum()
total_weight = df['Portfolio Weight (%)'].sum()

# Add summary row
summary_row = pd.DataFrame([{
    'Company Name': 'TOTAL (Top 20 Holdings)',
    'Value (Millions USD)': total_value,
    'Value (Formatted)': f'${total_value * 1_000_000:,.0f}',
    'Portfolio Weight (%)': total_weight
}])

df = pd.concat([df, summary_row], ignore_index=True)

# Create Excel file with formatting
try:
    with pd.ExcelWriter('BMEZ_Holdings_Report.xlsx', engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='BMEZ Holdings', index=False)
        
        # Get the workbook and worksheet
        workbook = writer.book
        worksheet = writer.sheets['BMEZ Holdings']
        
        # Format columns
        worksheet.column_dimensions['A'].width = 35
        worksheet.column_dimensions['B'].width = 20
        worksheet.column_dimensions['C'].width = 20
        worksheet.column_dimensions['D'].width = 22
        
        # Bold the header row
        for cell in worksheet[1]:
            cell.font = cell.font.copy(bold=True)
        
        # Bold the summary row
        last_row = len(df) + 1
        for cell in worksheet[last_row]:
            cell.font = cell.font.copy(bold=True)
    
    print("✅ BMEZ Holdings Report created successfully!")
    print(f"📊 Total Holdings: {len(df) - 1}")
    print(f"💰 Total Value: ${total_value:.2f}M")
    print(f"📈 Total Weight: {total_weight:.2f}%")
    print("\nFile saved as: BMEZ_Holdings_Report.xlsx")
    
except ImportError:
    print("❌ Error: openpyxl library is required")
    print("Please install it using: pip install openpyxl")
    # Fallback to CSV
    df.to_csv('BMEZ_Holdings_Report.csv', index=False)
    print("📄 CSV file created instead: BMEZ_Holdings_Report.csv")
