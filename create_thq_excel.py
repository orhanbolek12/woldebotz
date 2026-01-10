import pandas as pd

# THQ Holdings Data with Portfolio Weights
holdings_data = [
    ('Eli Lilly and Co.', 62928425, 5.62),
    ('AbbVie Inc.', 57870413, 5.46),
    ('Abbott Laboratories', 52525375, 4.91),
    ('Merck & Co Inc.', 34642443, 4.62),
    ('Medtronic PLC', 33308476, 3.55),
    ('Intuitive Surgical Inc.', 27017164, 3.29),
    ('Danaher Corp', 25800565, 3.28),
    ('UnitedHealth Group Inc.', 24792195, 2.79),
    ('Thermo Fisher Scientific Inc.', 23477878, 2.79),
    ('Becton Dickinson & Co.', 21663243, 2.39),
    ('Zoetis Inc.', 21152312, 2.29),
    ('CVS Health Corp', 20737754, 2.10),
    ('Boston Scientific Corp', 18868853, 1.98),
    ('Bristol-Myers Squibb Co.', 17800654, 1.89),
    ('Humana Inc.', 17004191, 1.81),
    ('Elevance Health Inc.', 16673961, 1.77),
    ('Cytokinetics Inc.', 16506851, 1.75),
    ('Sarepta Therapeutics Inc.', 15004740, 1.59),
    ('ARS Pharmaceuticals Inc.', 13376490, 1.42),
    ('BioMarin Pharmaceutical Inc.', 13132283, 1.39),
    ('Stryker Corp', 12998337, 1.38),
    ('Omega Healthcare Investors (REIT)', 12576874, 1.34),
    ('Inspire Medical Systems Inc.', 12304586, 1.31),
    ('Sabra Health Care REIT Inc.', 11684708, 1.24),
    ('Healthpeak Properties Inc. (REIT)', 10871474, 1.16),
    ('Exact Sciences Corp', 9712612, 1.03),
    ('Welltower Inc. (REIT)', 9224624, 0.98),
    ('Molina Healthcare Inc.', 8725633, 0.93),
    ('LivaNova PLC', 8647519, 0.92),
    ('Abivax SA (ADR)', 8087659, 0.86),
    ('Oculis Holding AG', 7560462, 0.80),
    ('Ventas Inc. (REIT)', 7434058, 0.79),
    ('Dexcom Inc.', 7310318, 0.78),
    ('Repligen Corp', 7262291, 0.77),
    ('Vericel Corp', 7059476, 0.75),
    ('Insulet Corp', 7001379, 0.74),
    ('Acadia Healthcare Co. Inc.', 6775500, 0.72),
    ('LTC Properties Inc. (REIT)', 6149796, 0.65),
    ('NeoGenomics Inc.', 6116270, 0.65),
    ('uniQure NV', 6016429, 0.64),
    ('Veracyte Inc.', 5564962, 0.59),
    ('Tenet Healthcare Corp', 5274167, 0.56),
    ('McKesson Corp', 5203057, 0.55),
    ('Myriad Genetics Inc.', 4620353, 0.49),
    ('ResMed Inc.', 4417728, 0.47),
    ('Regeneron Pharmaceuticals Inc.', 4384019, 0.47),
    ('HCA Healthcare Inc.', 3912516, 0.42),
    ('Tandem Diabetes Care Inc.', 3715653, 0.39),
    ('Sera Prognostics Inc.', 3469970, 0.37),
    ('Community Health Systems Inc.', 2977394, 0.32),
    ('Lucid Diagnostics Inc.', 2525000, 0.27),
    ('InspireMD Inc.', 1952399, 0.21),
    ('Arcus Biosciences Inc.', 1790154, 0.19),
    ('Veradigm Inc.', 1623442, 0.17),
    ('Medical Properties Trust (REIT)', 1351444, 0.14),
    ('Diversified Healthcare Trust (REIT)', 1296006, 0.14),
    ('Perrigo Co. PLC', 630061, 0.07),
    ('National Health Investors (REIT)', 444882, 0.05),
    ('Healthcare Realty Trust (REIT)', 452895, 0.05),
    ('Global Medical REIT Inc.', 213485, 0.02),
    # Preferred Stocks
    ('Abcuro Inc. (Preferred)', 4100000, 0.44),
    ('Atalanta Therapeutics Inc. (Preferred)', 3500000, 0.37),
    ('Glycomine Inc. (Preferred)', 4100000, 0.44),
    ('Seismic Therapeutics Inc. (Preferred)', 3300000, 0.35),
    ('Third Arc Bio Inc. (Preferred)', 4400000, 0.47),
    ('Crystalys Therapeutics Inc. (Preferred)', 2500000, 0.27),
    ('Endeavor Biomedicines Inc. (Preferred)', 4400000, 0.47),
    ('Engrail Therapeutics Inc. (Preferred)', 1700000, 0.18),
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

# Create Excel file with formatting
with pd.ExcelWriter('THQ_Holdings_Report.xlsx', engine='openpyxl') as writer:
    df.to_excel(writer, sheet_name='THQ Holdings', index=False)
    
    # Get workbook and worksheet
    workbook = writer.book
    worksheet = writer.sheets['THQ Holdings']
    
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
    from openpyxl.styles import Font
    for cell in worksheet[1]:
        cell.font = Font(bold=True)
    for cell in worksheet[summary_row]:
        cell.font = Font(bold=True)

print(f"✓ Excel file created: THQ_Holdings_Report.xlsx")
print(f"✓ Total Holdings: {len(df)}")
print(f"✓ Total Value: ${df['Value (USD)'].sum():,.0f} (${total_value:.2f}M)")
print(f"✓ Total Portfolio Weight: {total_weight:.2f}%")
