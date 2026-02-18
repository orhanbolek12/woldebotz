
import yfinance as yf
from datetime import datetime
import pandas as pd
import sys
import os

# Ticker lists
CEF_TICKERS = ["NAD", "ECF", "FAX", "AOD", "OPP", "NVG", "NEA", "BGB", "CEV", "ETW", "PDI", "RA", "FPF", "DIAX", "HYT", "NMZ", "AWF", "EVV", "BOE", "WIW", "VFL", "VCV", "EMD", "NCZ", "ARDC", "VMO", "BFK", "GLU", "VKI", "LGI", "PDT", "HQL", "ETV", "NMCO", "THQ", "NAC", "ERH", "BMEZ", "NQP", "BBN", "GDV", "MVT", "HTD", "BCX", "NAZ", "MMU", "EOT", "HPS", "HPI", "NML", "MEGI", "KTF", "AFB", "JPI", "NXP", "AIO", "RSF", "RQI", "HPF", "NMS", "LDP", "HQH", "PAI", "NRK", "IFN", "PTA", "NDMO", "ETJ", "ECAT", "BKT", "MQT", "ETG", "IIM", "NPV", "DPG", "BHV", "MUE", "RMM", "FTHY", "KIO", "RFM", "BCAT", "ASGI", "VGM", "GHY", "FMY", "MYD", "PCQ", "PFD", "EVM", "MQY", "MYN", "BDJ", "NMAI", "EVG", "RMMZ", "NKX", "EVN", "GDL", "BHK", "WEA", "BTT", "MUJ", "MAV", "SDHY", "EFR", "MIY", "BGT", "IGA", "NPFD", "BKN", "RIV", "IQI", "RMT", "IDE", "HNW", "JHI", "BNY", "BLE", "ETY", "DSU", "MHD", "BUI", "EXG", "TDF", "DBL", "EIM", "NPCT", "RFI", "ISD", "JCE", "NBB", "CAF", "MMD", "ADX", "MHI", "WDI", "MXF", "CEE", "PHD", "RNP", "BCV", "SPE", "GRX", "GF", "FMN", "THW", "JRI", "DNP", "UTF", "NMI", "SPXX", "BFZ", "PSF", "NFJ", "AGD", "DSL", "EOS", "VKQ", "PDO", "VBF", "MCI", "NUV", "GDO", "TEAF", "DLY", "NZF", "NBXG", "NCA", "BIT", "NXC", "JGH", "FINS", "KF", "NMT", "IGI", "HGLB", "RLTY", "VPV", "FFC", "NBH", "CII", "ENX", "BYM", "EMF", "EVT", "FFA", "ETX", "DFP", "BGX", "ERC", "MUC", "ETO", "PCN", "RGT", "TPZ", "RMI", "RFMZ", "PAXS", "STEW", "VLT", "SCD", "PHYS", "PFO", "PMO", "RVT", "VTN", "PFL", "SPPP", "PEO", "TBLD", "PSLV", "PTY", "QQQX", "PGZ", "DMB", "DMO", "DTF", "EEA", "EFT", "EIC", "EOI", "ETB", "FCT", "FLC", "FOF", "CSQ", "ACV", "AVK", "BANX", "BGH", "BGR", "BLW", "BSL", "BSTZ", "BTA", "BTZ", "BXMX", "CCD", "CEF", "CGO", "CHI", "CHN", "CHY", "CPZ", "FRA", "MHN", "MIO", "MPA", "MPV", "MUA", "MXE", "MYI", "NAN", "NCV", "NIE", "NIM", "NNY", "NOM", "NUW", "NXJ", "NXN", "GBAB", "GOF", "GUG", "HEQ", "HYI", "JHS", "JLS", "JOF", "IIF"]
PREF_TICKERS = ["CCID", "PSEC-A", "GS-D", "GS-C", "GS-A", "LANDO", "LANDP", "GOODO", "ADC-A", "GOODN", "LANDM"]

ALL_TICKERS = list(set(CEF_TICKERS + PREF_TICKERS))

def parse_ticker_yf(raw_ticker):
    if '-' in raw_ticker:
        parts = raw_ticker.split('-')
        if len(parts) == 2:
            base, suffix = parts
            if len(suffix) == 1:
                return f"{base}-P{suffix}"
            return f"{base}-{suffix}"
    return raw_ticker

print(f"{'Ticker':<10} | {'Status':<15} | {'Ex-Date':<12}")
print("-" * 40)

no_info = []

for raw in ALL_TICKERS:
    yf_sym = parse_ticker_yf(raw)
    try:
        t = yf.Ticker(yf_sym)
        ex_ts = t.info.get("exDividendDate")
        ex_date = None
        if ex_ts:
            ex_date = datetime.fromtimestamp(ex_ts).date().strftime('%Y-%m-%d')
        else:
            cal = t.calendar
            if cal and 'Ex-Dividend Date' in cal:
                val = cal['Ex-Dividend Date']
                if hasattr(val, 'iloc'): val = val.iloc[0]
                elif isinstance(val, list) and val: val = val[0]
                ex_date = val.strftime('%Y-%m-%d') if hasattr(val, 'strftime') else str(val)
        
        if not ex_date:
            no_info.append(raw)
            print(f"{raw:<10} | {'NO INFO':<15} | {'None'}")
        else:
            # print(f"{raw:<10} | {'OK':<15} | {ex_date}")
            pass
    except Exception as e:
        no_info.append(raw)
        print(f"{raw:<10} | {'ERROR':<15} | {'None'}")

print("\n--- TICKERS WITH NO INFO ---")
print(", ".join(no_info))
