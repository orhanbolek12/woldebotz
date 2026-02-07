import os
# CRITICAL: Fix for Vercel read-only filesystem
# yfinance tries to create a cache in ~/.cache which is read-only.
# We must point it to /tmp which is writable.
os.environ['XDG_CACHE_HOME'] = '/tmp'

from flask import Flask, render_template, request, jsonify
from logic import fetch_and_process, fetch_imbalance, fetch_range_ai, analyze_dividend_recovery
import threading
import uuid
import time
from datetime import datetime, timedelta, timezone

import json
import os

app = Flask(__name__)

# Persistence files
HISTORY_FILE = 'results_history.json'
IMBALANCE_FILE = 'imbalance_history.json'

CEF_TICKERS = ["NAD", "ECF", "FAX", "AOD", "OPP", "NVG", "NEA", "BGB", "CEV", "ETW", "PDI", "RA", "FPF", "DIAX", "HYT", "NMZ", "AWF", "EVV", "BOE", "WIW", "VFL", "VCV", "EMD", "NCZ", "ARDC", "VMO", "BFK", "GLU", "VKI", "LGI", "PDT", "HQL", "ETV", "NMCO", "THQ", "NAC", "ERH", "BMEZ", "NQP", "BBN", "GDV", "MVT", "HTD", "BCX", "NAZ", "MMU", "EOT", "HPS", "HPI", "NML", "MEGI", "KTF", "AFB", "JPI", "NXP", "AIO", "RSF", "RQI", "HPF", "NMS", "LDP", "HQH", "PAI", "NRK", "IFN", "PTA", "NDMO", "ETJ", "ECAT", "BKT", "MQT", "ETG", "IIM", "NPV", "DPG", "BHV", "MUE", "RMM", "FTHY", "KIO", "RFM", "BCAT", "ASGI", "VGM", "GHY", "FMY", "MYD", "PCQ", "PFD", "EVM", "MQY", "MYN", "BDJ", "NMAI", "EVG", "RMMZ", "NKX", "EVN", "GDL", "BHK", "WEA", "BTT", "MUJ", "MAV", "SDHY", "EFR", "MIY", "BGT", "IGA", "NPFD", "BKN", "RIV", "IQI", "RMT", "IDE", "HNW", "JHI", "BNY", "BLE", "ETY", "DSU", "MHD", "BUI", "EXG", "TDF", "DBL", "EIM", "NPCT", "RFI", "ISD", "JCE", "NBB", "CAF", "MMD", "ADX", "MHI", "WDI", "MXF", "CEE", "PHD", "RNP", "BCV", "SPE", "GRX", "GF", "FMN", "THW", "JRI", "DNP", "UTF", "NMI", "SPXX", "BFZ", "PSF", "NFJ", "AGD", "DSL", "EOS", "VKQ", "PDO", "VBF", "MCI", "NUV", "GDO", "TEAF", "DLY", "NZF", "NBXG", "NCA", "BIT", "NXC", "JGH", "FINS", "KF", "NMT", "IGI", "HGLB", "RLTY", "VPV", "FFC", "NBH", "CII", "ENX", "BYM", "EMF", "EVT", "FFA", "ETX", "DFP", "BGX", "ERC", "MUC", "ETO", "PCN", "RGT", "TPZ", "RMI", "RFMZ", "PAXS", "STEW", "VLT", "SCD", "PHYS", "PFO", "PMO", "RVT", "VTN", "PFL", "SPPP", "PEO", "TBLD", "PSLV", "PTY", "QQQX", "PGZ", "DMB", "DMO", "DTF", "EEA", "EFT", "EIC", "EOI", "ETB", "FCT", "FLC", "FOF", "CSQ", "ACV", "AVK", "BANX", "BGH", "BGR", "BLW", "BSL", "BSTZ", "BTA", "BTZ", "BXMX", "CCD", "CEF", "CGO", "CHI", "CHN", "CHY", "CPZ", "FRA", "MHN", "MIO", "MPA", "MPV", "MUA", "MXE", "MYI", "NAN", "NCV", "NIE", "NIM", "NNY", "NOM", "NUW", "NXJ", "NXN", "GBAB", "GOF", "GUG", "HEQ", "HYI", "JHS", "JLS", "JOF", "IIF"]

@app.route('/get_cef_tickers', methods=['GET'])
def get_cef_tickers():
    return jsonify({'tickers': CEF_TICKERS})

# In-memory storage
jobs = {}
imbalance_cache = {
    'status': 'idle',
    'last_updated': None,
    'last_updated_ts': 0,
    'results': [],
    'baseline_tickers': [], # Tickers from the previous day's final run
    'progress': 0,
    'total': 0,
    'stop_requested': False
}

prefs_cache = {
    'status': 'idle',
    'last_updated': None,
    'last_updated_ts': 0,
    'results': [],
    'baseline_tickers': [], # Tickers from the previous day's final run
    'progress': 0,
    'total': 0,
    'stop_requested': False
}

def load_history():
    global prefs_cache, imbalance_cache
    # Load Main History
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r') as f:
                data = json.load(f)
                prefs_cache['results'] = data.get('results', [])
                prefs_cache['last_updated'] = data.get('last_updated')
                prefs_cache['last_updated_ts'] = data.get('last_updated_ts', 0)
                prefs_cache['baseline_tickers'] = data.get('baseline_tickers', [])
                if prefs_cache['results']:
                    prefs_cache['status'] = 'completed'
                print(f"Loaded {len(prefs_cache['results'])} prefs results from history.")
        except Exception as e:
            print(f"Error loading history: {e}")
    
    # Load Imbalance History
    if os.path.exists(IMBALANCE_FILE):
        try:
            with open(IMBALANCE_FILE, 'r') as f:
                data = json.load(f)
                imbalance_cache['results'] = data.get('results', [])
                imbalance_cache['last_updated'] = data.get('last_updated')
                imbalance_cache['last_updated_ts'] = data.get('last_updated_ts', 0)
                imbalance_cache['baseline_tickers'] = data.get('baseline_tickers', [])
                # If we have results, mark as completed so the UI can show them
                if imbalance_cache['results']:
                    imbalance_cache['status'] = 'completed'
                print(f"Loaded {len(imbalance_cache['results'])} imbalance results from history.")
        except Exception as e:
            print(f"Error loading imbalance history: {e}")

def save_history(target='all'):
    try:
        if target in ['all', 'prefs']:
            data = {
                'results': prefs_cache['results'],
                'last_updated': prefs_cache['last_updated'],
                'last_updated_ts': prefs_cache['last_updated_ts'],
                'baseline_tickers': prefs_cache['baseline_tickers']
            }
            with open(HISTORY_FILE, 'w') as f:
                json.dump(data, f)
            print(f"Saved {len(prefs_cache['results'])} prefs results to history.")
        if target in ['all', 'imbalance']:
            data = {
                'results': imbalance_cache['results'],
                'last_updated': imbalance_cache['last_updated'],
                'last_updated_ts': imbalance_cache['last_updated_ts'],
                'baseline_tickers': imbalance_cache['baseline_tickers']
            }
            with open(IMBALANCE_FILE, 'w') as f:
                json.dump(data, f)
            print(f"Saved {len(imbalance_cache['results'])} imbalance results to history.")
    except Exception as e:
        print(f"Error saving history: {e}")

def get_tr_time():
    # TR is UTC+3
    return datetime.now(timezone(timedelta(hours=3)))


def load_and_analyze_prefs(force=False):
    """Background task to analyze the big list from tickers.txt"""
    global prefs_cache
    
    # Check scheduling: only run if force=True or > 24 hours
    now_ts = time.time()
    if not force and (now_ts - prefs_cache['last_updated_ts'] < 86400) and prefs_cache['results']:
        print("Prefs Analysis skipped: Recent results exist (less than 24h old).")
        return

    prefs_cache['status'] = 'processing'
    prefs_cache['progress'] = 0
    prefs_cache['stop_requested'] = False
    
    try:
        def progress_wrapper(c, t):
            prefs_cache.update({'progress': c})
            return 'STOP' if prefs_cache.get('stop_requested') else None
        now_tr = get_tr_time()
        last_run_tr = datetime.fromtimestamp(prefs_cache['last_updated_ts'], tz=timezone(timedelta(hours=3)))
        
        # Baseline is the set of tickers from the LAST completed scan
        # This is loaded from history and will be updated after this scan completes
        
        baseline = set(prefs_cache['baseline_tickers'])
        
        with open('tickers.txt', 'r') as f:
            content = f.read()
        
        tickers = [t.strip() for t in content.replace('\n', ',').split(',') if t.strip()]
        unique_tickers = list(set(tickers))
        prefs_cache['total'] = len(unique_tickers)
        
        # Run logic
        new_results = fetch_and_process(unique_tickers, progress_callback=progress_wrapper)
        
        if prefs_cache.get('stop_requested'):
            print("Prefs Analysis STOPPED by user.")
            prefs_cache['status'] = 'completed' if prefs_cache['results'] else 'idle'
            return
        
        # Mark "NEW" tickers based on the baseline (yesterday's set)
        # We also want to make sure the result itself persists that it is new
        for res in new_results:
            is_new = res['ticker'] not in baseline
            res['is_new'] = is_new
        
        # Update cache with new results
        # Set current results as baseline for NEXT scan (so they won't show as "new" next time)
        prefs_cache.update({
            'results': new_results,
            'baseline_tickers': [r['ticker'] for r in new_results],
            'status': 'completed',
            'last_updated': now_tr.strftime("%Y-%m-%d %H:%M:%S TR"),
            'last_updated_ts': now_ts
        })
        
        save_history('prefs')
        print(f"Prefs Analysis Completed. Found {len(new_results)} matches.")
        
    except Exception as e:
        print(f"Error in background prefs analysis: {e}")
        prefs_cache['status'] = 'error'

def load_and_analyze_imbalance(force=False, days=20, min_green_bars=12, min_red_bars=12, long_wick=0.05, short_wick=0.05):
    global imbalance_cache
    now_ts = time.time()
    if not force and (now_ts - imbalance_cache['last_updated_ts'] < 86400) and imbalance_cache['results']:
        print("Imbalance Analysis skipped: Recent results exist (less than 24h old).")
        return

    imbalance_cache['status'] = 'processing'
    imbalance_cache['progress'] = 0
    imbalance_cache['stop_requested'] = False
    try:
        def progress_wrapper(c, t):
            imbalance_cache.update({'progress': c})
            return 'STOP' if imbalance_cache.get('stop_requested') else None
        now_tr = get_tr_time()
        # Baseline is loaded from history and represents the last completed scan
            
        baseline = set(imbalance_cache['baseline_tickers'])
        
        with open('tickers.txt', 'r') as f:
            content = f.read()
        tickers = [t.strip() for t in content.replace('\n', ',').split(',') if t.strip()]
        unique_tickers = list(set(tickers))
        imbalance_cache['total'] = len(unique_tickers)
        
        new_results = fetch_imbalance(unique_tickers, 
                                      days=days,
                                      min_count=min_green_bars, # Reuse the min_green as general min_count for bg task
                                      max_wick=long_wick,       # Reuse long_wick as general max_wick for bg task
                                      progress_callback=progress_wrapper)
        
        if imbalance_cache.get('stop_requested'):
            print("Imbalance Analysis STOPPED by user.")
            imbalance_cache['status'] = 'completed' if imbalance_cache['results'] else 'idle'
            return
        
        for res in new_results:
            is_new = res['ticker'] not in baseline
            is_new = res['ticker'] not in baseline
            res['is_new'] = is_new
            res['days'] = days
            res['max_wick'] = long_wick
            
        # Update cache and set current results as baseline for next scan
        imbalance_cache.update({
            'results': new_results,
            'baseline_tickers': [r['ticker'] for r in new_results],
            'status': 'completed',
            'last_updated': now_tr.strftime("%Y-%m-%d %H:%M:%S TR"),
            'last_updated_ts': now_ts
        })
        save_history('imbalance')
        print(f"Imbalance Analysis Completed. Found {len(new_results)} matches.")
    except Exception as e:
        print(f"Error in imbalance analysis: {e}")
        imbalance_cache['status'] = 'error'

# Startup - Load history only, no auto-scan
load_history()
print("App started. Use 'Force Sync' or 'Recalculate AI' to run analysis.")


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/prefs', methods=['GET'])
def get_prefs():
    return jsonify(prefs_cache)

@app.route('/imbalance', methods=['GET'])
def get_imbalance():
    return jsonify(imbalance_cache)

@app.route('/get_tickers', methods=['GET'])
def get_tickers():
    try:
        if os.path.exists('tickers.txt'):
            with open('tickers.txt', 'r') as f:
                content = f.read()
            # Clean and return as comma-separated string
            tickers = [t.strip() for t in content.replace('\n', ',').split(',') if t.strip()]
            return jsonify({'tickers': tickers})
        return jsonify({'tickers': []})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/refresh_prefs', methods=['POST'])
def refresh_prefs():
    if prefs_cache['status'] == 'processing':
        return jsonify({'status': 'processing', 'message': 'Prefs analysis already running'})
    
    threading.Thread(target=load_and_analyze_prefs, args=(True,), daemon=True).start()
    return jsonify({'status': 'started'})

@app.route('/refresh_imbalance', methods=['POST'])
def refresh_imbalance():
    if imbalance_cache['status'] == 'processing':
        return jsonify({'status': 'processing', 'message': 'Imbalance analysis already running'})
    
    # Get parameters from request
    days = int(request.form.get('days', 20))
    min_green = int(request.form.get('min_green_bars', 12))
    min_red = int(request.form.get('min_red_bars', 12))
    long_wick = float(request.form.get('long_wick_size', 0.05))
    short_wick = float(request.form.get('short_wick_size', 0.05))
    
    threading.Thread(target=load_and_analyze_imbalance, 
                    args=(True, days, min_green, min_green, long_wick, long_wick), 
                    daemon=True).start()
    return jsonify({'status': 'started'})

@app.route('/stop_prefs', methods=['POST'])
def stop_prefs():
    prefs_cache['stop_requested'] = True
    return jsonify({'status': 'stopping'})

@app.route('/stop_imbalance', methods=['POST'])
def stop_imbalance():
    imbalance_cache['stop_requested'] = True
    return jsonify({'status': 'stopping'})

@app.route('/find', methods=['POST'])
def find_spreads():
    raw_text = request.form.get('tickers', '')
    if not raw_text:
        return jsonify({'error': 'No tickers provided'}), 400
    
    # Split by comma or newline
    tickers = [t.strip() for t in raw_text.replace('\n', ',').split(',') if t.strip()]
    
    job_id = str(uuid.uuid4())
    jobs[job_id] = {'status': 'processing', 'progress': 0, 'total': len(tickers), 'results': []}
    
    # Start processing in background thread
    thread = threading.Thread(target=process_job, args=(job_id, tickers))
    thread.start()
    
    return jsonify({'job_id': job_id})

def process_job(job_id, tickers):
    def update_progress(current, total):
        jobs[job_id]['progress'] = current
        
    results = fetch_and_process(tickers, progress_callback=update_progress)
    
    # Mark NEW for manual jobs too, relative to today's baseline
    baseline = set(prefs_cache.get('baseline_tickers', []))
    for res in results:
        res['is_new'] = res['ticker'] not in baseline
        
    jobs[job_id]['results'] = results
    jobs[job_id]['status'] = 'completed'

@app.route('/status/<job_id>')
def job_status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    return jsonify(job)

@app.route('/result_item/<job_id>')
def get_results(job_id):
    # This might be redundant if status returns results, but kept for clarity if needed
    job = jobs.get(job_id)
    if job and job['status'] == 'completed':
        return jsonify(job['results'])
    return jsonify([])

@app.route('/find_imbalance', methods=['POST'])
def find_imbalance():
    raw_text = request.form.get('tickers', '')
    if not raw_text:
        return jsonify({'error': 'No tickers provided'}), 400
    
    tickers = [t.strip() for t in raw_text.replace('\n', ',').split(',') if t.strip()]
    
    # Get parameters from request
    days = int(request.form.get('days', 20))
    min_green = int(request.form.get('min_green_bars', 12))
    min_red = int(request.form.get('min_red_bars', 12))
    long_wick = float(request.form.get('long_wick_size', 0.05))
    short_wick = float(request.form.get('short_wick_size', 0.05))
    
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        'status': 'processing', 
        'progress': 0, 
        'total': len(tickers), 
        'results': [], 
        'type': 'imbalance',
        'days': days,
        'min_green_bars': min_green,
        'min_red_bars': min_red,
        'long_wick_size': long_wick,
        'short_wick_size': short_wick
    }
    
    thread = threading.Thread(target=process_imbalance_job, args=(job_id, tickers))
    thread.start()
    
    return jsonify({'job_id': job_id})

def process_imbalance_job(job_id, tickers):
    def update_progress(current, total):
        jobs[job_id]['progress'] = current
    
    # Get parameters from job metadata
    job_data = jobs[job_id]
    days = job_data.get('days', 20)
    min_green = job_data.get('min_green_bars', 12)
    min_red = job_data.get('min_red_bars', 12)
    long_wick = job_data.get('long_wick_size', 0.05)
    short_wick = job_data.get('short_wick_size', 0.05)
        
    results = fetch_imbalance(tickers, 
                             days=days,
                             min_count=min_green,
                             max_wick=long_wick,
                             progress_callback=update_progress)
    
    # NEW logic for manual imbalance search
    baseline = set(imbalance_cache.get('baseline_tickers', []))
    for res in results:
        res['is_new'] = res['ticker'] not in baseline
        res['days'] = days
        res['max_wick'] = long_wick
        
    jobs[job_id]['results'] = results
    jobs[job_id]['results'] = results
    jobs[job_id]['status'] = 'completed'

@app.route('/analyze_range_batch', methods=['POST'])
def analyze_range_batch():
    """
    Synchronous endpoint for Range AI batch processing.
    """
    tickers_str = request.form.get('tickers', '')
    if not tickers_str:
        return jsonify({'results': []})
    
    tickers = [t.strip() for t in tickers_str.split(',') if t.strip()]
    
    # Get parameters
    days = int(request.form.get('days', 90))
    max_points = float(request.form.get('max_points', 1.0))
    max_percent = float(request.form.get('max_percent', 5.0))
    
    try:
        results = fetch_range_ai(tickers, 
                                days=days, 
                                max_points=max_points, 
                                max_percent=max_percent)
    except Exception as e:
        import traceback
        trace = traceback.format_exc()
        print(f"Range AI Batch Failed: {e}\n{trace}")
        return jsonify({'results': [], 'error': str(e), 'trace': trace})
        
    return jsonify({'results': results})

@app.route('/analyze_imbalance_batch', methods=['POST'])
def analyze_imbalance_batch():
    """
    Synchronous endpoint for processing a small batch of tickers.
    Designed for client-side chunking to avoid Vercel timeouts/background thread issues.
    """
    tickers_str = request.form.get('tickers', '')
    if not tickers_str:
        return jsonify({'results': []})
    
    tickers = [t.strip() for t in tickers_str.split(',') if t.strip()]
    
    # Get parameters
    days = int(request.form.get('days', 30))
    min_count = int(request.form.get('min_count', 20))
    candle_color = request.form.get('candle_color', 'Green') # 'Green' or 'Red'
    max_wick = float(request.form.get('max_wick', 0.12))
    
    # Run analysis synchronously with error capture
    try:
        results = fetch_imbalance(tickers, 
                                 days=days,
                                 min_count=min_count,
                                 max_wick=max_wick)
    except Exception as e:
        import traceback
        trace = traceback.format_exc()
        print(f"Batch Analysis Failed: {e}\n{trace}")
        return jsonify({'results': [], 'error': str(e), 'trace': trace})
    
    # Mark 'is_new' relative to baseline (still using in-memory baseline for now)
    baseline = set(imbalance_cache.get('baseline_tickers', []))
    for res in results:
        res['is_new'] = res['ticker'] not in baseline
        res['days'] = days
        res['min_count'] = min_count
        res['max_wick'] = max_wick
        
    return jsonify({'results': results})


@app.route('/analyze_dividend_recovery', methods=['POST'])
def analyze_dividend_recovery_endpoint():
    """
    Endpoint for dividend recovery analysis.
    Accepts tickers, lookback, and recovery_window parameters.
    """
    tickers_str = request.form.get('tickers', '')
    lookback = int(request.form.get('lookback', 3))
    recovery_window = int(request.form.get('recovery_window', 5))
    
    if not tickers_str.strip():
        return jsonify({'results': [], 'error': 'No tickers provided'})
    
    # Parse tickers
    tickers = [t.strip() for t in tickers_str.replace('\n', ',').split(',') if t.strip()]
    
    results = []
    for ticker in tickers:
        result = analyze_dividend_recovery(ticker, lookback, recovery_window)
        results.append(result)
    
    return jsonify({'results': results})


@app.route('/get_master_list_tickers', methods=['GET'])
def get_master_list_tickers():
    """
    Returns the current Master List tickers from tickers.txt
    """
    try:
        if os.path.exists('tickers.txt'):
            with open('tickers.txt', 'r') as f:
                content = f.read()
            tickers = [t.strip() for t in content.replace('\n', ',').split(',') if t.strip()]
            return jsonify({'tickers': sorted(list(set(tickers)))})
        return jsonify({'tickers': []})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/add_master_list_ticker', methods=['POST'])
def add_master_list_ticker():
    """
    Adds a new ticker to the Master List (tickers.txt)
    """
    ticker = request.form.get('ticker', '').strip().upper()
    if not ticker:
        return jsonify({'error': 'No ticker provided'}), 400
    
    try:
        # Read existing tickers
        tickers = []
        if os.path.exists('tickers.txt'):
            with open('tickers.txt', 'r') as f:
                content = f.read()
            tickers = [t.strip() for t in content.replace('\n', ',').split(',') if t.strip()]
        
        # Add new ticker if not already present
        if ticker not in tickers:
            tickers.append(ticker)
            # Write back to file
            with open('tickers.txt', 'w') as f:
                f.write(','.join(sorted(tickers)))
            return jsonify({'success': True, 'message': f'{ticker} added to Master List'})
        else:
            return jsonify({'success': False, 'message': f'{ticker} already exists in Master List'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/delete_master_list_ticker', methods=['POST'])
def delete_master_list_ticker():
    """
    Removes a ticker from the Master List (tickers.txt)
    """
    ticker = request.form.get('ticker', '').strip().upper()
    if not ticker:
        return jsonify({'error': 'No ticker provided'}), 400
    
    try:
        if os.path.exists('tickers.txt'):
            with open('tickers.txt', 'r') as f:
                content = f.read()
            tickers = [t.strip() for t in content.replace('\n', ',').split(',') if t.strip()]
            
            if ticker in tickers:
                tickers.remove(ticker)
                # Write back to file
                with open('tickers.txt', 'w') as f:
                    f.write(','.join(sorted(tickers)))
                return jsonify({'success': True, 'message': f'{ticker} removed from Master List'})
            else:
                return jsonify({'success': False, 'message': f'{ticker} not found in Master List'})
        return jsonify({'error': 'Master List file not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/get_pff_holdings', methods=['GET'])
def get_pff_holdings():
    """
    Returns PFF holdings from the CSV file, sorted by weight (descending)
    """
    try:
        import pandas as pd
        csv_path = os.path.join(os.environ.get('TEMP', '/tmp'), 'pff_holdings.csv')
        
        if not os.path.exists(csv_path):
            return jsonify({'error': 'PFF holdings CSV not found. Please run the analyzer script first.'}), 404
        
        # Read CSV (skip first 9 rows which are metadata)
        df = pd.read_csv(csv_path, skiprows=9)
        
        # Extract relevant columns and sort by weight
        holdings = []
        for _, row in df.iterrows():
            ticker = row.get('Ticker')
            name = row.get('Name')
            weight = row.get('Weight (%)')
            
            if pd.notna(ticker) and ticker != '-':
                holdings.append({
                    'ticker': ticker,
                    'name': name if pd.notna(name) else '',
                    'weight': float(weight) if pd.notna(weight) else 0.0
                })
        
        # Sort by weight descending
        holdings.sort(key=lambda x: x['weight'], reverse=True)
        
        return jsonify({'holdings': holdings})
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500


if __name__ == '__main__':
    # Railway uses PORT environment variable
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
