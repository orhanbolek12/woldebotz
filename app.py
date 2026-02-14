import os
# CRITICAL: Fix for Vercel read-only filesystem
# yfinance tries to create a cache in ~/.cache which is read-only.
# We must point it to /tmp which is writable.
os.environ['XDG_CACHE_HOME'] = '/tmp'

from flask import Flask, render_template, request, jsonify
from logic import fetch_and_process, fetch_imbalance, fetch_range_ai, analyze_dividend_recovery, fetch_rebalance_patterns
import threading
import uuid
import time
from datetime import datetime, timedelta, timezone

import json
import os
import glob
import pandas as pd
import logging

app = Flask(__name__)

# Caching for sector map to avoid repeated disk reads
_sector_map_cache = None

SECTOR_MAP_FILE = 'sector_map.json'

def get_sector_map():
    global _sector_map_cache
    
    # Return cached version if available
    if _sector_map_cache is not None:
        return _sector_map_cache

    try:
        # Load from static JSON file (deployed with the app)
        if os.path.exists(SECTOR_MAP_FILE):
            with open(SECTOR_MAP_FILE, 'r', encoding='utf-8') as f:
                _sector_map_cache = json.load(f)
            logging.info(f"Sector Map: Loaded {len(_sector_map_cache)} mappings from {SECTOR_MAP_FILE}")
            return _sector_map_cache
        else:
            logging.error(f"Sector Map: {SECTOR_MAP_FILE} not found.")
            return {}
            
    except Exception as e:
        import traceback
        logging.error(f"Error loading sector map: {e}\n{traceback.format_exc()}")
        return {}

# Persistence files
HISTORY_FILE = 'results_history.json'
IMBALANCE_FILE = 'imbalance_history.json'

# Helper to get tickers from file
def get_tickers_from_file(filename):
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                content = f.read()
            return sorted(list(set([t.strip().upper() for t in content.replace('\n', ',').split(',') if t.strip()])))
        return []
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return []

def save_tickers_to_file(filename, tickers):
    try:
        with open(filename, 'w') as f:
            f.write(','.join(sorted(list(set(tickers)))))
        return True
    except Exception as e:
        print(f"Error saving to {filename}: {e}")
        return False

@app.route('/get_cef_tickers', methods=['GET'])
def get_cef_tickers():
    return jsonify({'tickers': get_tickers_from_file('cef_tickers.txt')})

@app.route('/add_cef_ticker', methods=['POST'])
def add_cef_ticker():
    data = request.get_json()
    ticker = data.get('ticker', '').strip().upper()
    if not ticker:
        return jsonify({'error': 'Ticker cannot be empty'}), 400
    
    current_tickers = get_tickers_from_file('cef_tickers.txt')
    if ticker not in current_tickers:
        current_tickers.append(ticker)
        if save_tickers_to_file('cef_tickers.txt', current_tickers):
            return jsonify({'message': f'Ticker {ticker} added.', 'tickers': sorted(current_tickers)}), 200
        else:
            return jsonify({'error': 'Failed to save tickers'}), 500
    else:
        return jsonify({'message': f'Ticker {ticker} already exists.', 'tickers': sorted(current_tickers)}), 200

@app.route('/remove_cef_ticker', methods=['POST'])
def remove_cef_ticker():
    data = request.get_json()
    ticker = data.get('ticker', '').strip().upper()
    if not ticker:
        return jsonify({'error': 'Ticker cannot be empty'}), 400
    
    current_tickers = get_tickers_from_file('cef_tickers.txt')
    if ticker in current_tickers:
        current_tickers.remove(ticker)
        if save_tickers_to_file('cef_tickers.txt', current_tickers):
            return jsonify({'message': f'Ticker {ticker} removed.', 'tickers': sorted(current_tickers)}), 200
        else:
            return jsonify({'error': 'Failed to save tickers'}), 500
    else:
        return jsonify({'message': f'Ticker {ticker} not found.', 'tickers': sorted(current_tickers)}), 200

@app.route('/update_cef_tickers', methods=['POST'])
def update_cef_tickers():
    data = request.get_json()
    tickers_list = data.get('tickers', [])
    if not isinstance(tickers_list, list):
        return jsonify({'error': 'Invalid input, expected a list of tickers'}), 400
    
    cleaned_tickers = sorted(list(set([t.strip().upper() for t in tickers_list if t.strip()])))
    
    if save_tickers_to_file('cef_tickers.txt', cleaned_tickers):
        return jsonify({'message': 'CEF tickers updated successfully.', 'tickers': cleaned_tickers}), 200
    else:
        return jsonify({'error': 'Failed to save tickers'}), 500

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

def load_and_analyze_imbalance(force=False, days=20, min_green_bars=12, min_red_bars=12, long_wick=0.05, short_wick=0.05, min_profit=0.10, filter_wick=True, filter_profit=False):
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
                                      min_count=min_green_bars, 
                                      max_wick=long_wick,
                                      min_profit=min_profit,
                                      filter_wick=filter_wick,
                                      filter_profit=filter_profit,
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
    min_profit = float(request.form.get('min_profit', 0.10))
    # Checkbox handling: 'true' string from JS FormData
    filter_wick = request.form.get('filter_wick', 'true').lower() == 'true'
    filter_profit = request.form.get('filter_profit', 'false').lower() == 'true'

    threading.Thread(target=load_and_analyze_imbalance, 
                    args=(True, days, min_green, min_green, long_wick, long_wick, min_profit, filter_wick, filter_profit), 
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
        'short_wick_size': short_wick,
        'min_profit': float(request.form.get('min_profit', 0.10)),
        'filter_wick': request.form.get('filter_wick', 'true').lower() == 'true',
        'filter_profit': request.form.get('filter_profit', 'false').lower() == 'true'
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
    min_profit = job_data.get('min_profit', 0.10)
    filter_wick = job_data.get('filter_wick', True)
    filter_profit = job_data.get('filter_profit', False)
        
    results = fetch_imbalance(tickers, 
                             days=days,
                             min_count=min_green,
                             max_wick=long_wick,
                             min_profit=min_profit,
                             filter_wick=filter_wick,
                             filter_profit=filter_profit,
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
    days = 90 # Hardcoded for Phase 3 Range AI Logic
    # days = int(request.form.get('days', 90))
    min_points = float(request.form.get('min_points', 0.5))
    max_points = float(request.form.get('max_points', 1.0))
    max_percent = float(request.form.get('max_percent', 5.0))
    
    # Checkbox filters (sent as strings 'true'/'false')
    filter_min_point = request.form.get('filter_min_point', 'false').lower() == 'true'
    filter_point = request.form.get('filter_point', 'true').lower() == 'true'
    filter_percent = request.form.get('filter_percent', 'true').lower() == 'true'
    
    try:
        results = fetch_range_ai(tickers, 
                                days=days, 
                                min_points=min_points,
                                max_points=max_points, 
                                max_percent=max_percent,
                                filter_min_point=filter_min_point,
                                filter_point=filter_point,
                                filter_percent=filter_percent)
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
    min_profit = float(request.form.get('min_profit', 0.10))
    filter_wick = request.form.get('filter_wick', 'true').lower() == 'true'
    filter_profit = request.form.get('filter_profit', 'false').lower() == 'true'
    
    # Run analysis synchronously with error capture
    try:
        results = fetch_imbalance(tickers, 
                                 days=days,
                                 min_count=min_count,
                                 max_wick=max_wick,
                                 min_profit=min_profit,
                                 filter_wick=filter_wick,
                                 filter_profit=filter_profit)
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

@app.route('/analyze_rebalance_batch', methods=['POST'])
def analyze_rebalance_batch():
    """
    Synchronous endpoint for month-end rebalance pattern analysis.
    Designed for small batches to avoid timeouts.
    """
    tickers_str = request.form.get('tickers', '')
    months_back = int(request.form.get('months_back', 12))
    
    if not tickers_str.strip():
        return jsonify({'results': []})
        
    tickers = [t.strip().upper() for t in tickers_str.replace('\n', ',').split(',') if t.strip()]
    
    try:
        results = fetch_rebalance_patterns(tickers, months_back=months_back)
    except Exception as e:
        import traceback
        trace = traceback.format_exc()
        print(f"Rebalance Analysis Failed: {e}\n{trace}")
        return jsonify({'results': [], 'error': str(e), 'trace': trace})
        
    return jsonify({'results': results})


@app.route('/get_master_list_tickers', methods=['GET'])
def get_master_list_tickers():
    """
    Returns the current Master List tickers from tickers.txt with sector info
    """
    try:
        sector_map = get_sector_map()
        if os.path.exists('tickers.txt'):
            with open('tickers.txt', 'r') as f:
                content = f.read()
            tickers = [t.strip().upper() for t in content.replace('\n', ',').split(',') if t.strip()]
            unique_tickers = sorted(list(set(tickers)))
            
            # Map tickers to objects with sector
            ticker_objects = []
            for t in unique_tickers:
                ticker_objects.append({
                    'ticker': t,
                    'sector': sector_map.get(t, 'Other')
                })
            return jsonify({'tickers': ticker_objects})
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
        tickers = get_tickers_from_file('tickers.txt')
        if ticker in tickers:
            tickers.remove(ticker)
            if save_tickers_to_file('tickers.txt', tickers):
                return jsonify({'success': True, 'message': f'{ticker} removed from Master List'})
            return jsonify({'success': False, 'message': 'Failed to save changes'}), 500
        else:
            return jsonify({'success': False, 'message': f'{ticker} not found in Master List'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/get_cef_list_tickers', methods=['GET'])
def get_cef_list_tickers():
    """
    Returns the current CEF List tickers from cef_tickers.txt
    """
    return jsonify({'tickers': get_tickers_from_file('cef_tickers.txt')})


@app.route('/add_cef_list_ticker', methods=['POST'])
def add_cef_list_ticker():
    """
    Adds a new ticker to the CEF List (cef_tickers.txt)
    """
    ticker = request.form.get('ticker', '').strip().upper()
    if not ticker:
        return jsonify({'error': 'No ticker provided'}), 400
    
    try:
        tickers = get_tickers_from_file('cef_tickers.txt')
        if ticker not in tickers:
            tickers.append(ticker)
            if save_tickers_to_file('cef_tickers.txt', tickers):
                return jsonify({'success': True, 'message': f'{ticker} added to CEF List'})
            return jsonify({'success': False, 'message': 'Failed to save changes'}), 500
        else:
            return jsonify({'success': False, 'message': f'{ticker} already exists in CEF List'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/delete_cef_list_ticker', methods=['POST'])
def delete_cef_list_ticker():
    """
    Removes a ticker from the CEF List (cef_tickers.txt)
    """
    ticker = request.form.get('ticker', '').strip().upper()
    if not ticker:
        return jsonify({'error': 'No ticker provided'}), 400
    
    try:
        tickers = get_tickers_from_file('cef_tickers.txt')
        if ticker in tickers:
            tickers.remove(ticker)
            if save_tickers_to_file('cef_tickers.txt', tickers):
                return jsonify({'success': True, 'message': f'{ticker} removed from CEF List'})
            return jsonify({'success': False, 'message': 'Failed to save changes'}), 500
        else:
            return jsonify({'success': False, 'message': f'{ticker} not found in CEF List'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/get_pff_holdings', methods=['GET'])
def get_pff_holdings():
    """
    Returns PFF holdings. 
    Prioritizes 'pff_preferred_stocks_analysis.csv' (Analyzed Preferred Stocks).
    Falls back to 'pff_holdings.csv' (Raw ETF Holdings) if analysis not found.
    """
    try:
        import pandas as pd
        
        # 1. Try to load the Analyzed Preferred Stocks file first
        analysis_path = 'pff_holdings_tickers.csv'
        if os.path.exists(analysis_path):
            try:
                # Format: Base Ticker,Company Name,Preferred Stock,Last Price,Full Name
                df = pd.read_csv(analysis_path)
                holdings = []
                for _, row in df.iterrows():
                    ticker = row.get('Preferred Stock')
                    name = row.get('Full Name')
                    last_price = row.get('Last Price')
                    weight = row.get('Weight (%)')
                    market_value = row.get('Market Value')
                    quantity = row.get('Quantity')
                    
                    if pd.notna(ticker):
                        holdings.append({
                            'ticker': ticker,
                            'name': name if pd.notna(name) else '',
                            'price': float(last_price) if pd.notna(last_price) else 0.0,
                            'weight': float(weight) if pd.notna(weight) else 0.0,
                            'market_value': float(market_value) if pd.notna(market_value) else 0.0,
                            'quantity': float(quantity) if pd.notna(quantity) else 0.0,
                            'is_analyzed': True 
                        })
                # Map sectors to analyzed holdings
                sector_map = get_sector_map()
                for h in holdings:
                    if h.get('ticker'):
                        h['sector'] = sector_map.get(h['ticker'].upper(), 'Other')
                    else:
                        h['sector'] = 'Other'
                return jsonify({'holdings': holdings, 'source': 'analysis'})
            except Exception as e:
                logging.error(f"Failed to read analysis file: {e}")
                # Fallthrough to raw file
        
        # 2. Fallback to Raw PFF Holdings CSV
        csv_path = os.path.join(os.environ.get('TEMP', '/tmp'), 'pff_holdings.csv')
        
        if not os.path.exists(csv_path):
            return jsonify({'error': 'No PFF data found. Please run the analyzer script.'}), 404
        
        # Read CSV (skip first 9 rows which are metadata)
        df = pd.read_csv(csv_path, skiprows=9)
        
        # Extract relevant columns and sort by weight
        holdings = []
        for _, row in df.iterrows():
            ticker = row.get('Ticker')
            name = row.get('Name')
            weight = row.get('Weight (%)')
            market_value = row.get('Market Value')
            
            if pd.notna(ticker) and ticker != '-':
                # Handle Market Value string format "1,234.56"
                mv_val = 0.0
                if pd.notna(market_value):
                    try:
                        mv_val = float(str(market_value).replace(',', ''))
                    except:
                        pass

                holdings.append({
                    'ticker': ticker,
                    'name': name if pd.notna(name) else '',
                    'weight': float(weight) if pd.notna(weight) else 0.0,
                    'market_value': mv_val,
                    'is_analyzed': False
                })
        
        # Map Sectors to PFF holdings
        sector_map = get_sector_map()
        for h in holdings:
            if h.get('ticker'):
                h['sector'] = sector_map.get(h['ticker'].upper(), 'Other')
            else:
                h['sector'] = 'Other'

        return jsonify({'holdings': holdings, 'source': 'raw'})
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500


if __name__ == '__main__':
    # Railway uses PORT environment variable
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
