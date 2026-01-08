from flask import Flask, render_template, request, jsonify
from logic import fetch_and_process
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

# In-memory storage
jobs = {}
prefs_cache = {
    'status': 'idle', # idle, processing, completed
    'last_updated': None,
    'last_updated_ts': 0,
    'results': [],
    'progress': 0,
    'total': 0
}

imbalance_cache = {
    'status': 'idle',
    'last_updated': None,
    'last_updated_ts': 0,
    'results': [],
    'progress': 0,
    'total': 0
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
                'last_updated_ts': prefs_cache['last_updated_ts']
            }
            with open(HISTORY_FILE, 'w') as f:
                json.dump(data, f)
            print(f"Saved {len(prefs_cache['results'])} prefs results to history.")
        if target in ['all', 'imbalance']:
            data = {
                'results': imbalance_cache['results'],
                'last_updated': imbalance_cache['last_updated'],
                'last_updated_ts': imbalance_cache['last_updated_ts']
            }
            with open(IMBALANCE_FILE, 'w') as f:
                json.dump(data, f)
            print(f"Saved {len(imbalance_cache['results'])} imbalance results to history.")
    except Exception as e:
        print(f"Error saving history: {e}")

def get_tr_time():
    # TR is UTC+3
    return datetime.now(timezone(timedelta(hours=3)))

def scheduler_loop():
    """Background loop to check for 16:20 TR daily trigger"""
    while True:
        try:
            now = get_tr_time()
            target_time = now.replace(hour=16, minute=20, second=0, microsecond=0)
            
            # Check Prefs
            last_run_prefs = datetime.fromtimestamp(prefs_cache['last_updated_ts'], tz=timezone(timedelta(hours=3)))
            if now >= target_time and last_run_prefs.date() < now.date() and prefs_cache['status'] != 'processing':
                print(f"[{now}] Scheduler triggering daily Prefs analysis...")
                threading.Thread(target=load_and_analyze_prefs, args=(True,), daemon=True).start()
            
            # Check Imbalance
            last_run_imb = datetime.fromtimestamp(imbalance_cache['last_updated_ts'], tz=timezone(timedelta(hours=3)))
            if now >= target_time and last_run_imb.date() < now.date() and imbalance_cache['status'] != 'processing':
                print(f"[{now}] Scheduler triggering daily Imbalance analysis...")
                threading.Thread(target=load_and_analyze_imbalance, args=(True,), daemon=True).start()
                
        except Exception as e:
            print(f"Error in scheduler loop: {e}")
            
        time.sleep(60)

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
    
    try:
        # Keep track of old tickers to mark "NEW"
        old_tickers = {r['ticker'] for r in prefs_cache['results']}
        
        with open('tickers.txt', 'r') as f:
            content = f.read()
        
        tickers = [t.strip() for t in content.replace('\n', ',').split(',') if t.strip()]
        unique_tickers = list(set(tickers))
        prefs_cache['total'] = len(unique_tickers)
        
        # Run logic
        new_results = fetch_and_process(unique_tickers, progress_callback=lambda c, t: prefs_cache.update({'progress': c}))
        
        # Mark "NEW" tickers
        for res in new_results:
            res['is_new'] = res['ticker'] not in old_tickers
        
        prefs_cache.update({
            'results': new_results,
            'status': 'completed',
            'last_updated': get_tr_time().strftime("%Y-%m-%d %H:%M:%S TR"),
            'last_updated_ts': now_ts
        })
        
        save_history('prefs')
        print(f"Prefs Analysis Completed. Found {len(new_results)} matches.")
        
    except Exception as e:
        print(f"Error in background prefs analysis: {e}")
        prefs_cache['status'] = 'error'

def load_and_analyze_imbalance(force=False):
    global imbalance_cache
    now_ts = time.time()
    if not force and (now_ts - imbalance_cache['last_updated_ts'] < 86400) and imbalance_cache['results']:
        print("Imbalance Analysis skipped: Recent results exist (less than 24h old).")
        return

    imbalance_cache['status'] = 'processing'
    imbalance_cache['progress'] = 0
    try:
        old_tickers = {r['ticker'] for r in imbalance_cache['results']}
        with open('tickers.txt', 'r') as f:
            content = f.read()
        tickers = [t.strip() for t in content.replace('\n', ',').split(',') if t.strip()]
        unique_tickers = list(set(tickers))
        imbalance_cache['total'] = len(unique_tickers)
        
        new_results = fetch_imbalance(unique_tickers, progress_callback=lambda c, t: imbalance_cache.update({'progress': c}))
        for res in new_results:
            res['is_new'] = res['ticker'] not in old_tickers
            
        imbalance_cache.update({
            'results': new_results,
            'status': 'completed',
            'last_updated': get_tr_time().strftime("%Y-%m-%d %H:%M:%S TR"),
            'last_updated_ts': now_ts
        })
        save_history('imbalance')
        print(f"Imbalance Analysis Completed. Found {len(new_results)} matches.")
    except Exception as e:
        print(f"Error in imbalance analysis: {e}")
        imbalance_cache['status'] = 'error'

# Startup
load_history()
if not prefs_cache['results']:
    threading.Thread(target=load_and_analyze_prefs, args=(True,), daemon=True).start()
if not imbalance_cache['results']:
    threading.Thread(target=load_and_analyze_imbalance, args=(True,), daemon=True).start()

threading.Thread(target=scheduler_loop, daemon=True).start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/prefs', methods=['GET'])
def get_prefs():
    return jsonify(prefs_cache)

@app.route('/imbalance', methods=['GET'])
def get_imbalance():
    return jsonify(imbalance_cache)

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
    
    threading.Thread(target=load_and_analyze_imbalance, args=(True,), daemon=True).start()
    return jsonify({'status': 'started'})

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

if __name__ == '__main__':
    app.run(debug=True, port=5000)
