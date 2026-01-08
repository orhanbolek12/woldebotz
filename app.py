from flask import Flask, render_template, request, jsonify
from logic import fetch_and_process
import threading
import uuid
import time

import json
import os

app = Flask(__name__)

# Persistence file
HISTORY_FILE = 'results_history.json'

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

def load_history():
    global prefs_cache
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r') as f:
                data = json.load(f)
                prefs_cache['results'] = data.get('results', [])
                prefs_cache['last_updated'] = data.get('last_updated')
                prefs_cache['last_updated_ts'] = data.get('last_updated_ts', 0)
                prefs_cache['status'] = 'completed'
                print(f"Loaded {len(prefs_cache['results'])} results from history.")
        except Exception as e:
            print(f"Error loading history: {e}")

def save_history():
    try:
        data = {
            'results': prefs_cache['results'],
            'last_updated': prefs_cache['last_updated'],
            'last_updated_ts': prefs_cache['last_updated_ts']
        }
        with open(HISTORY_FILE, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        print(f"Error saving history: {e}")

def load_and_analyze_prefs(force=False):
    """Background task to analyze the big list from tickers.txt"""
    global prefs_cache
    
    # Check scheduling: only run if force=True or > 24 hours
    now_ts = time.time()
    if not force and (now_ts - prefs_cache['last_updated_ts'] < 86400) and prefs_cache['results']:
        print("Analysis skipped: Recent results exist (less than 24h old).")
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
        
        def update_progress(current, total):
            prefs_cache['progress'] = current

        # Run logic
        new_results = fetch_and_process(unique_tickers, progress_callback=update_progress)
        
        # Mark "NEW" tickers
        for res in new_results:
            if res['ticker'] not in old_tickers:
                res['is_new'] = True
            else:
                res['is_new'] = False
        
        prefs_cache['results'] = new_results
        prefs_cache['status'] = 'completed'
        prefs_cache['last_updated'] = time.ctime()
        prefs_cache['last_updated_ts'] = now_ts
        
        save_history()
        print(f"Prefs Analysis Completed. Found {len(new_results)} matches.")
        
    except Exception as e:
        print(f"Error in background prefs analysis: {e}")
        prefs_cache['status'] = 'error'

# Startup
load_history()
threading.Thread(target=load_and_analyze_prefs, daemon=True).start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/prefs', methods=['GET'])
def get_prefs():
    return jsonify(prefs_cache)

@app.route('/refresh_prefs', methods=['POST'])
def refresh_prefs():
    if prefs_cache['status'] == 'processing':
        return jsonify({'status': 'processing', 'message': 'Already strictly running'})
    
    threading.Thread(target=load_and_analyze_prefs, args=(True,), daemon=True).start()
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
