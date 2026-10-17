import os, time, threading, collections, datetime, re
import requests
from flask import Flask, jsonify, render_template, request

API_KEY = os.getenv('FOURSIM_API_KEY', '6155ce0f0d95e090ddceaba215eb7d30')
DASHBOARD_TOKEN = os.getenv('DASHBOARD_TOKEN', '')
CHECK_INTERVAL = 5
CANCEL_THRESHOLD = 765
MAX_RETRIES = 5
app = Flask(__name__)
lock = threading.RLock()
stop_event = threading.Event()
worker = None
session = requests.Session()
state = {'running': False, 'activations': [], 'cancelled': set(), 'failed': set(), 'in_progress': set(), 'logs': collections.deque(maxlen=250), 'last_sync': None, 'error': None}

def log(level, message):
    message = str(message).replace(API_KEY, '[REDACTED]') if API_KEY else str(message)
    message = re.sub(r'(?i)(apikey=)[^&\s]+', r'\1[REDACTED]', message)
    with lock:
        state['logs'].appendleft({'time': datetime.datetime.now().strftime('%H:%M:%S'), 'level': level, 'message': str(message)})

def seconds(value):
    try:
        if str(value).strip().lower() in ('expired', '00:00', '0:00', ''):
            return 0
        m, s = str(value).split(':')
        return int(m) * 60 + int(s)
    except (ValueError, TypeError):
        return None

def api_get(path, params):
    response = session.get('https://api.4sim.st/' + path, params={'apikey': API_KEY, **params}, timeout=15)
    response.raise_for_status()
    return response.json()

def cancellation_accepted(result):
    if not isinstance(result, dict):
        return False
    status = str(result.get('status', '')).strip().lower()
    message = str(result.get('message', '')).strip().lower()
    return (result.get('success') is True or status in ('200', 'success', 'cancelled', 'canceled')
            and (status != '200' or ('cancel' in message and ('success' in message or 'already' in message))))

def cancel(txn):
    for attempt in range(1, MAX_RETRIES + 1):
        if stop_event.is_set():
            return
        try:
            result = api_get('cancelNumber', {'id': txn})
            if cancellation_accepted(result):
                with lock:
                    state['cancelled'].add(str(txn))
                    state['failed'].discard(str(txn))
                log('success', f'Cancellation confirmed: {txn} (attempt {attempt}/{MAX_RETRIES})')
                return
            log('warning', f'Unconfirmed cancel response for {txn} (attempt {attempt}/{MAX_RETRIES}): {str(result)[:150]}')
            # An explicit provider rejection should not be repeated automatically.
            if isinstance(result, dict) and str(result.get('status', '')) in ('400', '401', '403', '404'):
                break
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else None
            log('error', f'Cancel {txn}, attempt {attempt}/{MAX_RETRIES}: HTTP {code}')
            if code is not None and 400 <= code < 500 and code not in (408, 429):
                break
        except requests.RequestException as exc:
            log('error', f'Cancel {txn}, attempt {attempt}/{MAX_RETRIES}: {type(exc).__name__}')
        except Exception as exc:
            log('error', f'Cancel {txn}, attempt {attempt}/{MAX_RETRIES}: {type(exc).__name__}')
            break
        if attempt < MAX_RETRIES and stop_event.wait(2):
            return
    with lock:
        state['failed'].add(str(txn))
    log('error', f'Cancellation failed/unconfirmed: {txn}')

def loop():
    log('info', 'Auto cancel worker started')
    try:
        while not stop_event.is_set():
            try:
                data = api_get('getActivations', {})
                if not isinstance(data, dict) or not isinstance(data.get('data'), list):
                    raise ValueError('Unexpected activation API response')
                activations = data['data']
                with lock:
                    state['activations'] = activations
                    state['last_sync'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    state['error'] = None
                for act in activations:
                    if stop_event.is_set():
                        break
                    txn = str(act.get('id') or '')
                    remaining = seconds(act.get('expire'))
                    if not txn or remaining is None or remaining > CANCEL_THRESHOLD:
                        continue
                    with lock:
                        if txn in state['cancelled'] or txn in state['in_progress'] or txn in state['failed']:
                            continue
                        state['in_progress'].add(txn)
                    try:
                        cancel(txn)
                    finally:
                        with lock:
                            state['in_progress'].discard(txn)
            except Exception as exc:
                with lock:
                    state['error'] = str(exc)
                log('error', f'Activation fetch: {exc}')
            stop_event.wait(CHECK_INTERVAL)
    finally:
        with lock:
            state['running'] = False
        log('info', 'Auto cancel worker stopped')

@app.before_request
def auth():
    if not DASHBOARD_TOKEN:
        return None
    from hmac import compare_digest
    token = request.headers.get('X-Dashboard-Token', '') or request.args.get('token', '')
    if not compare_digest(token, DASHBOARD_TOKEN):
        return jsonify(error='Unauthorized. Provide X-Dashboard-Token header or ?token=...'), 401

@app.get('/')
def home():
    return render_template('index.html')

@app.get('/api/status')
def status():
    with lock:
        rows = []
        for act in state['activations']:
            item = dict(act)
            txn = str(item.get('id') or '')
            remaining = seconds(item.get('expire'))
            item['status'] = ('Cancelled' if txn in state['cancelled'] else 'Failed' if txn in state['failed'] else 'Cancelling' if txn in state['in_progress'] else 'Due' if remaining is not None and remaining <= CANCEL_THRESHOLD else 'Active')
            rows.append(item)
        return jsonify(running=state['running'], rows=rows, cancelled=len(state['cancelled']), failed=len(state['failed']), logs=list(state['logs'])[:70], last_sync=state['last_sync'], error=state['error'], threshold='12:45', interval=CHECK_INTERVAL)

@app.post('/api/start')
def start():
    global worker
    if not API_KEY:
        return jsonify(error='Set FOURSIM_API_KEY environment variable'), 400
    with lock:
        if state['running']:
            return jsonify(ok=True, running=True)
        stop_event.clear()
        state['running'] = True
        worker = threading.Thread(target=loop, daemon=True)
        worker.start()
    return jsonify(ok=True, running=True)

@app.post('/api/stop')
def stop():
    stop_event.set()
    return jsonify(ok=True, message='Stop requested; active network request may take up to 15 seconds')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5050, debug=False, threaded=True)
