import os, time, threading, collections, datetime, re
import requests
from flask import Flask, jsonify, render_template, request

API_KEYS = [os.getenv(f'FOURSIM_API_KEY_{i}', os.getenv('FOURSIM_API_KEY', '') if i == 1 else '') for i in range(1, 5)]
DASHBOARD_TOKEN = os.getenv('DASHBOARD_TOKEN', '')
CHECK_INTERVAL = 5
CANCEL_THRESHOLD = 765
MAX_RETRIES = 5
app = Flask(__name__)
lock = threading.RLock()
slot_events = [threading.Event() for _ in range(4)]
slot_workers = [None] * 4
slot_running = [False] * 4
session = requests.Session()
state = {'running': False, 'activations': [], 'cancelled': set(), 'failed': set(), 'in_progress': set(), 'logs': collections.deque(maxlen=250), 'last_sync': None, 'error': None, 'api_errors': [''] * 4}

def log(level, message):
    message = str(message)
    for key in API_KEYS:
        if key:
            message = message.replace(key, '[REDACTED]')
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

def api_get(path, params, api_key):
    response = session.get('https://api.4sim.st/' + path, params={'apikey': api_key, **params}, timeout=15)
    response.raise_for_status()
    return response.json()

def cancellation_accepted(result):
    if not isinstance(result, dict):
        return False
    status = str(result.get('status', '')).strip().lower()
    message = str(result.get('message', '')).strip().lower()
    return (result.get('success') is True or status in ('200', 'success', 'cancelled', 'canceled')
            and (status != '200' or ('cancel' in message and ('success' in message or 'already' in message))))

def cancel(txn, slot, api_key, stop_event):
    for attempt in range(1, MAX_RETRIES + 1):
        if stop_event.is_set():
            return
        try:
            result = api_get('cancelNumber', {'id': txn}, api_key)
            if cancellation_accepted(result):
                with lock:
                    state['cancelled'].add((slot, str(txn)))
                    state['failed'].discard((slot, str(txn)))
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
        state['failed'].add((slot, str(txn)))
    log('error', f'Cancellation failed/unconfirmed: {txn}')

def loop(slot):
    event = slot_events[slot - 1]
    log('info', f'API {slot} worker started')
    try:
        while not event.is_set():
            api_key = API_KEYS[slot - 1]
            try:
                data = api_get('getActivations', {}, api_key)
                if not isinstance(data, dict) or not isinstance(data.get('data'), list):
                    raise ValueError('Unexpected activation API response')
                current = []
                for act in data['data']:
                    if event.is_set():
                        break
                    if not isinstance(act, dict):
                        continue
                    item = dict(act)
                    item['api_slot'] = slot
                    current.append(item)
                    txn = str(item.get('id') or '')
                    remaining = seconds(item.get('expire'))
                    key = (slot, txn)
                    if not txn or remaining is None or remaining > CANCEL_THRESHOLD:
                        continue
                    # Never cancel activations that already received an SMS.
                    if item.get('sms') or item.get('smsCode') or item.get('code'):
                        continue
                    with lock:
                        if key in state['cancelled'] or key in state['in_progress'] or key in state['failed']:
                            continue
                        state['in_progress'].add(key)
                    try:
                        cancel(txn, slot, api_key, event)
                    finally:
                        with lock:
                            state['in_progress'].discard(key)
                with lock:
                    state['activations'] = [r for r in state['activations'] if r.get('api_slot') != slot] + current
                    state['api_errors'][slot - 1] = ''
                    state['last_sync'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            except Exception as exc:
                with lock:
                    state['api_errors'][slot - 1] = f'{type(exc).__name__}: {str(exc)[:100]}'
                log('error', f'API {slot} activation fetch: {type(exc).__name__}')
            event.wait(CHECK_INTERVAL)
    finally:
        with lock:
            slot_running[slot - 1] = False
        log('info', f'API {slot} worker stopped')

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
            key = (item.get('api_slot'), txn)
            remaining = seconds(item.get('expire'))
            item['status'] = ('Cancelled' if key in state['cancelled'] else 'Failed' if key in state['failed'] else 'Cancelling' if key in state['in_progress'] else 'Due' if remaining is not None and remaining <= CANCEL_THRESHOLD else 'Active')
            rows.append(item)
        return jsonify(running=any(slot_running), api_running=list(slot_running), rows=rows, cancelled=len(state['cancelled']), failed=len(state['failed']), logs=list(state['logs'])[:70], last_sync=state['last_sync'], error=state['error'], threshold='12:45', interval=CHECK_INTERVAL, api_configured=[bool(k) for k in API_KEYS], api_errors=state['api_errors'])

@app.post('/api/keys/<int:slot>')
def save_key(slot):
    if not 1 <= slot <= 4:
        return jsonify(error='Invalid API slot'), 404
    data = request.get_json(silent=True) or {}
    key = data.get('key')
    if not isinstance(key, str) or len(key) > 300:
        return jsonify(error='Invalid API key'), 400
    with lock:
        if slot_running[slot - 1]:
            return jsonify(error='Stop this API before changing its key'), 409
        API_KEYS[slot - 1] = key.strip()
        state['activations'] = [r for r in state['activations'] if r.get('api_slot') != slot]
        for name in ('cancelled', 'failed', 'in_progress'):
            state[name] = {x for x in state[name] if x[0] != slot}
        state['api_errors'][slot - 1] = ''
    log('info', f'API {slot} key updated (value hidden)')
    return jsonify(ok=True, message=f'API {slot} saved for this server session')

@app.post('/api/keys/<int:slot>/start')
def start_slot(slot):
    if not 1 <= slot <= 4:
        return jsonify(error='Invalid API slot'), 404
    with lock:
        if slot_running[slot - 1]:
            return jsonify(ok=True, running=True)
        if slot_workers[slot - 1] and slot_workers[slot - 1].is_alive():
            return jsonify(error='Previous worker is still stopping; retry shortly'), 409
        if not API_KEYS[slot - 1]:
            return jsonify(error='Save an API key first'), 400
        slot_events[slot - 1] = threading.Event()
        slot_running[slot - 1] = True
        slot_workers[slot - 1] = threading.Thread(target=loop, args=(slot,), daemon=True)
        slot_workers[slot - 1].start()
    return jsonify(ok=True, running=True)

@app.post('/api/keys/<int:slot>/stop')
def stop_slot(slot):
    if not 1 <= slot <= 4:
        return jsonify(error='Invalid API slot'), 404
    slot_events[slot - 1].set()
    return jsonify(ok=True, message=f'API {slot} stop requested; in-flight requests may take up to 15 seconds')

@app.post('/api/start')
def start():
    for slot in range(1, 5):
        if API_KEYS[slot - 1] and not slot_running[slot - 1]:
            start_slot(slot)
    return jsonify(ok=True, running=any(slot_running))

@app.post('/api/stop')
def stop():
    for event in slot_events:
        event.set()
    return jsonify(ok=True, message='Stop requested for all APIs')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=False, threaded=True)
