import json
import time
import logging
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
import websocket # The 'websocket-client' package provides the 'websocket' module
import ta
import threading
from collections import deque
import os # Added for file path operations
import requests
import hashlib
import hmac
import base64
import _thread

# Global variables for OKX API configuration
server_time_offset = 0
okx_simulated_trading_header = {}
okx_api_key = ""
okx_api_secret = ""
okx_passphrase = ""
okx_rest_api_base_url = "https://www.okx.com"

# Placeholder for PRODUCT_INFO, will be populated by fetch_product_info
PRODUCT_INFO = {
    "pricePrecision": None,
    "qtyPrecision": None,
    "priceTickSize": None,
    "minOrderQty": None,
    "contractSize": None,
}

def safe_float(value, default=0.0):
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def get_okx_server_time_and_offset(log_callback):
    global server_time_offset
    try:
        response = requests.get(f"{okx_rest_api_base_url}/api/v5/public/time", timeout=5)
        response.raise_for_status()
        json_response = response.json()
        if json_response.get('code') == '0' and json_response.get('data'):
            server_timestamp_ms = int(json_response['data'][0]['ts'])
            local_timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            server_time_offset = server_timestamp_ms - local_timestamp_ms
            log_callback(f"OKX server time synchronized. Offset: {server_time_offset}ms", level="info")
            return True
        else:
            log_callback(f"Failed to get OKX server time: {json_response.get('msg', 'Unknown error')}", level="error")
            return False
    except requests.exceptions.RequestException as e:
        log_callback(f"Error fetching OKX server time: {e}", level="error")
        return False
    except Exception as e:
        log_callback(f"Unexpected error in get_okx_server_time_and_offset: {e}", level="error")
        return False

def generate_okx_signature(timestamp, method, request_path, body_str=''):
    """
    Generate HMAC SHA256 signature for OKX API.
    Returns Base64-encoded HMAC-SHA256 digest.
    """
    message = str(timestamp) + method.upper() + request_path + body_str
    hashed = hmac.new(okx_api_secret.encode('utf-8'), message.encode('utf-8'), hashlib.sha256)
    signature = base64.b64encode(hashed.digest()).decode('utf-8')
    return signature

def okx_request(method, path, params=None, body_dict=None, max_retries=3, log_callback=None):
    local_dt = datetime.now(timezone.utc)
    adjusted_dt = local_dt + timedelta(milliseconds=server_time_offset)
    timestamp = adjusted_dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    body_str = ''
    if body_dict:
        body_str = json.dumps(body_dict, separators=(',', ':'), sort_keys=True)

    request_path_for_signing = path
    final_url = f"{okx_rest_api_base_url}{path}" 

    if params and method.upper() == 'GET':
        query_string = '?' + '&'.join([f'{k}={v}' for k, v in sorted(params.items())])
        request_path_for_signing += query_string
        final_url += query_string

    signature = generate_okx_signature(timestamp, method, request_path_for_signing, body_str)

    headers = {
        "OK-ACCESS-KEY": okx_api_key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": okx_passphrase,
        "Content-Type": "application/json"
    }

    headers.update(okx_simulated_trading_header)

    for attempt in range(max_retries):
        try:
            req_func = getattr(requests, method.lower(), None)
            if not req_func:
                if log_callback: log_callback(f"Unsupported HTTP method: {method}", level="error")
                return None

            kwargs = {'headers': headers, 'timeout': 15}

            if body_dict and method.upper() in ['POST', 'PUT', 'DELETE']:
                kwargs['data'] = body_str

            if log_callback: log_callback(f"{method} {path} (Attempt {attempt + 1}/{max_retries})", level="info")
            response = req_func(final_url, **kwargs)

            if response.status_code != 200:
                try:
                    error_json = response.json()
                    if log_callback: log_callback(f"API Error: Status={response.status_code}, Code={error_json.get('code')}, Msg={error_json.get('msg')}", level="error")
                    okx_error_code = error_json.get('code')
                    if okx_error_code:
                        return error_json
                except json.JSONDecodeError:
                    if log_callback: log_callback(f"API Error: Status={response.status_code}, Response: {response.text[:200]}", level="error")

                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None

            try:
                json_response = response.json()
                if json_response.get('code') != '0':
                    if log_callback: log_callback(f"OKX API returned non-zero code: {json_response.get('code')} Msg: {json_response.get('msg')} for {method} {path}. Full Response: {json_response}", level="warning")
                return json_response
            except json.JSONDecodeError:
                if log_callback: log_callback(f"Failed to decode JSON for {method} {path}. Status: {response.status_code}, Resp: {response.text[:200]}", level="error")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None

        except requests.exceptions.Timeout:
            if log_callback: log_callback(f"API request timeout (Attempt {attempt + 1}/{max_retries})", level="error")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            return None
        except requests.exceptions.RequestException as e:
            status_code = e.response.status_code if e.response is not None else "N/A"
            err_text = e.response.text[:200] if e.response is not None else 'No response text'
            if log_callback: log_callback(f"OKX API HTTP Error ({method} {path}): Status={status_code}, Error={e}. Response: {err_text}", level="error")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            return None
        except Exception as e:
            if log_callback: log_callback(f"Unexpected error during OKX API request ({method} {path}): {e}", level="error")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            return None
    return None

def fetch_historical_data_okx(symbol, timeframe, start_ts_ms, end_ts_ms, log_callback):
    try:
        path = "/api/v5/market/history-candles"

        okx_timeframe_map = {
            '1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m',
            '1h': '1H', '2h': '2H', '4h': '4H', '6h': '6H', '8h': '8H',
            '12h': '12H', '1d': '1D', '1w': '1W', '1M': '1M'
        }
        okx_timeframe = okx_timeframe_map.get(timeframe)

        if not okx_timeframe:
            log_callback(f"Invalid timeframe for OKX: {timeframe}", level="error")
            return []

        all_data = []
        max_candles_limit = 100

        current_before_ms = end_ts_ms

        log_callback(f"Fetching historical data for {symbol} ({timeframe}) from {datetime.fromtimestamp(start_ts_ms/1000, tz=timezone.utc)} to {datetime.fromtimestamp(end_ts_ms/1000, tz=timezone.utc)}", level="info")

        while True:
            params = {
                "instId": symbol,
                "bar": okx_timeframe,
                "limit": str(max_candles_limit),
                "before": str(current_before_ms)
            }

            response = okx_request("GET", path, params=params, log_callback=log_callback)
            
            if response and response.get('code') == '0':
                rows = response.get('data', [])
                if rows:
                    log_callback(f"Fetched {len(rows)} candles for {timeframe}", level="info")
                    parsed_klines = []
                    for kline in rows:
                        try:
                            parsed_klines.append([
                                int(kline[0]),
                                float(kline[1]),
                                float(kline[2]),
                                float(kline[3]),
                                float(kline[4]),
                                float(kline[5])
                            ])
                        except (ValueError, TypeError, IndexError) as e:
                            log_callback(f"Error parsing OKX kline: {kline} - {e}", level="error")
                            continue
                    
                    all_data.extend(parsed_klines)
                    
                    oldest_ts = int(rows[-1][0])
                    current_before_ms = oldest_ts

                    if oldest_ts <= start_ts_ms or len(rows) < max_candles_limit:
                        break 
                else:
                    break 

                time.sleep(0.3)
            else:
                log_callback(f"Error fetching OKX klines: {response}", level="error")
                return []
        
        final_data = pd.DataFrame(all_data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        if not final_data.empty:
            final_data = final_data.drop_duplicates(subset=['Timestamp'])
            final_data = final_data[final_data['Timestamp'] >= start_ts_ms]
            final_data = final_data.sort_values(by='Timestamp', ascending=True)
            return final_data.values.tolist()
        else:
            return []
    except Exception as e:
        log_callback(f"Exception in fetch_historical_data_okx: {e}", level="error")
        return []

def fetch_product_info(target_symbol, log_callback):
    global PRODUCT_INFO
    try:
        path = "/api/v5/public/instruments"
        params = {"instType": "SWAP", "instId": target_symbol}
        response = okx_request("GET", path, params=params, log_callback=log_callback)

        if response and response.get('code') == '0':
            product_data = None
            if isinstance(response.get('data'), list):
                for item in response['data']:
                    if item.get('instId') == target_symbol:
                        product_data = item
                        break
            elif isinstance(response.get('data'), dict) and response.get('data').get('instId') == target_symbol:
                product_data = response.get('data')

            if not product_data:
                log_callback(f"Product {target_symbol} not found in OKX instruments response.", level="error")
                return False

            PRODUCT_INFO['priceTickSize'] = safe_float(product_data.get('tickSz'))
            PRODUCT_INFO['qtyPrecision'] = int(np.abs(np.log10(safe_float(product_data.get('lotSz'))))) if safe_float(product_data.get('lotSz')) > 0 else 0
            PRODUCT_INFO['pricePrecision'] = int(np.abs(np.log10(safe_float(product_data.get('tickSz'))))) if safe_float(product_data.get('tickSz')) > 0 else 0
            PRODUCT_INFO['qtyStepSize'] = safe_float(product_data.get('lotSz'))
            PRODUCT_INFO['minOrderQty'] = safe_float(product_data.get('minSz'))

            PRODUCT_INFO['contractSize'] = safe_float(product_data.get('ctVal', '1'), 1.0)

            log_callback(f"Product info loaded for {target_symbol}: {PRODUCT_INFO}", level="info")
            return True
        else:
            log_callback(f"Failed to fetch product info for {target_symbol} (code: {response.get('code') if response else 'N/A'}, msg: {response.get('msg') if response else 'N/A'})", level="error")
            return False
    except Exception as e:
        log_callback(f"Exception in fetch_product_info: {e}", level="error")
        return False

def okx_set_leverage(symbol, leverage_val, log_callback):
    try:
        path = "/api/v5/account/set-leverage"
        body = {
            "instId": symbol,
            "lever": str(int(leverage_val)),
            "mgnMode": "cross"
        }

        log_callback(f"Setting leverage to {leverage_val}x for {symbol}", level="info")
        response = okx_request("POST", path, body_dict=body, log_callback=log_callback)

        if response and response.get('code') == '0':
            log_callback(f"Leverage set successfully for {symbol}", level="info")
            return True
        else:
            log_callback(f"Failed to set leverage for {symbol}: {response.get('msg') if response else 'No response'}", level="error")
            return False
    except Exception as e:
        log_callback(f"Exception in okx_set_leverage: {e}", level="error")
        return False

def get_current_market_price(symbol, log_callback):
    try:
        path = "/api/v5/market/ticker"
        params = {"instId": symbol}
        response = okx_request("GET", path, params=params, log_callback=log_callback)

        if response and response.get('code') == '0':
            data = response.get('data', [])
            if data and isinstance(data, list) and len(data) > 0:
                ticker_info = data[0]
                last_price = ticker_info.get('last')
                if last_price is not None:
                    current_price = safe_float(last_price)
                    if log_callback: log_callback(f"Current market price (REST): ${current_price:.2f}", level="info")
                    return current_price
                else:
                    if log_callback: log_callback("'last' price not found in OKX ticker response.", level="error")
            else:
                if log_callback: log_callback("OKX ticker data is empty or malformed.", level="error")

        if log_callback: log_callback("Failed to fetch current price from REST.", level="error")
        return None
    except Exception as e:
        if log_callback: log_callback(f"Exception in get_current_market_price: {e}", level="error")
        return None

class TradingBotEngine:
    def __init__(self, config_path, emit_callback):
        self.config_path = config_path
        self.emit = emit_callback
        
        self.console_logs = deque(maxlen=500)
        self.config = self._load_config()

        # Initialize OKX API credentials globally
        global okx_api_key, okx_api_secret, okx_passphrase, okx_simulated_trading_header
        okx_api_key = self.config['okx_api_key']
        okx_api_secret = self.config['okx_api_secret']
        okx_passphrase = self.config['okx_passphrase']
        if self.config['use_testnet']:
            okx_simulated_trading_header = {'x-simulated-trading': '1'}
        else:
            okx_simulated_trading_header = {}

        self.ws = None
        self.ws_thread = None
        self.is_running = False
        self.stop_event = threading.Event()
        
        self.current_balance = 0.0
        self.open_trades = []
        self.is_bot_initialized = threading.Event()
        self.last_stake_amount = 0.0
        self.last_trade_was_loss = False
        
        # OKX specific variables (from example bot)
        self.historical_data_store = {}
        self.data_lock = threading.Lock()
        self.trade_data_lock = threading.Lock()
        self.latest_trade_price = None
        self.latest_trade_timestamp = None
        self.account_balance = 0.0
        self.available_balance = 0.0
        self.account_info_lock = threading.Lock()
        self.in_position = False
        self.position_entry_price = 0.0
        self.position_qty = 0.0
        self.current_stop_loss = 0.0
        self.current_take_profit = 0.0
        self.position_lock = threading.Lock()
        self.pending_entry_order_id = None
        self.pending_entry_order_details = {}
        self.position_exit_orders = {}
        self.entry_reduced_tp_flag = False
        self.entry_sl_price = 0.0
        self.sl_hit_triggered = False
        self.sl_hit_lock = threading.Lock()
        self.entry_order_with_sl = None
        self.entry_order_sl_lock = threading.Lock()
        self.tp_hit_triggered = False
        self.tp_hit_lock = threading.Lock()
        self.bot_startup_complete = False

        self.ws_subscriptions_ready = threading.Event()
        self.pending_subscriptions = set()
        self.confirmed_subscriptions = set()

        self.intervals = {
            '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
            '1h': 3600, '2h': 7200, '4h': 14400, '6h': 21600, '8h': 28800,
            '12h': 43200, '1d': 86400, '1w': 604800, '1M': 2592000
        }
        self.interval_to_timeframe_str = {v: k for k, v in self.intervals.items()}
        
    def log(self, message, level='info', to_file=False, filename=None):
        timestamp = datetime.now().strftime('%H:%M:%S')
        log_entry = {'timestamp': timestamp, 'message': message, 'level': level}
        # Always append to console_logs for internal history, but filter what gets emitted to frontend
        self.console_logs.append(log_entry)
        
        # Only emit info, warning, and error levels to the frontend
        if level in ['info', 'warning', 'error']:
            self.emit('console_log', log_entry)
        
        # Always write to the local log file based on level
        if level == 'info':
            logging.info(message)
        elif level == 'warning':
            logging.warning(message)
        elif level == 'error':
            logging.error(message)
        elif level == 'debug':
            logging.debug(message)
    
    def start(self):
        if self.is_running:
            self.log('Bot is already running', 'warning')
            return
        
        self.is_running = True
        self.log('Bot starting...', 'info')
        
        # New initialization sequence for OKX
        if not get_okx_server_time_and_offset(self.log):
            self.log("Failed to synchronize server time. Please check network connection or API.", 'error')
            self.is_running = False
            self.emit('bot_status', {'running': False})
            return
        
        if not fetch_product_info(self.config['symbol'], self.log):
            self.log("Failed to fetch product info. Exiting.", 'error')
            self.is_running = False
            self.emit('bot_status', {'running': False})
            return
 
        if not okx_set_leverage(self.config['symbol'], self.config['leverage'], self.log):
            self.log("Failed to set leverage. Exiting.", 'error')
            self.is_running = False
            self.emit('bot_status', {'running': False})
            return
        
        self.log("Checking for and closing any existing open positions...", level="info")
        self._check_and_close_any_open_position()

        self.log('Bot initialized. Starting live trading connection...', 'info')
        self.ws_thread = threading.Thread(target=self._initialize_websocket_and_start_main_loop, daemon=True)
        self.ws_thread.start()
    
    def stop(self):
        if not self.is_running:
            self.log('Bot is not running', 'warning')
            return
        
        self.is_running = False
        self.log('Bot stopping...', 'info')
        
        self.stop_event.set() # Signal all threads to stop
        if self.ws:
            self.ws.close()
        
        self.emit('bot_status', {'running': False})
    
    def _load_config(self):
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
                # Ensure new config parameters have default values if not present
                config.setdefault('max_allowed_used', 1000.0)
                config.setdefault('cancel_on_tp_price_below_market', True)
                config.setdefault('cancel_on_entry_price_below_market', True)
                config.setdefault('websocket_timeframes', ['1m', '5m']) # Add default for websocket_timeframes
                return config
        except FileNotFoundError:
            self.log(f"Config file not found: {self.config_path}", 'error')
            raise
        except json.JSONDecodeError as e:
            self.log(f"Error decoding config file {self.config_path}: {e}", 'error')
            raise
        except Exception as e:
            self.log(f"An unexpected error occurred while loading config: {e}", 'error')
            raise

    # ================================================================================
    # OKX API Helper Functions (Adapted as methods)
    # ================================================================================

    def _okx_request(self, method, path, params=None, body_dict=None, max_retries=3):
        local_dt = datetime.now(timezone.utc)
        adjusted_dt = local_dt + timedelta(milliseconds=server_time_offset)
        timestamp = adjusted_dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        body_str = ''
        if body_dict:
            body_str = json.dumps(body_dict, separators=(',', ':'), sort_keys=True)

        request_path_for_signing = path
        final_url = f"{okx_rest_api_base_url}{path}" 

        if params and method.upper() == 'GET':
            query_string = '?' + '&'.join([f'{k}={v}' for k, v in sorted(params.items())])
            request_path_for_signing += query_string
            final_url += query_string

        signature = generate_okx_signature(timestamp, method, request_path_for_signing, body_str)

        headers = {
            "OK-ACCESS-KEY": okx_api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": okx_passphrase,
            "Content-Type": "application/json"
        }

        headers.update(okx_simulated_trading_header)

        for attempt in range(max_retries):
            try:
                req_func = getattr(requests, method.lower(), None)
                if not req_func:
                    self.log(f"Unsupported HTTP method: {method}", level="error")
                    return None

                kwargs = {'headers': headers, 'timeout': 15}

                if body_dict and method.upper() in ['POST', 'PUT', 'DELETE']:
                    kwargs['data'] = body_str

                self.log(f"{method} {path} (Attempt {attempt + 1}/{max_retries})", level="info")
                response = req_func(final_url, **kwargs)

                if response.status_code != 200:
                    try:
                        error_json = response.json()
                        self.log(f"API Error: Status={response.status_code}, Code={error_json.get('code')}, Msg={error_json.get('msg')}", level="error")
                        okx_error_code = error_json.get('code')
                        if okx_error_code:
                            return error_json
                    except json.JSONDecodeError:
                        self.log(f"API Error: Status={response.status_code}, Response: {response.text[:200]}", level="error")

                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return None

                try:
                    json_response = response.json()
                    if json_response.get('code') != '0':
                        self.log(f"OKX API returned non-zero code: {json_response.get('code')} Msg: {json_response.get('msg')} for {method} {path}. Full Response: {json_response}", level="warning")
                    return json_response
                except json.JSONDecodeError:
                    self.log(f"Failed to decode JSON for {method} {path}. Status: {response.status_code}, Resp: {response.text[:200]}", level="error")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return None

            except requests.exceptions.Timeout:
                self.log(f"API request timeout (Attempt {attempt + 1}/{max_retries})", level="error")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None
            except requests.exceptions.RequestException as e:
                status_code = e.response.status_code if e.response is not None else "N/A"
                err_text = e.response.text[:200] if e.response is not None else 'No response text'
                self.log(f"OKX API HTTP Error ({method} {path}): Status={status_code}, Error={e}. Response: {err_text}", level="error")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None
            except Exception as e:
                self.log(f"Unexpected error during OKX API request ({method} {path}): {e}", level="error")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return None
        return None

    def _fetch_historical_data_okx(self, symbol, timeframe, start_ts_ms, end_ts_ms):
        try:
            path = "/api/v5/market/history-candles"

            okx_timeframe_map = {
                '1m': '1m', '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m',
                '1h': '1H', '2h': '2H', '4h': '4H', '6h': '6H', '8h': '8H',
                '12h': '12H', '1d': '1D', '1w': '1W', '1M': '1M'
            }
            okx_timeframe = okx_timeframe_map.get(timeframe)

            if not okx_timeframe:
                self.log(f"Invalid timeframe for OKX: {timeframe}", level="error")
                return []

            all_data = []
            max_candles_limit = 100

            current_before_ms = end_ts_ms

            self.log(f"Fetching historical data for {symbol} ({timeframe}) from {datetime.fromtimestamp(start_ts_ms/1000, tz=timezone.utc)} to {datetime.fromtimestamp(end_ts_ms/1000, tz=timezone.utc)}", level="info")

            while True:
                params = {
                    "instId": symbol,
                    "bar": okx_timeframe,
                    "limit": str(max_candles_limit),
                    "before": str(current_before_ms)
                }

                response = self._okx_request("GET", path, params=params)
                
                if response and response.get('code') == '0':
                    rows = response.get('data', [])
                    if rows:
                        self.log(f"Fetched {len(rows)} candles for {timeframe}", level="info")
                        parsed_klines = []
                        for kline in rows:
                            try:
                                parsed_klines.append([
                                    int(kline[0]),
                                    float(kline[1]),
                                    float(kline[2]),
                                    float(kline[3]),
                                    float(kline[4]),
                                    float(kline[5])
                                ])
                            except (ValueError, TypeError, IndexError) as e:
                                self.log(f"Error parsing OKX kline: {kline} - {e}", level="error")
                                continue
                        
                        all_data.extend(parsed_klines)
                        
                        oldest_ts = int(rows[-1][0])
                        current_before_ms = oldest_ts

                        if oldest_ts <= start_ts_ms or len(rows) < max_candles_limit:
                            break 
                    else:
                        break 

                    time.sleep(0.3)
                else:
                    self.log(f"Error fetching OKX klines: {response}", level="error")
                    return []
            
            final_data = pd.DataFrame(all_data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            if not final_data.empty:
                final_data = final_data.drop_duplicates(subset=['Timestamp'])
                final_data = final_data[final_data['Timestamp'] >= start_ts_ms]
                final_data = final_data.sort_values(by='Timestamp', ascending=True)
                return final_data.values.tolist()
            else:
                return []
        except Exception as e:
            self.log(f"Exception in _fetch_historical_data_okx: {e}", level="error")
            return []

    def _fetch_product_info(self, target_symbol):
        global PRODUCT_INFO
        try:
            path = "/api/v5/public/instruments"
            params = {"instType": "SWAP", "instId": target_symbol}
            response = self._okx_request("GET", path, params=params)

            if response and response.get('code') == '0':
                product_data = None
                if isinstance(response.get('data'), list):
                    for item in response['data']:
                        if item.get('instId') == target_symbol:
                            product_data = item
                            break
                elif isinstance(response.get('data'), dict) and response.get('data').get('instId') == target_symbol:
                    product_data = response.get('data')

                if not product_data:
                    self.log(f"Product {target_symbol} not found in OKX instruments response.", level="error")
                    return False

                PRODUCT_INFO['priceTickSize'] = safe_float(product_data.get('tickSz'))
                PRODUCT_INFO['qtyPrecision'] = int(np.abs(np.log10(safe_float(product_data.get('lotSz'))))) if safe_float(product_data.get('lotSz')) > 0 else 0
                PRODUCT_INFO['pricePrecision'] = int(np.abs(np.log10(safe_float(product_data.get('tickSz'))))) if safe_float(product_data.get('tickSz')) > 0 else 0
                PRODUCT_INFO['qtyStepSize'] = safe_float(product_data.get('lotSz'))
                PRODUCT_INFO['minOrderQty'] = safe_float(product_data.get('minSz'))

                PRODUCT_INFO['contractSize'] = safe_float(product_data.get('ctVal', '1'), 1.0)

                self.log(f"Product info loaded for {target_symbol}: {PRODUCT_INFO}", level="info")
                return True
            else:
                self.log(f"Failed to fetch product info for {target_symbol} (code: {response.get('code') if response else 'N/A'}, msg: {response.get('msg') if response else 'N/A'})", level="error")
                return False
        except Exception as e:
            self.log(f"Exception in fetch_product_info: {e}", level="error")
            return False

    def _okx_set_leverage(self, symbol, leverage_val):
        try:
            path = "/api/v5/account/set-leverage"
            body = {
                "instId": symbol,
                "lever": str(int(leverage_val)),
                "mgnMode": "cross"
            }

            self.log(f"Setting leverage to {leverage_val}x for {symbol}", level="info")
            response = self._okx_request("POST", path, body_dict=body)

            if response and response.get('code') == '0':
                self.log(f"Leverage set successfully for {symbol}", level="info")
                return True
            else:
                self.log(f"Failed to set leverage for {symbol}: {response.get('msg') if response else 'No response'}", level="error")
                return False
        except Exception as e:
            self.log(f"Exception in okx_set_leverage: {e}", level="error")
            return False

    def _get_current_market_price(self, symbol):
        try:
            path = "/api/v5/market/ticker"
            params = {"instId": symbol}
            response = self._okx_request("GET", path, params=params)

            if response and response.get('code') == '0':
                data = response.get('data', [])
                if data and isinstance(data, list) and len(data) > 0:
                    ticker_info = data[0]
                    last_price = ticker_info.get('last')
                    if last_price is not None:
                        current_price = safe_float(last_price)
                        self.log(f"Current market price (REST): ${current_price:.2f}", level="info")
                        return current_price
                    else:
                        self.log("'last' price not found in OKX ticker response.", level="error")
                else:
                    self.log("OKX ticker data is empty or malformed.", level="error")

            self.log("Failed to fetch current price from REST.", level="error")
            return None
        except Exception as e:
            self.log(f"Exception in get_current_market_price: {e}", level="error")
            return None

    # ================================================================================
    # OKX WebSocket Implementation
    # ================================================================================

    def _get_ws_url(self):
        # Use the public WebSocket endpoint as requested
        return "wss://ws.okx.com:8443/ws/v5/public"

    def _on_websocket_message(self, ws_app, message):
        self.log(f"DEBUG: _on_websocket_message received raw message: {message[:500]}", level="debug") # Log all incoming messages
        try:
            msg = json.loads(message)
            self.log(f"DEBUG: _on_websocket_message received parsed message: {msg}", level="debug")

            # Handle event messages (subscribe)
            if 'event' in msg:
                if msg['event'] == 'subscribe':
                    arg = msg.get('arg', {})
                    channel_id = f"{arg.get('channel')}:{arg.get('instId')}"
                    self.log(f"Subscription confirmed for {channel_id}: {msg}", level="info")
                    self.confirmed_subscriptions.add(channel_id)
                    if self.pending_subscriptions == self.confirmed_subscriptions:
                        self.log("All expected WebSocket subscriptions are ready.", level="info")
                        self.ws_subscriptions_ready.set()
                else: # Log other event messages
                    self.log(f"Received non-subscribe event message: {msg}", level="warning")
                # Do NOT return here, allow further processing if it's a data message that also has an event.
            
            if 'data' in msg:
                channel = msg.get('arg', {}).get('channel', '')
                data = msg.get('data', [])

                if channel == 'trades' and data:
                    with self.trade_data_lock:
                        self.latest_trade_timestamp = int(data[-1].get('ts'))
                        self.latest_trade_price = safe_float(data[-1].get('px'))

                elif channel == 'tickers' and data:
                    # Process ticker data to update latest_trade_price
                    # The `last` field from ticker data represents the current price
                    self.latest_trade_price = safe_float(data[0].get('last'))
                    # No need to update historical data store from tickers channel

        except json.JSONDecodeError:
            self.log(f"DEBUG: Non-JSON WebSocket message received: {message[:500]}", level="debug")
        except Exception as e:
            self.log(f"Exception in on_websocket_message: {e}", level="error")

    def _on_websocket_open(self, ws_app):
        self.log("OKX WebSocket connection opened.", level="info")
        # For public endpoints, authentication is not required, directly send subscriptions
        self._send_websocket_subscriptions()
        # The _send_websocket_subscriptions method will populate self.pending_subscriptions

    def _send_websocket_subscriptions(self):
        channels = [
            {"channel": "trades", "instId": self.config['symbol']},
            {"channel": "tickers", "instId": self.config['symbol']}, # Public tickers channel for real-time price
        ]
        
        # Temporarily removed candle subscriptions until correct format for ETH-USDT-SWAP is confirmed
 
        subscription_payload = {
            "op": "subscribe",
            "args": channels
        }
        self.log(f"WS Sending public subscription request: {json.dumps(subscription_payload)}", level="info")
        self.ws.send(json.dumps(subscription_payload))
        self.log(f"WS Sent public subscription request for {len(channels)} channels.", level="info")
        # Populate pending_subscriptions with the channels we just sent
        self.pending_subscriptions = {f"{arg['channel']}:{arg['instId']}" for arg in channels}

    def _on_websocket_error(self, ws_app, error):
        self.log(f"OKX WebSocket error: {error}", level="error")

    def _on_websocket_close(self, ws_app, close_status_code, close_msg):
        self.log("OKX WebSocket closed.", level="warning")
        if self.is_running and not self.stop_event.is_set():
            self.log('Attempting to reconnect WebSocket...', level="info")
            time.sleep(5)
            self.ws_thread = threading.Thread(target=self._initialize_websocket_and_start_main_loop, daemon=True)
            self.ws_thread.start()
        else:
            self.log('WebSocket will not reconnect as bot is stopped.', level="info")

    def connect(self): # This method will be called from start()
        ws_url = self._get_ws_url()
        try:
            self.ws = websocket.WebSocketApp(
                ws_url,
                on_open=self._on_websocket_open,
                on_message=self._on_websocket_message,
                on_error=self._on_websocket_error,
                on_close=self._on_websocket_close
            )
            self.emit('bot_status', {'running': True})
            self.ws.run_forever()
        except Exception as e:
            self.log(f"Exception initializing WebSocket: {e}", level="error")

    def _update_historical_data_from_ws(self, timeframe_key, klines_ws):
        if not klines_ws:
            return

        with self.data_lock:
            df = self.historical_data_store.get(timeframe_key)
            if df is None:
                return

            new_data_points = []
            for kline in klines_ws:
                try:
                    ts_ms = int(kline[0])
                    o = float(kline[1])
                    h = float(kline[2])
                    l = float(kline[3])
                    c = float(kline[4])
                    v = float(kline[5])

                    if not (l <= h):
                        continue

                    dt_utc = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

                    if not df.empty and dt_utc == df.index[-1]:
                        df.at[dt_utc, 'Open'] = o
                        df.at[dt_utc, 'High'] = h
                        df.at[dt_utc, 'Low'] = l
                        df.at[dt_utc, 'Close'] = c
                        df.at[dt_utc, 'Volume'] = v
                        continue

                    new_data_points.append({
                        'Datetime': dt_utc,
                        'Open': o,
                        'High': h,
                        'Low': l,
                        'Close': c,
                        'Volume': v
                    })

                except (ValueError, TypeError, IndexError):
                    continue

            if not new_data_points:
                return

            temp_df = pd.DataFrame(new_data_points).set_index('Datetime')
            original_last_time = df.index[-1] if not df.empty else None

            combined_df = pd.concat([df, temp_df])
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            combined_df = combined_df.sort_index()

            if len(combined_df) > 1000:
                combined_df = combined_df.iloc[-1000:]

            self.historical_data_store[timeframe_key] = combined_df

            if original_last_time is not None and not combined_df.empty:
                current_last_time = combined_df.index[-1]
                if current_last_time > original_last_time:
                    self.log(f"New {timeframe_key} candle: {current_last_time}", level="info")

    def _fetch_initial_historical_data(self, symbol, timeframe, start_date_str, end_date_str):
        with self.data_lock:
            try:
                start_dt = datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                start_ts_ms = int(start_dt.timestamp() * 1000)
                end_dt = datetime.strptime(end_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                end_ts_ms = int(end_dt.timestamp() * 1000)

                raw_data = self._fetch_historical_data_okx(symbol, timeframe, start_ts_ms, end_ts_ms)

                if raw_data:
                    df = pd.DataFrame(raw_data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
                    df.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'], inplace=True)

                    if df.empty:
                        self.log(f"No valid data for {timeframe}", level="error")
                        return False

                    invalid_rows = df[(df['Low'] > df['High']) |
                                    (df['Open'] < df['Low']) | (df['Open'] > df['High']) |
                                    (df['Close'] < df['Low']) | (df['Close'] > df['High'])]

                    if not invalid_rows.empty:
                        self.log(f"WARNING: Found {len(invalid_rows)} invalid OHLC rows", level="warning")
                        df = df[(df['Low'] <= df['High'])]

                    df['Datetime'] = pd.to_datetime(df['Timestamp'], unit='ms', utc=True)
                    df = df.set_index('Datetime')
                    df = df[~df.index.duplicated(keep='first')]
                    df = df.sort_index()

                    self.historical_data_store[timeframe] = df

                    self.log(f"Loaded {len(df)} candles for {timeframe}", level="info")
                    return True
                else:
                    self.log(f"Failed to fetch data for {timeframe}", level="error")
                    return False
            except Exception as e:
                self.log(f"Exception in _fetch_initial_historical_data: {e}", level="error")
                return False

    def _okx_place_order(self, symbol, side, qty, price=None, order_type="Market",
                        time_in_force=None, reduce_only=False,
                        stop_loss_price=None, take_profit_price=None):
        try:
            path = "/api/v5/trade/order"
            price_precision = PRODUCT_INFO.get('pricePrecision', 4)
            qty_precision = PRODUCT_INFO.get('qtyPrecision', 8)

            order_qty_str = f"{qty:.{qty_precision}f}"

            body = {
                "instId": symbol,
                "tdMode": "cross",
                "side": side.lower(),
                "ordType": order_type.lower(),
                "sz": order_qty_str,
            }

            if order_type.lower() == "limit" and price is not None:
                body["px"] = f"{price:.{price_precision}f}"

            if time_in_force:
                if time_in_force == "GoodTillCancel":
                    body["timeInForce"] = "GTC"
                else:
                    body["timeInForce"] = time_in_force

            if reduce_only:
                body["reduceOnly"] = True

            self.log(f"Placing {order_type} {side} order for {order_qty_str} {symbol} at {price}", level="info")
            response = self._okx_request("POST", path, body_dict=body)

            if response and response.get('code') == '0':
                order_data = response.get('data', [])
                if order_data and order_data[0].get('ordId'):
                    self.log(f"✓ Order placed: OrderID={order_data[0]['ordId']}", level="info")
                    return order_data[0]
                else:
                    self.log(f"✗ Order placement failed: No order ID in response. Response: {response}", level="error")
                    return None
            else:
                error_msg = response.get('msg', 'Unknown error') if response else 'No response'
                self.log(f"✗ Order placement failed: {error_msg}. Response: {response}", level="error")
                return None
        except Exception as e:
            self.log(f"Exception in _okx_place_order: {e}", level="error")
            return None

    def _okx_place_algo_order(self, body):
        try:
            path = "/api/v5/trade/order-algo"
            self.log(f"Placing algo order: {body}", level="info")
            response = self._okx_request("POST", path, body_dict=body)
            if response and response.get('code') == '0':
                data = response.get('data', [])
                if data and (data[0].get('algoId') or data[0].get('ordId')):
                    self.log(f"✓ Algo order placed: {data[0]}", level="info")
                    return data[0]
                else:
                    self.log(f"✗ Algo order placed but no algoId/ordId returned: {response}", level="error")
                    return None
            else:
                self.log(f"✗ Algo order failed: {response}", level="error")
                return None
        except Exception as e:
            self.log(f"Exception in _okx_place_algo_order: {e}", level="error")
            return None

    def _okx_cancel_order(self, symbol, order_id):
        try:
            path = "/api/v5/trade/cancel-order"
            body = {
                "instId": symbol,
                "ordId": order_id,
            }

            self.log(f"Cancelling OKX order {order_id[:12]}...", level="info")
            response = self._okx_request("POST", path, body_dict=body)

            if response and response.get('code') == '0':
                self.log(f"✓ Order cancelled", level="info")
                return True
            elif response and response.get('code') == '51001':
                self.log(f"Order already filled/cancelled (OK)", level="info")
                return True
            else:
                self.log(f"Failed to cancel order (OK, continuing): {response.get('msg') if response else 'No response'}", level="warning")
                return False
        except Exception as e:
            self.log(f"Exception in _okx_cancel_order: {e}", level="error")
            return False

    def _okx_cancel_algo_order(self, symbol, algo_id):
        try:
            path = "/api/v5/trade/cancel-algo-order"
            body = {
                "instId": symbol,
                "algoId": algo_id,
            }

            self.log(f"Cancelling OKX algo order {str(algo_id)[:12]}...", level="info")
            response = self._okx_request("POST", path, body_dict=body)

            if response and response.get('code') == '0':
                self.log(f"✓ Algo order cancelled", level="info")
                return True
            elif response and response.get('code') == '51001':
                self.log(f"Algo order already filled/cancelled (OK)", level="info")
                return True
            else:
                self.log(f"Failed to cancel algo order (OK, continuing): {response.get('msg') if response else 'No response'}", level="warning")
                return False
        except Exception as e:
            self.log(f"Exception in _okx_cancel_algo_order: {e}", level="error")
            return False

    def _close_all_entry_orders(self):
        try:
            self.log("Attempting to close unfilled linear entry orders...", level="info")

            path = "/api/v5/trade/orders-pending"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)

            if not response or response.get('code') != '0':
                self.log("No orders found or API error (OK if no orders)", level="info")
                return True

            orders = response.get('data', [])
            cancelled_count = 0

            for order in orders:
                try:
                    order_id = order.get('ordId')
                    status = order.get('state')
                    side = order.get('side')
                    if side == 'buy' and status not in ['filled', 'canceled', 'rejected']:
                        if self._okx_cancel_order(self.config['symbol'], order_id):
                            cancelled_count += 1
                            time.sleep(0.1)
                except Exception as e:
                    self.log(f"Error processing OKX order: {e}", level="error")

            if cancelled_count > 0:
                self.log(f"✓ Closed {cancelled_count} unfilled linear entry orders", level="info")
            else:
                self.log(f"No unfilled linear entry orders to close (OK)", level="info")

            return True
        except Exception as e:
            self.log(f"Exception in _close_all_entry_orders: {e} (continuing)", level="error")
            return True

    def _handle_tp_hit(self):
        with self.tp_hit_lock:
            self.tp_hit_triggered = True # Set the flag immediately

        try:
            self.log("=" * 80, level="info")
            self.log("🎯 TP HIT (0.7%) - EXECUTING PROTOCOL", level="info")
            self.log("=" * 80, level="info")

            self.log("Step 1: Closing unfilled entry orders...", level="info")
            self._close_all_entry_orders()

            time.sleep(1)

            self.log("Step 2: Checking OKX position status...", level="info")
            path = "/api/v5/account/positions"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)

            position_still_open = False
            open_qty = 0.0

            if response and response.get('code') == '0':
                positions = response.get('data', [])
                for pos in positions:
                    if pos.get('instId') == self.config['symbol']:
                        pos_qty_str = pos.get('pos', '0')
                        size_val = safe_float(pos_qty_str)
                        if size_val > 0:
                            position_still_open = True
                            open_qty = size_val
                            self.log(f"OKX position still open: {open_qty} {self.config['symbol']} (partial fill)", level="info")
                            break

            if position_still_open and open_qty > 0:
                self.log("Step 3: Waiting 3 seconds (monitoring 3 x 1-second candles)...", level="info")
                for i in range(3):
                    self.log(f"  [{i+1}/3 seconds elapsed]", level="info")
                    time.sleep(1)

                self.log("Step 4: Market closing remaining OKX position...", level="info")
                exit_order_response = self._okx_place_order(
                    self.config['symbol'],
                    "Sell",
                    open_qty,
                    order_type="Market",
                    reduce_only=True
                )

                if exit_order_response and exit_order_response.get('ordId'):
                    self.log(f"✓ Market close order placed for {open_qty} {self.config['symbol']}", level="info")
                else:
                    self.log(f"⚠ Market close order may have failed (OK if already closed)", level="warning")

                time.sleep(1)
                self._cancel_all_exit_orders_and_reset("TP hit - OKX position closed")
            else:
                self.log("OKX position fully closed by TP order. No market close needed.", level="info")
                self._cancel_all_exit_orders_and_reset("TP hit - fully closed")

            with self.tp_hit_lock:
                self.tp_hit_triggered = False

            self.log("=" * 80, level="info")
            self.log("✓ TP HIT PROTOCOL COMPLETE (OKX)", level="info")
            self.log("=" * 80, level="info")

        except Exception as e:
            self.log(f"Exception in _handle_tp_hit (OKX): {e} (continuing)", level="error")
            with self.tp_hit_lock:
                self.tp_hit_triggered = False

    def _handle_eod_exit(self):
        try:
            self.log("=" * 80, level="info")
            self.log("🕐 EOD EXIT TRIGGERED (OKX)", level="info")
            self.log("=" * 80, level="info")

            with self.position_lock:
                is_in_pos = self.in_position
                pos_qty = self.position_qty

            self.log("Step 1: Checking for open OKX positions...", level="info")

            try:
                path = "/api/v5/account/positions"
                params = {"instType": "SWAP", "instId": self.config['symbol']}
                response = self._okx_request("GET", path, params=params)

                if response and response.get('code') == '0':
                    positions = response.get('data', [])
                    for pos in positions:
                        if pos.get('instId') == self.config['symbol']:
                            pos_qty_str = pos.get('pos', '0')
                            size_val = safe_float(pos_qty_str)
                            if size_val > 0:
                                self.log(f"Found open long OKX position: {size_val} {self.config['symbol']} - closing...", level="info")
                                exit_order_response = self._okx_place_order(
                                    self.config['symbol'],
                                    "Sell",
                                    size_val,
                                    order_type="Market",
                                    reduce_only=True
                                )
                                if exit_order_response and exit_order_response.get('ordId'):
                                    self.log(f"✓ Market close order placed", level="info")
                                else:
                                    self.log(f"⚠ Market close failed (OK if already closed)", level="warning")
                                time.sleep(1)
                                break
                else:
                    self.log("No OKX positions found or API error (OK)", level="info")
            except Exception as e:
                self.log(f"Error closing OKX position: {e} (OK, continuing)", level="warning")

            self.log("Step 2: Closing unfilled entry orders...", level="info")
            try:
                self._close_all_entry_orders()
            except Exception as e:
                self.log(f"Error closing entry orders: {e} (OK, continuing)", level="warning")

            time.sleep(0.5)

            self.log("Step 3: Force cancelling all remaining OKX orders...", level="info")
            try:
                path = "/api/v5/trade/cancel-all-after"
                body = {"timeOut": "0", "instType": "SWAP"}
                response = self._okx_request("POST", path, body_dict=body)
                if response and response.get('code') == '0':
                    self.log(f"✓ All OKX orders cancelled", level="info")
                else:
                    self.log(f"⚠ All OKX orders cancel response: {response} (OK)", level="warning")
            except Exception as e:
                self.log(f"Error force cancelling OKX orders: {e} (OK, continuing)", level="error")

            self.log("=" * 80, level="info")
            self.log("✓ EOD EXIT COMPLETE (OKX)", level="info")
            self.log("=" * 80, level="info")

            self._cancel_all_exit_orders_and_reset("EOD Exit")

        except Exception as e:
            self.log(f"Exception in _handle_eod_exit (OKX): {e} (continuing)", level="error")
            self._cancel_all_exit_orders_and_reset("EOD Exit - forced")

    # The _update_account_info method has been removed as private WebSocket endpoints are no longer used.
    # The bot will not track account balance or available equity in real-time.
    # This might impact functionality that relies on account balance checks.

    def _handle_order_update(self, orders_data):
        with self.position_lock:
            current_pending_id = self.pending_entry_order_id
            is_in_pos = self.in_position
            active_exit_orders = dict(self.position_exit_orders)
            tracked_qty = self.position_qty

        with self.entry_order_sl_lock:
            tracked_entry_order = self.entry_order_with_sl

        for order in orders_data:
            if not isinstance(order, dict):
                continue

            order_id = order.get('ordId') or order.get('algoId')
            status = order.get('state')
            symbol = order.get('instId')
            cum_qty = order.get('accFillSz', 0)
            order_qty = order.get('sz', 0)
            exec_status = order.get('execType', '')
            order_type = order.get('ordType', '')

            if not order_id or not status:
                continue

            if symbol and symbol != self.config['symbol']:
                continue

            if active_exit_orders.get('sl') and order_id == active_exit_orders.get('sl') and status in ['filled', 'partially_filled']:
                self.log("=" * 80, level="info")
                self.log(f"🛑 SL HIT DETECTED via SL Order Fill!", level="info")
                self.log(f"Order ID: {str(order_id)[:12]}... Status: {status} | ExecType: {exec_status}", level="info")
                self.log("=" * 80, level="info")

                with self.sl_hit_lock:
                    if not self.sl_hit_triggered:
                        self.sl_hit_triggered = True
                        threading.Timer(0.5, self._handle_sl_hit).start()
                return

            if current_pending_id and order_id == current_pending_id:
                with self.position_lock:
                    if self.pending_entry_order_details:
                        self.pending_entry_order_details['status'] = status
                        self.pending_entry_order_details['cum_qty'] = cum_qty # Changed from cumQty to cum_qty

                if status in ['filled', 'partially_filled'] or safe_float(cum_qty) > 0:
                    self.log("=" * 80, level="info")
                    self.log(f"🎉 ENTRY FILLED: {cum_qty}/{order_qty} {self.config['symbol']}", level="info")
                    self.log("=" * 80, level="info")

                    if status in ['filled']:
                        threading.Timer(2.0, lambda: self._confirm_and_set_active_position(order_id)).start()
                    else:
                        threading.Timer(5.0, lambda: self._confirm_and_set_active_position(order_id)).start()
                    return

                elif status in ['canceled', 'live', 'failed'] and not is_in_pos:
                    self.log(f"❌ Entry order {status}", level="warning")
                    self._reset_entry_state(f"Entry order {status}")
                    with self.entry_order_sl_lock:
                        self.entry_order_with_sl = None
                    return

            elif is_in_pos and order_id == active_exit_orders.get('tp'):
                if status in ['filled', 'partially_filled'] or safe_float(cum_qty) > 0:
                    self.log("=" * 80, level="info")
                    self.log(f"!!! TP HIT !!! {cum_qty}/{order_qty} {self.config['symbol']}", level="info")
                    self.log("=" * 80, level="info")

                    with self.tp_hit_lock:
                        if not self.tp_hit_triggered:
                            self.tp_hit_triggered = True
                            threading.Timer(0.5, self._handle_tp_hit).start()
                    return


    def _detect_sl_from_position_update(self, positions_msg):
        with self.position_lock:
            was_in_position = self.in_position
            expected_qty = self.position_qty

        if not was_in_position or expected_qty == 0:
            return

        current_position_size = 0
        for pos in positions_msg:
            if pos.get('instId') == self.config['symbol']:
                size_rv = safe_float(pos.get('pos', 0))
                current_position_size = size_rv
                break

        if was_in_position and current_position_size == 0 and expected_qty > 0:
            self.log("=" * 80, level="info")
            self.log("🛑 SL/CLOSURE DETECTED via Position Update!", level="info")
            self.log(f"Expected Qty: {expected_qty} → Current Qty: 0", level="info")
            self.log("=" * 80, level="info")

            with self.sl_hit_lock:
                if not self.sl_hit_triggered:
                    self.sl_hit_triggered = True
                    with self.entry_order_sl_lock:
                        self.entry_order_with_sl = None
                    threading.Timer(0.1, self._handle_sl_hit).start()


    def _handle_sl_hit(self):
        with self.sl_hit_lock:
            self.sl_hit_triggered = True # Set the flag immediately

        try:
            self.log("=" * 80, level="info")
            self.log("🛑 STOP LOSS HIT - EXECUTING CLEANUP", level="info")
            self.log("=" * 80, level="info")

            self.log("Position already closed by exchange SL", level="info")

            try:
                self._close_all_entry_orders()
            except Exception as e:
                self.log(f"Entry order cleanup: {e} (OK)", level="warning")

            time.sleep(0.5)

            self.log("Cancelling TP order and resetting state...", level="info")
            self._cancel_all_exit_orders_and_reset("SL hit - position closed by exchange")

            with self.entry_order_sl_lock:
                self.entry_order_with_sl = None

            with self.sl_hit_lock:
                self.sl_hit_triggered = False

            self.log("=" * 80, level="info")
            self.log("✓ SL CLEANUP COMPLETE", level="info")
            self.log("=" * 80, level="info")
        except Exception as e:
            self.log(f"Exception in _handle_sl_hit: {e}", level="error")
            try:
                self._cancel_all_exit_orders_and_reset("SL hit - forced reset")
            except:
                pass
            with self.sl_hit_lock:
                self.sl_hit_triggered = False
            with self.entry_order_sl_lock:
                self.entry_order_with_sl = None

    def _confirm_and_set_active_position(self, filled_order_id):
        try:
            self.log(f"Confirming OKX position...", level="info")

            path = "/api/v5/account/positions"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)

            entry_confirmed = False
            actual_entry_price = 0.0
            actual_qty = 0.0

            if response and response.get('code') == '0':
                positions = response.get('data', [])
                for pos in positions:
                    if pos.get('instId') == self.config['symbol']:
                        size_rv = safe_float(pos.get('pos', 0))
                        if size_rv > 0:
                            avg_entry_price_rv = safe_float(pos.get('avgPx', 0))
                            actual_entry_price = avg_entry_price_rv
                            actual_qty = size_rv
                            entry_confirmed = True
                            break

            if not entry_confirmed or actual_entry_price <= 0:
                self.log("CRITICAL: Could not confirm OKX position!", level="error")
                return

            reduced_tp = self.entry_reduced_tp_flag if hasattr(self, 'entry_reduced_tp_flag') else False

            tp_price_offset = self.config['tp_price_offset']
            sl_price_offset = self.config['sl_price_offset']

            # Assuming long position for now based on strategy (Entry Price Offset +1.0, TP -0.6, SL +30)
            signal_direction = self.pending_entry_order_details.get('signal')

            if signal_direction == 1: # Long position
                tp_price = actual_entry_price + tp_price_offset
                sl_price = actual_entry_price - sl_price_offset
            else: # Short position (signal_direction == -1)
                tp_price = actual_entry_price - tp_price_offset
                sl_price = actual_entry_price + sl_price_offset
            
            with self.position_lock:
                self.in_position = True
                self.position_entry_price = actual_entry_price
                self.position_qty = actual_qty
                self.current_take_profit = tp_price
                self.current_stop_loss = sl_price
                self.pending_entry_order_id = None
                self.position_exit_orders = {}

            self.log("=" * 80, level="info")
            self.log("OKX POSITION OPENED", level="info")
            self.log(f"Entry: ${actual_entry_price:.2f} | Qty: {actual_qty}", level="info")
            self.log(f"TP: ${tp_price:.2f} | SL: ${sl_price:.2f} (separate algo order)", level="info")
            self.log("=" * 80, level="info")

            price_precision = PRODUCT_INFO.get('pricePrecision', 4)
            qty_precision = PRODUCT_INFO.get('qtyPrecision', 8)

            # Place TP and SL as algo (conditional) orders via /api/v5/trade/order-algo
            tp_body = {
                "instId": self.config['symbol'],
                "tdMode": "cross",
                "side": "sell",
                "posSide": "long",
                "ordType": "conditional",
                "sz": f"{actual_qty:.{qty_precision}f}",
                "tpTriggerPx": f"{tp_price:.{price_precision}f}",
                "tpOrdPx": "market",
                "reduceOnly": "true"
            }

            tp_order = self._okx_place_algo_order(tp_body)
            if tp_order and (tp_order.get('algoId') or tp_order.get('ordId')):
                with self.position_lock:
                    self.position_exit_orders['tp'] = tp_order.get('algoId') or tp_order.get('ordId')
                self.log(f"✓ TP algo order placed", level="info")
            else:
                self.log(f"CRITICAL: TP algo order failed! Closing position", level="error")
                self._execute_trade_exit("Failed to place TP")
                return

            sl_body = {
                "instId": self.config['symbol'],
                "tdMode": "cross",
                "side": "sell",
                "posSide": "long",
                "ordType": "conditional",
                "sz": f"{actual_qty:.{qty_precision}f}",
                "slTriggerPx": f"{sl_price:.{price_precision}f}",
                "slOrdPx": "market",
                "reduceOnly": "true"
            }

            sl_order = self._okx_place_algo_order(sl_body)
            if sl_order and (sl_order.get('algoId') or sl_order.get('ordId')):
                with self.position_lock:
                    self.position_exit_orders['sl'] = sl_order.get('algoId') or sl_order.get('ordId')
                self.log(f"✓ SL algo order placed", level="info")
            else:
                self.log(f"CRITICAL: SL algo order failed! Closing position", level="error")
                self._execute_trade_exit("Failed to place SL")
                return

            self.log("=" * 80, level="info")
            self.log("✓ OKX POSITION CONFIGURED (SL and TP active)", level="info")
            self.log("=" * 80, level="info")

            # Account information is no longer updated in real-time via private WebSocket.
        except Exception as e:
            self.log(f"Exception in _confirm_and_set_active_position (OKX): {e}", level="error")

    def _execute_trade_exit(self, reason):
        try:
            self.log(f"=== MANUAL EXIT === Reason: {reason}", level="info")

            with self.position_lock:
                if not self.in_position:
                    self.log("Exit aborted: Not in position", level="warning")
                    return
                qty_to_close = self.position_qty

            with self.position_lock:
                orders_to_cancel = list(self.position_exit_orders.values())

            for order_id in orders_to_cancel:
                if order_id:
                    try:
                        self._okx_cancel_algo_order(self.config['symbol'], order_id)
                        time.sleep(0.2)
                    except Exception as e:
                        self.log(f"Error cancelling order: {e} (OK, continuing)", level="error")

            try:
                self.log(f"Placing market sell for {qty_to_close} {self.config['symbol']}", level="info")
                exit_order = self._okx_place_order(
                    self.config['symbol'],
                    "Sell",
                    qty_to_close,
                    order_type="Market",
                    reduce_only=True
                )

                if not (exit_order and exit_order.get('ordId')):
                    self.log(f"WARNING: Market exit order may have failed (OK if already closed)", level="warning")
            except Exception as e:
                self.log(f"Exception during market exit: {e} (OK, continuing)", level="error")

            time.sleep(1)
            self._cancel_all_exit_orders_and_reset(reason)
        except Exception as e:
            self.log(f"Exception in _execute_trade_exit: {e}", level="error")

    def _check_and_close_any_open_position(self):
        try:
            self.log("Checking for any open OKX positions...", level="info")
            path = "/api/v5/account/positions"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)

            if response and response.get('code') == '0':
                positions = response.get('data', [])
                for pos in positions:
                    if pos.get('instId') == self.config['symbol']:
                        size_rv = safe_float(pos.get('pos', 0))
                        pos_side = pos.get('posSide') or pos.get('side')
                        if size_rv > 0:
                            self.log(f"⚠️ Found open {pos_side} OKX position: {size_rv} {self.config['symbol']}", level="warning")
                            close_side = "Sell" if size_rv > 0 else "Buy"
                            self.log(f"Closing {size_rv} {self.config['symbol']} with market {close_side} order", level="info")
                            close_order = self._okx_place_order(
                                self.config['symbol'],
                                close_side,
                                size_rv,
                                order_type="Market",
                                reduce_only=True
                            )
                            if close_order and close_order.get('ordId'):
                                self.log(f"✓ Position close order placed", level="info")
                                return True
                            else:
                                self.log(f"❌ Failed to place close order", level="error")
                                return False

            self.log("No open OKX positions found", level="info")
            return False
        except Exception as e:
            self.log(f"Exception in _check_and_close_any_open_position (OKX): {e}", level="error")
            return False

    def _reset_entry_state(self, reason):
        with self.position_lock:
            self.pending_entry_order_id = None
            self.entry_reduced_tp_flag = False
            self.pending_entry_order_details = {}
        with self.entry_order_sl_lock:
            self.entry_order_with_sl = None
        self.log(f"Entry state reset. Reason: {reason}", level="info")

    def _cancel_all_exit_orders_and_reset(self, reason):
        with self.position_lock:
            orders_to_cancel = list(self.position_exit_orders.values())

            self.in_position = False
            self.position_entry_price = 0.0
            self.position_qty = 0.0
            self.current_take_profit = 0.0
            self.current_stop_loss = 0.0
            self.position_exit_orders = {}
            self.pending_entry_order_id = None
            self.entry_reduced_tp_flag = False

        with self.entry_order_sl_lock:
            self.entry_order_with_sl = None

        self.log("=" * 80, level="info")
        self.log(f"POSITION CLOSED - Reason: {reason}", level="info")
        self.log("=" * 80, level="info")

        for order_id in orders_to_cancel:
            if order_id:
                try:
                    self._okx_cancel_algo_order(self.config['symbol'], order_id)
                    time.sleep(0.1)
                except Exception as e:
                    self.log(f"Error cancelling order: {e} (OK, continuing)", level="error")

        # Account information is no longer updated in real-time via private WebSocket.

    def _get_latest_data_and_indicators(self):
        try:
            with self.data_lock:
                current_price = self._get_current_market_price(self.config['symbol'])
                if current_price is None:
                    self.log(f"Could not get current market price.", level="error")
                    return None

            self.log("=" * 80, level="info")
            self.log("MARKET DATA ACQUIRED", level="info")
            self.log("=" * 80, level="info")
            self.log(f"Current Price: ${current_price:.2f}", level="info")
            self.log("=" * 80, level="info")

            return {
                'current_price': current_price
            }

        except Exception as e:
            self.log(f"Exception in _get_latest_data_and_indicators: {e}", level="error")
            return None

    def _check_entry_conditions(self, market_data):
        with self.position_lock:
            if self.in_position:
                self.log("Entry check: Already in position", level="info")
                return False, 0.0, None
            if self.pending_entry_order_id:
                self.log("Entry check: Pending entry order exists", level="info")
                return False, 0.0, None

        current_price = market_data['current_price']
        
        # Determine entry side based on safety lines
        long_condition = current_price > self.config['long_safety_line_price']
        short_condition = current_price < self.config['short_safety_line_price']

        signal = 0
        if long_condition:
            signal = 1 # BUY
        elif short_condition:
            signal = -1 # SELL
        
        if signal == 0:
            self.log(f"No entry signal: Current price {current_price:.2f} not past safety lines.", level="info")
            return False, 0.0, None

        entry_price_offset = self.config['entry_price_offset']
        if signal == 1: # Long
            limit_price = current_price - entry_price_offset # Buy at a slightly lower price
        else: # Short
            limit_price = current_price + entry_price_offset # Sell at a slightly higher price

        self.log(f"Entry check PASSED for {('LONG' if signal == 1 else 'SHORT')} at limit price {limit_price:.2f}", level="info")
        return True, limit_price, signal

    def _initiate_entry_sequence(self, initial_limit_price, signal, batch_size):
        with self.position_lock:
            if self.in_position or self.pending_entry_order_id:
                self.log("Entry aborted: Already in position or pending order exists.", level="warning")
                return

        # Removed _update_account_info as private endpoints are no longer used.
        # The bot will not track account balance or available equity in real-time.
        # This might impact functionality that relies on account balance checks.

        # Calculate quantity based on max_allowed_used and rate_divisor
        max_amount = self.config['max_allowed_used'] / self.config['rate_divisor']
        batch_offset = self.config['batch_offset']
        
        for i in range(batch_size):
            current_limit_price = initial_limit_price
            if i > 0: # Apply batch offset for subsequent orders
                if signal == 1: # Long
                    current_limit_price -= (batch_offset * i)
                else: # Short
                    current_limit_price += (batch_offset * i)

            if current_limit_price <= 0:
                self.log(f"Entry aborted for batch order {i+1}: Invalid limit entry price {current_limit_price}", level="error")
                continue

            qty_base_asset = max_amount / current_limit_price

            qty_precision = PRODUCT_INFO.get('qtyPrecision', 8)
            qty_base_asset = round(qty_base_asset, qty_precision)

            min_contract_qty = PRODUCT_INFO.get('minOrderQty', self.config['min_order_amount'])

            if qty_base_asset < min_contract_qty:
                self.log(f"Entry aborted for batch order {i+1}: Calculated quantity {qty_base_asset} < minimum {min_contract_qty}", level="warning")
                continue

            self.log("=" * 80, level="info")
            self.log(f"PLACING BATCH ENTRY ORDER {i+1}/{batch_size} ({'BUY' if signal == 1 else 'SELL'})", level="info")
            self.log(f"Limit Entry Price: ${current_limit_price:.2f}", level="info")
            self.log(f"Quantity: {qty_base_asset} {self.config['symbol']}", level="info")
            self.log("=" * 80, level="info")

            entry_order_response = self._okx_place_order(
                self.config['symbol'],
                "Buy" if signal == 1 else "Sell",
                qty_base_asset,
                price=current_limit_price,
                order_type="Limit",
                time_in_force="GoodTillCancel"
            )

            if entry_order_response and entry_order_response.get('ordId'):
                order_id = entry_order_response['ordId']
                with self.position_lock:
                    self.pending_entry_order_id = order_id # Only track the last one for now, or need a list
                    self.pending_entry_order_details = {
                        'order_id': order_id,
                        'side': "Buy" if signal == 1 else "Sell",
                        'qty': qty_base_asset,
                        'limit_price': current_limit_price,
                        'signal': signal,
                        'order_type': 'Limit',
                        'status': 'New',
                        'placed_at': datetime.now(timezone.utc)
                    }
                self.log(f"✓ Batch entry order {i+1} placed: OrderID={order_id}", level="info")

                # Start position manager if not already running
                if not hasattr(self, 'position_manager_thread') or self.position_manager_thread is None or not self.position_manager_thread.is_alive():
                    self.position_manager_thread = threading.Thread(
                        target=self._manage_position_lifecycle,
                        name="PositionManager",
                        daemon=True
                    )
                    self.position_manager_thread.start()
                    self.log("✓ Position manager started", level="info")
            else:
                self.log(f"Batch entry order {i+1} placement failed", level="error")
                # If one order fails, should we stop the sequence or continue? For now, continue.

    def _manage_position_lifecycle(self):
        try:
            self.log("Position lifecycle manager started", level="info")

            loop_time_seconds = self.config['loop_time_seconds']
            cancel_unfilled_seconds = self.config['cancel_unfilled_seconds']
            
            last_check_time = time.time()

            while not self.stop_event.is_set():
                time.sleep(loop_time_seconds)
                current_time = time.time()

                with self.position_lock:
                    is_in_pos = self.in_position
                    has_pending_entry = (self.pending_entry_order_id is not None)
                    pending_order_details = self.pending_entry_order_details.copy()

                # Handle pending entry orders
                if has_pending_entry:
                    placed_at = pending_order_details.get('placed_at')
                    if placed_at and (datetime.now(timezone.utc) - placed_at).total_seconds() > cancel_unfilled_seconds:
                        self.log(f"Pending entry order {self.pending_entry_order_id} not filled within {cancel_unfilled_seconds} seconds. Cancelling...", level="warning")
                        self._okx_cancel_order(self.config['symbol'], self.pending_entry_order_id)
                        self._reset_entry_state("Order not filled in time")
                        continue # Skip to next loop iteration

                # Check current market price for TP condition 2 (TP price below market price for short)
                current_market_price = self._get_current_market_price(self.config['symbol'])
                if current_market_price is None:
                    self.log("Could not get current market price for condition checks.", level="warning")
                    continue

                if has_pending_entry and pending_order_details.get('signal') is not None:
                    signal_direction = pending_order_details['signal']
                    limit_price = pending_order_details['limit_price']

                    # Condition 2: Cancel if TP price becomes unfavorable
                    if self.config['cancel_on_tp_price_below_market'] and self.current_take_profit > 0:
                        if (signal_direction == 1 and self.current_take_profit > current_market_price) or \
                           (signal_direction == -1 and self.current_take_profit < current_market_price):
                            self.log(f"Cancelling pending order {self.pending_entry_order_id}: TP price ({self.current_take_profit:.2f}) is now unfavorable ({current_market_price:.2f}).", level="warning")
                            self._okx_cancel_order(self.config['symbol'], self.pending_entry_order_id)
                            self._reset_entry_state("TP price became unfavorable")
                            continue

                    # Condition 3: Cancel if Entry price becomes unfavorable
                    if self.config['cancel_on_entry_price_below_market']:
                        if (signal_direction == 1 and limit_price > current_market_price) or \
                           (signal_direction == -1 and limit_price < current_market_price):
                            self.log(f"Cancelling pending order {self.pending_entry_order_id}: Entry price ({limit_price:.2f}) is now unfavorable ({current_market_price:.2f}).", level="warning")
                            self._okx_cancel_order(self.config['symbol'], self.pending_entry_order_id)
                            self._reset_entry_state("Entry price became unfavorable")
                            continue

                if not is_in_pos and not has_pending_entry:
                    self.log("Position manager exiting (no active position/order)", level="info")
                    break

            self.log("Position manager thread finished", level="info")
        except Exception as e:
            self.log(f"Exception in _manage_position_lifecycle: {e}", level="error")

    def _process_new_cycle_and_check_entry(self):
        try:
            self.log("=" * 80, level="info")
            self.log(f"🕐 NEW TRADING CYCLE - Entry Check @ {datetime.now(timezone.utc).strftime('%H:%M:%S')}", level="info")
            self.log("=" * 80, level="info")

            market_data = self._get_latest_data_and_indicators()

            if not market_data:
                self.log("Failed to get market data. Skipping entry check.", level="error")
                return

            with self.position_lock:
                is_in_pos = self.in_position
                has_pending = (self.pending_entry_order_id is not None)

            if is_in_pos or has_pending:
                self.log("Skipping entry: Already in position or pending order exists", level="info")
                return

            self.log("Checking entry conditions...", level="info")
            entry_signal, limit_price, signal_direction = self._check_entry_conditions(market_data)

            if entry_signal:
                self.log(f">>> ENTRY SIGNAL <<< Limit order at {limit_price:.2f} for {('LONG' if signal_direction == 1 else 'SHORT')}", level="info")
                self._initiate_entry_sequence(limit_price, signal_direction, self.config['batch_size_per_loop'])
            else:
                self.log("No entry signal. Waiting for next check.", level="info")
        except Exception as e:
            self.log(f"Exception in _process_new_cycle_and_check_entry: {e}", level="error")

    def _main_trading_logic(self):
        try:
            self.log("=== MAIN TRADING LOGIC STARTED ===", level="info")
            loop_time_seconds = self.config['loop_time_seconds']

            while not self.stop_event.is_set():
                self._process_new_cycle_and_check_entry()
                time.sleep(loop_time_seconds)

        except Exception as e:
            self.log(f"CRITICAL ERROR in _main_trading_logic: {e}", level="error")

    def _initialize_websocket(self):
        ws_url = self._get_ws_url()
        try:
            self.ws = websocket.WebSocketApp(
                ws_url,
                on_open=self._on_websocket_open,
                on_message=self._on_websocket_message,
                on_error=self._on_websocket_error,
                on_close=self._on_websocket_close
            )
            return self.ws
        except Exception as e:
            self.log(f"Exception initializing WebSocket: {e}", level="error")
            return None

    def _initialize_websocket_and_start_main_loop(self):
        self.log("OKX BOT STARTING", level="info")
        try:
            self.ws_client = self._initialize_websocket()
            if self.ws_client is None:
                self.log("Failed to initialize WebSocket. Exiting.", level="error")
                return

            # For public WebSocket, no authentication is needed. Subscriptions are sent directly on_open.
            self.log("Public WebSocket: No authentication required.", level="info")
            
            # Start the WebSocket in a separate thread
            ws_thread = threading.Thread(target=self.ws_client.run_forever, daemon=True)
            ws_thread.start()
            self.log("WebSocket client started in a separate thread.", level="info")

            self.log("Waiting for WebSocket subscriptions to be ready...", level="info")
            if not self.ws_subscriptions_ready.wait(timeout=20): # Longer timeout for subscriptions
                self.log("WebSocket subscriptions not ready within timeout. Exiting.", level="error")
                return

            # Initial account information is no longer updated in real-time via private WebSocket.
            # The bot will not track account balance or available equity in real-time.
            # This might impact functionality that relies on account balance checks.
            
            self.bot_startup_complete = True
            self.log("Bot startup sequence complete.", level="info")

            # Start a periodic task to update account info and emit to frontend
            self.account_info_updater_thread = threading.Thread(target=self._periodic_account_info_update, daemon=True)
            self.account_info_updater_thread.start()

            self._main_trading_logic()

        except Exception as e:
            self.log(f"CRITICAL ERROR in _initialize_websocket_and_start_main_loop: {e}", level="error")
        finally:
            self.stop_event.set()
            self.log("Shutting down...", level="info")
            if self.ws:
                try:
                    self.ws.close()
                except Exception:
                    pass
            self.log("OKX BOT SHUTDOWN COMPLETE", level="info")

    def _periodic_account_info_update(self):
        while not self.stop_event.is_set():
            try:
                # Fetch account balance
                path_balance = "/api/v5/account/balance"
                params_balance = {"ccy": "USDT"} # Assuming USDT as the currency for balance
                response_balance = self._okx_request("GET", path_balance, params=params_balance)

                total_balance = 0.0
                available_balance = 0.0
                if response_balance and response_balance.get('code') == '0':
                    data = response_balance.get('data', [])
                    if data and isinstance(data, list) and len(data) > 0:
                        account_details = data[0]
                        total_balance = safe_float(account_details.get('totalEq', '0'))
                        available_balance = safe_float(account_details.get('availEq', '0'))
                
                # Fetch open orders to calculate total trades (simplified, can be improved)
                path_orders = "/api/v5/trade/fills"
                params_orders = {"instType": "SWAP", "limit": "100"} # Fetch last 100 fills
                response_orders = self._okx_request("GET", path_orders, params=params_orders)
                
                total_trades = 0
                if response_orders and response_orders.get('code') == '0':
                    fills = response_orders.get('data', [])
                    total_trades = len(fills) # Simple count of recent fills

                # Calculate net profit (very basic, needs actual trade tracking for accuracy)
                # For now, let's assume net profit is 0 or derived from some simple metric
                net_profit = 0.0 # Placeholder, actual calculation requires tracking trades

                self.emit('account_update', {
                    'total_balance': total_balance,
                    'available_balance': available_balance,
                    'net_profit': net_profit,
                    'total_trades': total_trades
                })
                self.log(f"Account info updated: Total Balance={total_balance:.2f}, Available={available_balance:.2f}, Total Trades={total_trades}", level="info")

            except Exception as e:
                self.log(f"Error in periodic account info update: {e}", level="error")
            finally:
                time.sleep(self.config.get('account_update_interval_seconds', 10)) # Update every 10 seconds (configurable)

    def batch_modify_tpsl(self):
        self.log("Initiating batch TP/SL modification...", level="info")
        try:
            current_market_price = self._get_current_market_price(self.config['symbol'])
            if current_market_price is None:
                self.log("Could not get current market price for batch TP/SL modification.", level="error")
                self.emit('error', {'message': 'Failed to batch modify TP/SL: Could not get current market price.'})
                return

            path = "/api/v5/account/positions"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)

            if not response or response.get('code') != '0':
                self.log(f"Failed to fetch open positions for batch TP/SL modification: {response}", level="error")
                self.emit('error', {'message': 'Failed to batch modify TP/SL: Could not fetch open positions.'})
                return

            positions = response.get('data', [])
            modified_count = 0
            tp_price_offset = self.config['tp_price_offset']
            sl_price_offset = self.config['sl_price_offset']
            price_precision = PRODUCT_INFO.get('pricePrecision', 4)
            qty_precision = PRODUCT_INFO.get('qtyPrecision', 8)

            for pos in positions:
                if pos.get('instId') == self.config['symbol']:
                    pos_qty = safe_float(pos.get('pos', '0'))
                    pos_side = pos.get('posSide')
                    avg_px = safe_float(pos.get('avgPx', '0'))

                    if pos_qty > 0 and avg_px > 0:
                        # Recalculate TP/SL based on current market price (or avg_px if preferred)
                        # Here we use avg_px as the base for recalculation, similar to initial placement
                        if pos_side == 'long':
                            new_tp = avg_px + tp_price_offset
                            new_sl = avg_px - sl_price_offset
                        else: # short
                            new_tp = avg_px - tp_price_offset
                            new_sl = avg_px + sl_price_offset
                        
                        # Cancel existing algo orders for this position
                        # This part assumes we track algoIds for each position or can retrieve them
                        # For simplicity, we'll try to cancel any existing TP/SL for this instId and then place new ones
                        # A more robust solution would track algoIds per position
                        self.log(f"Cancelling existing TP/SL for {self.config['symbol']} before placing new ones...", level="info")
                        # OKX API does not have a direct way to cancel all algo orders for a position easily without their algoId
                        # A more complex implementation would involve listing algo orders and filtering by instId and type.
                        # For now, we assume we want to update the *current* position's TP/SL if it exists.
                        # The _confirm_and_set_active_position already places TP/SL.
                        # To modify, we need to cancel existing algo orders and place new ones.
                        # This implies we need to store algoIds in self.position_exit_orders more persistently.

                        # For demonstration, let's assume we can update them or replace them
                        # This requires cancelling existing algo orders and placing new ones.
                        # The current structure of self.position_exit_orders stores the algoIds for the *current* open position.
                        # We need to make sure this is properly managed when multiple batch orders could lead to multiple open positions.
                        # However, the bot is designed for a single open position at a time (in_position flag).
                        # So, we modify the TP/SL for the *current* active position.

                        with self.position_lock:
                            if self.in_position and self.position_exit_orders:
                                if 'tp' in self.position_exit_orders and self.position_exit_orders['tp']:
                                    self._okx_cancel_algo_order(self.config['symbol'], self.position_exit_orders['tp'])
                                if 'sl' in self.position_exit_orders and self.position_exit_orders['sl']:
                                    self._okx_cancel_algo_order(self.config['symbol'], self.position_exit_orders['sl'])
                                time.sleep(0.5) # Give some time for cancellation

                                # Place new TP and SL algo orders
                                tp_body = {
                                    "instId": self.config['symbol'],
                                    "tdMode": "cross",
                                    "side": "sell" if pos_side == 'long' else "buy",
                                    "posSide": pos_side,
                                    "ordType": "conditional",
                                    "sz": f"{pos_qty:.{qty_precision}f}",
                                    "tpTriggerPx": f"{new_tp:.{price_precision}f}",
                                    "tpOrdPx": "market",
                                    "reduceOnly": "true"
                                }

                                tp_order = self._okx_place_algo_order(tp_body)
                                if tp_order and (tp_order.get('algoId') or tp_order.get('ordId')):
                                    self.position_exit_orders['tp'] = tp_order.get('algoId') or tp_order.get('ordId')
                                    self.log(f"✓ New TP algo order placed for position", level="info")
                                else:
                                    self.log(f"CRITICAL: New TP algo order failed for position!", level="error")

                                sl_body = {
                                    "instId": self.config['symbol'],
                                    "tdMode": "cross",
                                    "side": "sell" if pos_side == 'long' else "buy",
                                    "posSide": pos_side,
                                    "ordType": "conditional",
                                    "sz": f"{pos_qty:.{qty_precision}f}",
                                    "slTriggerPx": f"{new_sl:.{price_precision}f}",
                                    "slOrdPx": "market",
                                    "reduceOnly": "true"
                                }

                                sl_order = self._okx_place_algo_order(sl_body)
                                if sl_order and (sl_order.get('algoId') or sl_order.get('ordId')):
                                    self.position_exit_orders['sl'] = sl_order.get('algoId') or sl_order.get('ordId')
                                    self.log(f"✓ New SL algo order placed for position", level="info")
                                else:
                                    self.log(f"CRITICAL: New SL algo order failed for position!", level="error")
                                
                                self.current_take_profit = new_tp
                                self.current_stop_loss = new_sl
                                modified_count += 1
                                self.emit('position_update', {
                                    'in_position': self.in_position,
                                    'position_entry_price': self.position_entry_price,
                                    'position_qty': self.position_qty,
                                    'current_take_profit': self.current_take_profit,
                                    'current_stop_loss': self.current_stop_loss
                                })

            if modified_count > 0:
                self.log(f"Successfully modified TP/SL for {modified_count} positions.", level="info")
                self.emit('success', {'message': f'Successfully modified TP/SL for {modified_count} positions.'})
            else:
                self.log("No active positions found to modify TP/SL.", level="warning")
                self.emit('warning', {'message': 'No active positions found to modify TP/SL.'})

        except Exception as e:
            self.log(f"Exception in batch_modify_tpsl: {e}", level="error")
            self.emit('error', {'message': f'Failed to batch modify TP/SL: {str(e)}'})
        self.log("Batch TP/SL modification complete.", level="info")



    def batch_cancel_orders(self):
        self.log("Initiating batch order cancellation...", level="info")
        try:
            path = "/api/v5/trade/orders-pending"
            params = {"instType": "SWAP", "instId": self.config['symbol']}
            response = self._okx_request("GET", path, params=params)

            if not response or response.get('code') != '0':
                self.log(f"Failed to fetch pending orders for batch cancellation: {response}", level="error")
                self.emit('error', {'message': 'Failed to batch cancel orders: Could not fetch pending orders.'})
                return

            orders = response.get('data', [])
            cancelled_count = 0

            for order in orders:
                order_id = order.get('ordId')
                algo_id = order.get('algoId') # Check if it's an algo order

                if order_id:
                    if algo_id: # It's an algo order
                        if self._okx_cancel_algo_order(self.config['symbol'], algo_id):
                            cancelled_count += 1
                            time.sleep(0.1)
                    else: # Regular order
                        if self._okx_cancel_order(self.config['symbol'], order_id):
                            cancelled_count += 1
                            time.sleep(0.1)

            if cancelled_count > 0:
                self.log(f"Successfully cancelled {cancelled_count} pending orders.", level="info")
                self.emit('success', {'message': f'Successfully cancelled {cancelled_count} pending orders.'})
            else:
                self.log("No pending orders found to cancel.", level="warning")
                self.emit('warning', {'message': 'No pending orders found to cancel.'})

        except Exception as e:
            self.log(f"Exception in batch_cancel_orders: {e}", level="error")
            self.emit('error', {'message': f'Failed to batch cancel orders: {str(e)}'})
            self.log("Batch order cancellation complete.", level="info")

