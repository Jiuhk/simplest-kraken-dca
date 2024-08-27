import requests
from datetime import datetime
import hashlib
import hmac
import base64
import time
import math
from dateutil.relativedelta import relativedelta

KRAKEN_API_PUBLIC_KEY = ""
KRAKEN_API_PRIVATE_KEY = ""
PUBLIC_API_PATH = "/0/public/"
PRIVATE_API_PATH = "/0/private/"

def get_balance():
    try:
        nonce = str(int(time.time() * 1000))
        body = f"nonce={nonce}"
        signature = create_signature(
            KRAKEN_API_PRIVATE_KEY,
            PRIVATE_API_PATH,
            "Balance",
            nonce,
            body
        )
        resp = requests.post("https://api.kraken.com/0/private/Balance", 
                             headers={"API-Key": KRAKEN_API_PUBLIC_KEY, "API-Sign": signature},
                             data=body)
        resp_json = resp.json()
        if resp_json.get('error'):
            return None
        return resp_json.get('result', {})
    except (requests.RequestException, ValueError) as e:
        print(f"Error getting balance: {e}")
        return None

def execute_order():
    try:
        nonce = str(int(time.time() * 1000))
        body = f"nonce={nonce}&pair=xbtgbp&type=buy&ordertype=market&volume=0.0001"
        signature = create_signature(
            KRAKEN_API_PRIVATE_KEY,
            PRIVATE_API_PATH,
            "AddOrder",
            nonce,
            body
        )
        resp = requests.post("https://api.kraken.com/0/private/AddOrder", 
                             headers={"API-Key": KRAKEN_API_PUBLIC_KEY, "API-Sign": signature},
                             data=body)
        resp_json = resp.json()
        if resp_json.get('error'):
            print(f"Order error: {resp_json['error']}")
            return False
        print("Order executed:", resp_json)
        return True
    except (requests.RequestException, ValueError) as e:
        print(f"Error executing order: {e}")
        return False

def calculate_next_order_date(now, fiat_balance, next_fiat_day, btc_fiat_price):
    order_price = btc_fiat_price * 0.0001
    max_orders = math.floor(fiat_balance / order_price)
    
    if max_orders == 0:
        return None
    
    time_delta = next_fiat_day - now
    return now + (time_delta / max_orders)

def create_signature(api_private_key, api_path, endpoint_name, nonce, api_post_body_data):
    api_post = nonce + api_post_body_data
    secret = base64.b64decode(api_private_key)
    sha256 = hashlib.sha256()
    sha256.update(api_post.encode('utf-8'))
    hash256 = sha256.digest()
    hmac512 = hmac.new(secret, digestmod=hashlib.sha512)
    hmac512.update((api_path + endpoint_name).encode('utf-8') + hash256)
    signature_string = base64.b64encode(hmac512.digest()).decode('utf-8')
    return signature_string

def get_btc_fiat_price():
    try:
        resp = requests.get("https://api.kraken.com/0/public/Ticker?pair=XXBTZGBP")
        resp_json = resp.json()
        if resp_json.get('error'):
            return None
        return float(resp_json['result']['XXBTZGBP']['p'][0])
    except (requests.RequestException, ValueError) as e:
        print(f"Error getting BTC price: {e}")
        return None

def dca():
    last_fiat_balance = None
    now = datetime.now()
    next_order_date = now
    next_fiat_day = now + relativedelta(months=1)

    while True:
        balance = get_balance()
        if balance is None:
            print("Skipping cycle due to balance retrieval error.")
            time.sleep(60)
            continue

        btc_fiat_price = get_btc_fiat_price()
        if btc_fiat_price is None:
            print("Skipping cycle due to BTC price retrieval error.")
            time.sleep(60)
            continue

        fiat_balance = balance.get("ZGBP")
        if fiat_balance is None:
            print("ZGBP balance not found.")
            time.sleep(60)
            continue

        now = datetime.now()
        new_fiat_arrived = last_fiat_balance is not None and fiat_balance > last_fiat_balance
        last_fiat_balance = fiat_balance

        if next_order_date is not None and now >= next_order_date:
            order_successful = execute_order(btc_fiat_price)
            if not order_successful:
                print("Order failed, retrying in the next cycle.")
                time.sleep(60)
                continue
            
            next_order_date = calculate_next_order_date(now, fiat_balance, next_fiat_day, btc_fiat_price)

        if new_fiat_arrived:
            next_fiat_day = now + relativedelta(months=1)
            next_order_date = calculate_next_order_date(now, fiat_balance, next_fiat_day, btc_fiat_price)

        time.sleep(60)

if __name__ == "__main__":
    dca()
