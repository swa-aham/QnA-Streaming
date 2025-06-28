import requests
import time
import json

import os

API_KEY = os.environ.get("API_KEY")
API_URL = os.environ.get("API_URL", "https://api.example.com/v1/poll")

HEADERS = {
    "Content-Type": "application/json",
}

PROMPT = "Tell me a quick fact."

PAYLOAD = {
    "contents": [
        {
            "parts": [
                {"text": PROMPT}
            ]
        }
    ]
}

def send_request():
    response = requests.post(f"{API_URL}?key={API_KEY}", headers=HEADERS, json=PAYLOAD)
    return response

def extract_retry_delay(response_json):
    try:
        details = response_json["error"].get("details", [])
        for detail in details:
            if detail.get("@type") == "type.googleapis.com/google.rpc.RetryInfo":
                return detail.get("retryDelay", "unknown")
    except Exception as e:
        pass
    return "unknown"

def main():
    request_id = 1
    start_time = time.time()

    while True:
        elapsed = time.time() - start_time
        response = send_request()

        if response.status_code == 200:
            print(f"request {request_id} successful at {elapsed:.2f}s")
            request_id += 1
        elif response.status_code == 429:
            response_json = response.json()
            retry_delay = extract_retry_delay(response_json)
            print(f"request {request_id} failed due to rate limit at {elapsed:.2f}s and retrydelay is {retry_delay}")
            # Do not increment request_id – keep retrying this one
        else:
            print(f"request {request_id} failed with status {response.status_code} at {elapsed:.2f}s")
            break  # stop on unexpected errors

if __name__ == "__main__":
    main()
