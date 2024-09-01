import os
import time
import signal
from datetime import datetime, timedelta
from vcon import Vcon
from vcon.party import Party
from vcon.dialog import Dialog

import requests
import json
import logging
import dotenv
import requests
import email.utils


# Load environment variables from .env file
dotenv.load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# SignalWire credentials
PROJECT_ID = os.getenv('SIGNALWIRE_PROJECT_ID')
AUTH_TOKEN = os.getenv('SIGNALWIRE_AUTH_TOKEN')
SPACE_URL = os.getenv('SIGNALWIRE_SPACE_URL')

# Webhook URL
WEBHOOK_URL = os.getenv('WEBHOOK_URL')

# Poll interval in seconds (default to 5 minutes if not set)
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', 300))

# Check if all required environment variables are set
required_vars = {
    'SIGNALWIRE_PROJECT_ID': PROJECT_ID,
    'SIGNALWIRE_SPACE_URL': SPACE_URL,
    'SIGNALWIRE_AUTH_TOKEN': AUTH_TOKEN,
    'WEBHOOK_URL': WEBHOOK_URL,
    'POLL_INTERVAL': POLL_INTERVAL
}

missing_vars = [var for var, value in required_vars.items() if value is None]
if missing_vars:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

recording_url = f"{SPACE_URL}/api/laml/2010-04-01/Accounts/{PROJECT_ID}/Recordings"
calls_url = f"{SPACE_URL}/api/laml/2010-04-01/Accounts/{PROJECT_ID}/Calls"

# Flag to control the main loop
running = True

def signal_handler(signum, frame):
    """Handle termination signals"""
    global running
    logging.info(f"Received signal {signum}. Shutting down gracefully...")
    running = False

# Register the signal handler
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
last_check_time = datetime.utcnow() - timedelta(seconds=POLL_INTERVAL)


def fetch_call_meta(call_sid):
    """
    Fetch the meta data of a call given the call SID.

    :param call_sid: The SID of the call to fetch the meta data for
    :return: A dictionary containing the call meta data
    """

    url = f"{calls_url}/{call_sid}"

    payload = {}
    headers = {
        'Accept': 'application/json'
    }

    # Send a GET request to the SignalWire API
    response = requests.request("GET", url, 
                                headers=headers, 
                                data=payload, 
                                auth=(PROJECT_ID, AUTH_TOKEN))

    # Return the meta data as a JSON object
    return response.json()
    
    
def fetch_new_recordings(last_check_time):
    """
    Fetch all the recordings created after the given date and time.

    :param last_check_time: The date and time to fetch recordings after
    :return: A list of recordings created after the given date and time
    """
    url = f"{recording_url}/?DateCreatedAfter={last_check_time.isoformat()}"
    payload = {}
    headers = {
        'Accept': 'application/json'
    }
    
    response = requests.request("GET",
                                url,
                                headers=headers,
                                data=payload,
                                auth=(PROJECT_ID, AUTH_TOKEN))
    
    # Update last_check_time
    last_check_time = datetime.utcnow()
    
    # Check if the request was successful
    if response.status_code == 200:
        # Extract the recordings from the response
        recordings = response.json()['recordings']
        return recordings
    else:
        raise Exception(f"Failed to fetch recordings: {response.status_code}")
    
    
def fetch_transcription(url):
    """
    Fetch the transcription of a recording given the transcription URL.

    :param url: The URL of the transcription
    :return: The transcription as a JSON object
    :raises Exception: If the request is not successful
    """
    url = f"{SPACE_URL}{url}"
    
    response = requests.get(url, auth=(PROJECT_ID, AUTH_TOKEN))
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch transcription: {response.status_code}")
    
def create_vcon_from_recording(recording) -> Vcon:
    """
    Create a vCon object from a SignalWire recording object.

    :param recording: The SignalWire recording object
    :return: The created vCon object
    """
    
    vcon = Vcon.build_new()
    
    # Fetch the call meta data based on ths recording['call_sid']
    call_meta = fetch_call_meta(recording['call_sid'])
    
    # Try to convert recording['date_created'] from RFC 2822 to ISO format
    try:
        recording_date_created = email.utils.parsedate_to_datetime(recording['date_created'])
        recording_date_created_iso = recording_date_created.isoformat()
    except TypeError:
        logging.warning(f"Failed to parse recording['date_created'] for {recording['sid']}")
        recording_date_created_iso = None

    party1 = Party({"tel": call_meta['to_formatted']})
    vcon.add_party(party1)
    party2 = Party({"tel": call_meta['from_formatted']})
    vcon.add_party(party2)
    
    # Add the dialog, and calculate the correct URL for the recording
    # Add the attachment with the recording metadata
    # Add the transcription if it exists
    recording_url = f"{SPACE_URL}{recording['uri']}"
    # remove the trailing .json and add .mp3
    recording_url = recording_url[:-5] + '.mp3'
    dialog = Dialog(start=recording_date_created_iso, 
                    parties=[0, 1], 
                    type="recording", 
                    duration=recording['duration'],
                    url=recording_url,
                    mimetype="audio/mpeg")
    vcon.add_dialog(dialog)
    vcon.add_attachment(
        type="recording_metadata",
        body={
            "sid": recording['sid'],
            "account_sid": recording['account_sid'],
            "call_sid": recording['call_sid'],
            "channels": recording['channels'],
            "source": "SignalWire",
        } 
    )
    # Add the transcription if it exists
    if 'transcriptions' in recording['subresource_uris']:
        response = fetch_transcription(recording['subresource_uris']['transcriptions'])
        
        for transcription in response['transcriptions']:
            if 'text' in transcription:
                vcon.add_attachment(
                    type="transcription",
                    body=transcription
                )
    return vcon

def download_recording(url):
    """Download the recording file"""
    response = requests.get(url, auth=(PROJECT_ID, AUTH_TOKEN))
    if response.status_code == 200:
        return response.content
    else:
        raise Exception(f"Failed to download recording: {response.status_code}")

def send_vcon_to_webhook(vcon):
    """Send the vCon to the configured webhook"""
    headers = {'Content-Type': 'application/json'}
    payload = vcon.to_json()

    try:
        response = requests.post(WEBHOOK_URL, headers=headers, data=payload, timeout=10)
        response.raise_for_status()
        logging.info(f"Successfully sent vCon to webhook: {vcon.uuid}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send vCon to webhook: {str(e)}")

def process_recordings(last_check_time):
    new_recordings = fetch_new_recordings(last_check_time)

    for recording in new_recordings:
        logging.info(f"Processing recording: {recording['sid']}")

        try:
            # Create vCon
            vcon = create_vcon_from_recording(recording)
            # Send the vCon to the configured webhook
            send_vcon_to_webhook(vcon)

            logging.info(f"Processed vCon for recording: {recording['sid']}")
        except Exception as e:
            logging.error(f"Error processing recording {recording['sid']}: {str(e)}")

def main():
    global running
    last_check_time = datetime.utcnow() - timedelta(seconds=POLL_INTERVAL)

    logging.info("Starting SignalWire vCon processing script")

    while running:
        current_time = datetime.utcnow()
        logging.info(f"Checking for new recordings since {last_check_time}")

        try:
            process_recordings(last_check_time)
        except Exception as e:
            logging.error(f"Error in main loop: {str(e)}")

        last_check_time = current_time

        # Check if we should continue running before sleeping
        if running:
            logging.info(f"Sleeping for {POLL_INTERVAL} seconds")
            # Sleep in small intervals to allow for quicker shutdown
            for _ in range(POLL_INTERVAL):
                if not running:
                    break
                time.sleep(1)

    logging.info("SignalWire vCon processing script has shut down")

if __name__ == "__main__":
    main()