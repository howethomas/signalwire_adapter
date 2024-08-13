import os
import time
import signal
from datetime import datetime, timedelta
from signalwire.rest import Client as signalwire_client
from vcon import Vcon
import requests
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# SignalWire credentials
PROJECT_ID = os.environ.get('SIGNALWIRE_PROJECT_ID')
AUTH_TOKEN = os.environ.get('SIGNALWIRE_AUTH_TOKEN')
SPACE_URL = os.environ.get('SIGNALWIRE_SPACE_URL')

# Webhook URL
WEBHOOK_URL = os.environ.get('WEBHOOK_URL')

# Poll interval in seconds (default to 5 minutes if not set)
POLL_INTERVAL = int(os.environ.get('POLL_INTERVAL', 300))

# Initialize SignalWire client
client = signalwire_client(PROJECT_ID, AUTH_TOKEN, signalwire_space_url=SPACE_URL)

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

def fetch_new_recordings(last_check_time):
    """Fetch new recordings since the last check"""
    recordings = client.recordings.list(date_created_after=last_check_time.isoformat())
    return recordings

def create_vcon_from_recording(recording):
    """Create a vCon object from a SignalWire recording"""
    vcon = Vcon.build_new()
    
    # Add recording details to dialog
    vcon.add_dialog({
        "type": "recording",
        "start": recording.date_created.isoformat(),
        "duration": recording.duration,
        "url": recording.media_url,
        "mimetype": recording.media_content_type,
        "parties": [0, 1],  # Assuming two parties for simplicity
    })
    
    # Add parties (this is a simplified example, you might want to fetch more details)
    vcon.add_party({"tel": recording.from_formatted})
    vcon.add_party({"tel": recording.to_formatted})
    
    # Add recording metadata as an attachment
    vcon.add_attachment(
        type="recording_metadata",
        body={
            "sid": recording.sid,
            "account_sid": recording.account_sid,
            "call_sid": recording.call_sid,
            "channels": recording.channels,
            "source": "SignalWire",
        },
        encoding="json"
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
    payload = vcon.to_dict()
    
    try:
        response = requests.post(WEBHOOK_URL, headers=headers, data=json.dumps(payload), timeout=10)
        response.raise_for_status()
        logging.info(f"Successfully sent vCon to webhook: {vcon.uuid}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send vCon to webhook: {str(e)}")

def process_recordings(last_check_time):
    new_recordings = fetch_new_recordings(last_check_time)
    
    for recording in new_recordings:
        logging.info(f"Processing recording: {recording.sid}")
        
        try:
            # Create vCon
            vcon = create_vcon_from_recording(recording)
            
            # Download the actual recording file
            audio_content = download_recording(recording.media_url)
            
            # Add the audio content to the vCon
            vcon.add_attachment(
                type="audio_recording",
                body=audio_content,
                encoding="base64url"
            )
            
            # Send the vCon to the configured webhook
            send_vcon_to_webhook(vcon)
            
            logging.info(f"Processed vCon for recording: {recording.sid}")
        except Exception as e:
            logging.error(f"Error processing recording {recording.sid}: {str(e)}")

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