# SignalWire to vCon Processor

This Python script continuously polls the SignalWire API for new call recordings, creates vCon (virtual conversation) objects from these recordings, and sends them to a specified webhook. It's designed to run as a long-running process, making it suitable for production environments where continuous monitoring of new recordings is required.

## Features

- Polls SignalWire API at configurable intervals
- Creates vCon objects from SignalWire recordings
- Includes audio content in the vCon
- Sends vCons to a configured webhook
- Handles graceful shutdown on termination signals
- Provides detailed logging

## Prerequisites

- Python 3.6 or higher
- SignalWire account and API credentials
- Webhook endpoint to receive vCon data

## Installation

1. Clone this repository or download the script.

2. Install the required Python packages:

   ```
   pip install signalwire vcon requests
   ```

## Configuration

The script uses environment variables for configuration. Set the following variables before running the script:

- `SIGNALWIRE_PROJECT_ID`: Your SignalWire Project ID
- `SIGNALWIRE_AUTH_TOKEN`: Your SignalWire Auth Token
- `SIGNALWIRE_SPACE_URL`: Your SignalWire Space URL
- `WEBHOOK_URL`: The URL of the webhook to receive vCon data
- `POLL_INTERVAL`: The interval (in seconds) between API polls (default: 300)

Example:

```bash
export SIGNALWIRE_PROJECT_ID=your_project_id
export SIGNALWIRE_AUTH_TOKEN=your_auth_token
export SIGNALWIRE_SPACE_URL=your_space_url
export WEBHOOK_URL=https://your-webhook-endpoint.com/vcon
export POLL_INTERVAL=300
```

## Usage

Run the script using Python:

```
python signalwire_vcon_script.py
```

The script will start polling the SignalWire API for new recordings at the specified interval. For each new recording, it will:

1. Create a vCon object
2. Download the audio content
3. Add the audio content to the vCon
4. Send the vCon to the configured webhook

## Logging

The script logs its operations to the console. You can redirect this output to a file for persistent logging:

```
python signalwire_vcon_script.py > signalwire_vcon_processor.log 2>&1
```

## Termination

The script can be terminated gracefully in the following ways:

- Press Ctrl+C in the terminal where it's running
- Send a SIGTERM signal (e.g., `kill <pid>`)
- Use process management tools like supervisord or systemd

When terminated, the script will complete processing the current batch of recordings before shutting down.

## Error Handling

The script includes error handling to manage issues with the SignalWire API, webhook communication, or other unexpected errors. All errors are logged for later review.

## Production Deployment

For production deployment, consider using a process manager like supervisord or systemd to ensure the script keeps running and to manage automatic restarts if needed.

## Contributing

Contributions to improve the script are welcome. Please submit a pull request or create an issue to discuss proposed changes.

## License

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.

## Support

For questions or issues, please open an issue in this repository. We'll do our best to address your concerns promptly.