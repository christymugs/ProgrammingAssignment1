import zmq
import json
import time
import logging

# Command-line arguments
import argparse
parser = argparse.ArgumentParser(description="Subscriber for Pub/Sub system.")
parser.add_argument('--topic', type=str, required=True, help="Topic to subscribe to.")
parser.add_argument('--discovery_address', type=str, default="tcp://localhost:5555", help="Discovery service address.")
args = parser.parse_args()

# ZMQ context and sockets
context = zmq.Context()

# Connect to the discovery service to get the publisher's address
discovery_socket = context.socket(zmq.REQ)
discovery_socket.connect(args.discovery_address)

#### LOGGGING CONFIG
logger = logging.getLogger("SubscriberAppIn")
logging.basicConfig(filename='logs/application.log',level=logging.DEBUG,format='%(asctime)s %(levelname)-8s SubscriberAppIn: %(message)s')
logger.info("Starting up")

# Function to query discovery for the publisher's address
def lookup_publisher():
    message = {
        'type': 'lookup',
        'topic': args.topic
    }
    discovery_socket.send_json(message)
    response = discovery_socket.recv_json()
    if response['status'] == 'found':
        return response['address']
    else:
        logger.warning(f"No publisher found for topic {args.topic}")
        #print(f"No publisher found for topic {args.topic}")
        return None

# Subscribe to the topic from the publisher
def subscribe_to_topic(publisher_address):
    subscriber_socket = context.socket(zmq.SUB)
    subscriber_socket.connect(publisher_address)
    subscriber_socket.setsockopt_string(zmq.SUBSCRIBE, args.topic)
    
    while True:
        message = subscriber_socket.recv_string()
        logger.info(f"Received: {message}")
        #print(f"Received: {message}")

# Main loop
if __name__ == '__main__':
    publisher_address = lookup_publisher()
    while publisher_address is None:
        time.sleep(1)
        publisher_address = lookup_publisher()
    if publisher_address:
        subscribe_to_topic(publisher_address)
