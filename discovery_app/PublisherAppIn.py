import zmq
import json
import time

# Command-line arguments
import argparse
parser = argparse.ArgumentParser(description="Publisher for Pub/Sub system.")
parser.add_argument('--topic', type=str, required=True, help="Topic name.")
parser.add_argument('--address', type=str, required=True, help="Address to bind the publisher.")
parser.add_argument('--discovery_address', type=str, default="tcp://localhost:5555", help="Discovery service address.")
args = parser.parse_args()

# ZMQ context and sockets
context = zmq.Context()

# Connect to the discovery service
discovery_socket = context.socket(zmq.REQ)
discovery_socket.connect(args.discovery_address)

# Register the publisher with the discovery service
def register_publisher():
    message = {
        'type': 'register',
        'entity': 'publisher',
        'topic': args.topic,
        'address': args.address
    }
    discovery_socket.send_json(message)
    response = discovery_socket.recv_json()
    print(f"Publisher registered: {response}")

# Publisher socket to send messages
publisher_socket = context.socket(zmq.PUB)
publisher_socket.bind(args.address)

# Simulate publishing messages
def publish_messages():
    while True:
        message = f"Message from publisher on topic {args.topic}"
        publisher_socket.send_string(f"{args.topic} {message}")
        print(f"Published: {message}")
        time.sleep(1)

# Main loop
if __name__ == '__main__':
    register_publisher()
    publish_messages()
