###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Discovery application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Discovery service is a server
# and hence only responds to requests. It should be able to handle the register,
# is_ready, the different variants of the lookup methods. etc.
#
# The key steps for the discovery application are
# (1) parse command line and configure application level parameters. One
# of the parameters should be the total number of publishers and subscribers
# in the system.
# (2) obtain the discovery middleware object and configure it.
# (3) since we are a server, we always handle events in an infinite event loop.
# See publisher code to see how the event loop is written. Accordingly, when a
# message arrives, the middleware object parses the message and determines
# what method was invoked and then hands it to the application logic to handle it
# (4) Some data structure or in-memory database etc will need to be used to save
# the registrations.
# (5) When all the publishers and subscribers in the system have registered with us,
# then we are in a ready state and will respond with a true to is_ready method. Until then
# it will be false.
###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Discovery application
#
# Created: Spring 2023
#
###############################################

import argparse
import zmq
import json

# This is left as an exercise for the student. The Discovery service is a server
# and hence only responds to requests. It should be able to handle the register,
# is_ready, the different variants of the lookup methods, etc.
#
# The key steps for the discovery application are:
# (1) Parse command line and configure application level parameters. One
# of the parameters should be the total number of publishers and subscribers
# in the system.
# (2) Obtain the discovery middleware object and configure it.
# (3) Since we are a server, we always handle events in an infinite event loop.
# See publisher code to see how the event loop is written. Accordingly, when a
# message arrives, the middleware object parses the message and determines
# what method was invoked and then hands it to the application logic to handle it.
# (4) Some data structure or in-memory database, etc., will need to be used to save
# the registrations.
# (5) When all the publishers and subscribers in the system have registered with us,
# then we are in a ready state and will respond with a true to the is_ready method.
# Until then, it will be false.

# Command-line arguments
parser = argparse.ArgumentParser(description="Discovery Service for Pub/Sub system.")
parser.add_argument('--num_publishers', type=int, required=True, help="Total number of publishers in the system.")
parser.add_argument('--num_subscribers', type=int, required=True, help="Total number of subscribers in the system.")
parser.add_argument('--bind_address', type=str, default="tcp://*:5555", help="Address to bind the discovery server.")
args = parser.parse_args()

NUM_PUBLISHERS = args.num_publishers
NUM_SUBSCRIBERS = args.num_subscribers
BIND_ADDRESS = args.bind_address

# Set up the ZMQ context and socket
context = zmq.Context()

# Create a REP socket for receiving requests
discovery_socket = context.socket(zmq.REP)
discovery_socket.bind(BIND_ADDRESS)

# Data structure to store registered publishers and subscribers
# This registry will store the addresses of publishers and subscribers for each topic
registry = {'publishers': {}, 'subscribers': {}}

def handle_register(message):
    """
    Handle the 'register' request.
    This function registers either a publisher or a subscriber by adding it
    to the respective category (publishers or subscribers) in the registry.

    Args:
        message (dict): The incoming message containing details for registration.

    Returns:
        dict: A response confirming the successful registration.
    """
    entity = message['entity']
    topic = message['topic']
    address = message['address']
    
    # Register the entity (publisher or subscriber) in the registry
    if entity == 'publisher':
        registry['publishers'][topic] = address
    elif entity == 'subscriber':
        registry['subscribers'][topic] = address
    
    return {"status": "success", "entity": entity, "topic": topic, "address": address}

def handle_is_ready():
    """
    Handle the 'is_ready' request.
    This function checks if all the publishers and subscribers have registered.
    If the number of publishers and subscribers is sufficient, it returns "ready".

    Returns:
        dict: A response indicating whether the system is ready or not.
    """
    ready = len(registry['publishers']) >= NUM_PUBLISHERS and len(registry['subscribers']) >= NUM_SUBSCRIBERS
    return {"status": "ready" if ready else "not_ready"}

def handle_lookup(message):
    """
    Handle the 'lookup' request.
    This function checks if a publisher is registered for a given topic and
    returns the publisher's address if found.

    Args:
        message (dict): The incoming message containing the topic to be looked up.

    Returns:
        dict: A response containing the publisher's address or indicating not found.
    """
    topic = message['topic']
    publisher_address = registry['publishers'].get(topic)
    
    if publisher_address:
        return {"status": "found", "address": publisher_address}
    else:
        return {"status": "not_found"}

# Event loop to handle requests
while True:
    try:
        # Receive the message from the client
        message = discovery_socket.recv_json()
        print(f"Received message: {message}")  # Log the incoming message

        msg_type = message.get('type')  # Extract the type of request
        
        # Handle the different message types: register, is_ready, lookup
        if msg_type == 'register':
            response = handle_register(message)
            print(f"Registry after registration: {registry}")  # Log the updated registry
        elif msg_type == 'is_ready':
            response = handle_is_ready()
        elif msg_type == 'lookup':
            response = handle_lookup(message)
        else:
            # Unknown message type
            response = {"status": "error", "message": "Unknown message type"}
        
        # Send the response back to the client
        discovery_socket.send_json(response)
    
    except zmq.ZMQError as e:
        print(f"ZMQError occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
