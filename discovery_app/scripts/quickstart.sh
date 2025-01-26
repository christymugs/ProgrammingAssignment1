python3 DiscoveryAppIn.py --num_publishers 1 --num_subscribers 1 --bind_address "tcp://127.0.0.1:5555" &
python3 PublisherAppIn.py --topic "weather" --address "tcp://127.0.0.1:5556" &
python3 SubscriberAppIn.py --topic "weather" &