import os
from confluent_kafka import SerializingProducer
import simplejson as json
import random
import uuid
import time
from datetime import datetime, timedelta


LONDON_COORDINATES = { "latitude": 51.5074, "longitude": -0.1278 }
BIRMINGHAM_COORDINATES = { "latitude": 52.4862, "longitude": -1.8904 }

# calculate movement increments
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["latitude"] - LONDON_COORDINATES["latitude"]) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["longitude"] - LONDON_COORDINATES["longitude"]) / 100

# environment variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VEHICLE_TOPIC = os.environ.get("VEHICLE_TOPIC", "vehicle-data")
GPS_TOPIC = os.environ.get("GPS_TOPIC", "gps-data")
TRAFFIC_TOPIC = os.environ.get("TRAFFIC_TOPIC", "traffic-data")
WEATHER_TOPIC = os.environ.get("WEATHER_TOPIC", "weather-data")
EMERGENCY_TOPIC = os.environ.get("EMERGENCY_TOPIC", "emergency-data")

random.seed(42)
start_time = datetime.now()
start_location = LONDON_COORDINATES

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time

def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "timestamp": timestamp,
        "speed": random.uniform(0, 40),
        "direction": "North-East",
        "vehicle_type": vehicle_type,
    }

def generate_traffic_data(device_id, location, timestamp):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "timestamp": timestamp,
        "location": location,
        "intensity": random.randint(0, 10),
    }

def generate_weather_data(device_id, location, timestamp):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "timestamp": timestamp,
        "location": location,
        "temperature": random.uniform(-5, 25),
        "condition": random.choice(["Sunny", "Rainy", "Cloudy", "Snowy"]),
        "preciptation": random.uniform(0, 25),
        "wind_speed": random.uniform(0, 100),
        "humidity": random.randint(0, 100),
        "air_quality": random.uniform(0, 500),
    }

def generate_emergency_data(device_id, location, timestamp):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "timestamp": timestamp,
        "location": location,
        "type": random.choice(["Accident", "Fire", "Theft", "Medical"]),
        "severity": random.randint(1, 10),
        "description": "An accident has been reported",
        "status": random.choice(["Reported", "In Progress", "Resolved"]),
    }

def simulate_vehicle_movement():
    global start_location
    # moves twards Burmingham
    start_location["latitude"] += LATITUDE_INCREMENT
    start_location["longitude"] += LONGITUDE_INCREMENT
    # add ramdomness
    start_location["latitude"] += random.uniform(-0.0005, 0.0005)
    start_location["longitude"] += random.uniform(-0.0005, 0.0005)
    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "timestamp": get_next_time().isoformat(),
        "location": (location['latitude'], location['longitude']),
        "speed": random.uniform(10, 40),
        "direction": "North-East",
        "make": "Toyota",
        "model": "Corolla",
        "year": 2019,
        "fuel_type": "Petrol",
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to: {msg.topic()} [{msg.partition()}]")

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic, 
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report 
    )
    producer.poll(0)
    producer.flush()

def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_data = generate_traffic_data(device_id, vehicle_data['location'], vehicle_data['timestamp'])
        weather_data = generate_weather_data(device_id, vehicle_data['location'], vehicle_data['timestamp'])
        emergency_data = generate_emergency_data(device_id, vehicle_data['location'], vehicle_data['timestamp'])

        if (vehicle_data['location'][0] >= BIRMINGHAM_COORDINATES['latitude']
        and vehicle_data['location'][1] >= BIRMINGHAM_COORDINATES['longitude']):
            print("Journey completed. Simulation ending...")

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_data)

        time.sleep(5)

if __name__ == "__main__":
    producer_conf = { 
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "error_cb": lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_conf)

    try:
        simulate_journey(producer, 'vehicle-1')
    
    except KeyboardInterrupt:
        print("Simulation done by the user")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")