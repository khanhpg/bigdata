# producer.py
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta

import pandas as pd
import numpy as np


# gen timestamp
def gen_timestamp():
    return int(datetime.now().timestamp() * 1000)

def generate_zpid():
    return random.randint(19000000, 442999999)

def generate_address():
    street_numbers = range(1000, 99999)
    street_names = ["Oak", "Maple", "Pine", "Cedar", "Elm", "Willow", "Birch", "Magnolia", "Palm", 
                   "Beverly", "Sunset", "Hollywood", "Venice", "Melrose", "Rodeo", "Mulholland",
                   "Vine", "Highland", "Laurel", "Ocean"]
    street_types = ["Ave", "St", "Blvd", "Dr", "Ln", "Way", "Pl", "Ter", "Circle"]
    
    street_number = random.choice(street_numbers)
    street_name = random.choice(street_names)
    street_type = random.choice(street_types)
    
    return f"{street_number} {street_name} {street_type}"

def generate_city():
    cities = [
        ("Los Angeles", "90001", 34.0522, -118.2437),
        ("Beverly Hills", "90210", 34.0736, -118.4004),
        ("Santa Monica", "90401", 34.0195, -118.4912),
        ("Sherman Oaks", "91403", 34.1508, -118.4489),
        ("Encino", "91316", 34.1517, -118.5214),
        ("Studio City", "91604", 34.1395, -118.3871),
        ("Woodland Hills", "91364", 34.1683, -118.6057),
        ("Pasadena", "91101", 34.1478, -118.1445),
        ("Glendale", "91201", 34.1425, -118.2551),
        ("Burbank", "91501", 34.1808, -118.3090)
    ]
    return random.choice(cities)

def generate_home_type():
    return random.choice(["SINGLE_FAMILY", "CONDO", "MANUFACTURED", "MULTI_FAMILY"])

def generate_price():
    ranges = [
        (400000, 1000000, 0.4),
        (1000001, 3000000, 0.3),
        (3000001, 10000000, 0.2),
        (10000001, 100000000, 0.1)
    ]
    
    selected_range = random.choices(ranges, weights=[r[2] for r in ranges])[0]
    return random.randint(selected_range[0], selected_range[1])

def generate_lot_area():
    return round(random.uniform(0.1, 8.0), 4)

def generate_specs():
    bedrooms = random.randint(1, 12)
    bathrooms = random.randint(1, bedrooms + 2)
    living_area = int(bedrooms * random.uniform(400, 800))
    return bedrooms, bathrooms, living_area

def generate_broker():
    brokers = [
        "Compass", "The Agency", "Coldwell Banker Realty", "Sotheby's International Realty",
        "Berkshire Hathaway HomeServices", "Keller Williams Realty", "RE/MAX Estate Properties",
        "Christie's International Real Estate", "Douglas Elliman", "Rodeo Realty"
    ]
    return random.choice(brokers)

def generate_data():
        city_info = generate_city()
        bedrooms, bathrooms, living_area = generate_specs()
        price = generate_price()
        address = generate_address()
        
        record = {
            "timestamp": gen_timestamp(),
            "zpid": generate_zpid(), #ID
            "homeStatus": "FOR_SALE",
            "detailUrl": f"https://www.zillow.com/homedetails/{address.replace(' ', '-')}-{city_info[0]}-CA-{city_info[1]}/",
            "address": f"{address}, {city_info[0]}, CA {city_info[1]}",
            "streetAddress": address,
            "city": city_info[0],
            "state": "CA",
            "country": "USA",
            "zipcode": city_info[1],
            "latitude": city_info[2] + random.uniform(-0.05, 0.05),# vi do
            "longitude": city_info[3] + random.uniform(-0.05, 0.05),#kinh do
            "homeType": generate_home_type(),
            "price": price,
            "currency": "USD",
            "zestimate": int(price * random.uniform(0.9, 1.1)),
            "rentZestimate": int(price * 0.004),
            "taxAssessedValue": int(price * 0.7),
            "lotAreaValue": generate_lot_area(), #dien tich dat
            "lotAreaUnit": "acres",
            "bathrooms": bathrooms,
            "bedrooms": bedrooms,
            "livingArea": living_area,
            "daysOnZillow": random.randint(0, 200),
            "isFeatured": random.choice([True, False]), #co phai noi bat khong
            "isPreforeclosureAuction": False,# co phai dau gia khong
            "timeOnZillow": random.randint(100000, 20000000),
            "isNonOwnerOccupied": True,# co phai chu nha khong
            "isPremierBuilder": False,# Có phải bất động sản từ nhà thầu cao cấp không
            "isZillowOwned": False, # có phải zillow sở hữu không
            "isShowcaseListing": random.choice([True, False]), # có phải danh sách noi bat không
            "imgSrc": f"https://photos.zillowstatic.com/fp/{random.randbytes(16).hex()}-p_e.jpg",
            "hasImage": True,
            "brokerName": generate_broker(), # tên môi giới
            "listingSubType.is_FSBA": True, # có phải bán chính chủ không
            "priceChange": random.choice([None, int(price * random.uniform(-0.1, 0.1))]),
            "datePriceChanged": random.choice([None, int((datetime.now() + timedelta(days=random.randint(-30, 30))).timestamp() * 1000)]),
            "openHouse": random.choice([None, f"Sat. {random.randint(4, 10)}pm-{random.randint(5,8)}pm"]),
            "priceReduction": None,
            "unit": None,
            "listingSubType.is_openHouse": True, #Có phải là danh sách nhà mở cửa xem không 
            "newConstructionType": random.choice([None, "BUILDER_SPEC", "NEW_CONSTRUCTION_TYPE_OTHER"]), # Loại xây dựng mới
            "listingSubType.is_newHome": random.choice([True, False]), #Có phải là nhà mới không
            "videoCount": random.choice([None, 0, 1, 2])
        }
    
        return record
# Khởi tạo producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Tên topic
TOPIC_NAME = "example_topic"

# Gửi dữ liệu
def send_data():
    try:
        while True:
            data = generate_data()
            producer.send(TOPIC_NAME, value=data)
            print(f"Sent data: {data}")
            time.sleep(2)  # Đợi 2 giây trước khi gửi dữ liệu tiếp
    except KeyboardInterrupt:
        producer.close()
        print("\nProducer stopped")

if __name__ == "__main__":
    send_data()