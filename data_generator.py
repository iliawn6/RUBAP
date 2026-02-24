#!/usr/bin/env python3
"""
Real-Time User Behavior Data Generator

Each script instance represents a single user generating behavior events.
The output is designed to be consumed by Kafka (via stdout or direct integration).

Usage:
    python data_generator.py [--user-id USER_ID] [--interval SECONDS] [--duration SECONDS]
    python data_generator.py --help
"""

import json
import random
import time
import argparse
import sys
from datetime import datetime
from typing import Dict, Optional
from enum import Enum

# Optional Kafka support
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


class EventType(Enum):
    """Types of user behavior events"""
    PAGE_VIEW = "page_view"
    CLICK = "click"
    ADD_TO_CART = "add_to_cart"
    PURCHASE = "purchase"
    SEARCH = "search"
    FILTER = "filter"
    PRODUCT_VIEW = "product_view"
    REVIEW = "review"
    SHARE = "share"


class UserBehaviorGenerator:
    """Generates user behavior events for a single user"""
    
    # Product categories and items
    PRODUCT_CATEGORIES = [
        "electronics", "clothing", "books", "home", "sports", 
        "toys", "beauty", "automotive", "food", "health"
    ]
    
    PRODUCTS = {
        "electronics": ["laptop", "smartphone", "tablet", "headphones", "smartwatch"],
        "clothing": ["shirt", "pants", "shoes", "jacket", "dress"],
        "books": ["novel", "textbook", "magazine", "ebook", "comic"],
        "home": ["furniture", "appliance", "decoration", "lighting"],
        "sports": ["bicycle", "treadmill", "yoga_mat", "dumbbells"],
        "toys": ["action_figure", "board_game", "puzzle", "doll"],
        "beauty": ["skincare", "makeup", "perfume", "haircare"],
        "automotive": ["car_parts", "accessories", "tools", "tires"],
        "food": ["snacks", "beverages", "groceries", "frozen_food"],
        "health": ["vitamins", "supplements", "fitness_tracker"]
    }
    
    # Page paths
    PAGES = [
        "/", "/home", "/products", "/products/electronics", "/products/clothing",
        "/products/books", "/cart", "/checkout", "/account", "/search"
    ]
    
    # Search terms
    SEARCH_TERMS = [
        "laptop", "phone", "shoes", "book", "gift", "sale", "discount",
        "best", "cheap", "review", "compare", "deal", "new", "popular"
    ]
    
    def __init__(self, user_id: str, interval: float = 2.0):
        """
        Initialize the generator for a single user
        
        Args:
            user_id: Unique identifier for this user
            interval: Base interval between events in seconds
        """
        self.user_id = user_id
        self.interval = interval
        
        # Event type weights (more common events have higher weights)
        self.event_weights = {
            EventType.PAGE_VIEW: 30,
            EventType.CLICK: 25,
            EventType.PRODUCT_VIEW: 15,
            EventType.SEARCH: 10,
            EventType.ADD_TO_CART: 8,
            EventType.FILTER: 5,
            EventType.PURCHASE: 3,
            EventType.REVIEW: 2,
            EventType.SHARE: 2,
        }
        
        self.total_weight = sum(self.event_weights.values())
    
    def _select_event_type(self) -> EventType:
        """Select event type based on weights"""
        rand = random.uniform(0, self.total_weight)
        cumulative = 0
        for event_type, weight in self.event_weights.items():
            cumulative += weight
            if rand <= cumulative:
                return event_type
        return EventType.PAGE_VIEW  # Fallback
    
    def _generate_product_id(self, category: Optional[str] = None) -> str:
        """Generate a product ID"""
        if category is None:
            category = random.choice(self.PRODUCT_CATEGORIES)
        product = random.choice(self.PRODUCTS.get(category, ["item"]))
        return f"{category}_{product}_{random.randint(1000, 9999)}"
    
    def _generate_event_properties(self, event_type: EventType) -> Dict:
        """Generate event-specific properties"""
        properties = {}
        
        if event_type == EventType.PAGE_VIEW:
            properties = {
                "page_path": random.choice(self.PAGES),
                "referrer": random.choice(["direct", "google", "facebook", "twitter", "email"])
            }
        
        elif event_type == EventType.CLICK:
            properties = {
                "element_type": random.choice(["button", "link", "image", "product_card"]),
                "element_id": f"elem_{random.randint(1, 1000)}",
                "page_path": random.choice(self.PAGES)
            }
        
        elif event_type == EventType.PRODUCT_VIEW:
            category = random.choice(self.PRODUCT_CATEGORIES)
            product_id = self._generate_product_id(category)
            properties = {
                "product_id": product_id,
                "product_name": product_id.replace("_", " ").title(),
                "category": category,
                "price": round(random.uniform(10, 500), 2)
            }
        
        elif event_type == EventType.SEARCH:
            properties = {
                "search_query": random.choice(self.SEARCH_TERMS),
                "results_count": random.randint(0, 500)
            }
        
        elif event_type == EventType.FILTER:
            properties = {
                "filter_type": random.choice(["price", "category", "rating", "brand"]),
                "filter_value": random.choice(["low", "high", "medium", "4+"])
            }
        
        elif event_type == EventType.ADD_TO_CART:
            category = random.choice(self.PRODUCT_CATEGORIES)
            product_id = self._generate_product_id(category)
            quantity = random.randint(1, 3)
            price = round(random.uniform(10, 500), 2)
            properties = {
                "product_id": product_id,
                "product_name": product_id.replace("_", " ").title(),
                "category": category,
                "quantity": quantity,
                "price": price,
                "total_value": round(price * quantity, 2)
            }
        
        elif event_type == EventType.PURCHASE:
            category = random.choice(self.PRODUCT_CATEGORIES)
            product_id = self._generate_product_id(category)
            price = round(random.uniform(10, 500), 2)
            properties = {
                "order_id": f"order_{int(time.time())}_{random.randint(1000, 9999)}",
                "product_id": product_id,
                "quantity": random.randint(1, 3),
                "price": price,
                "total_value": round(price * random.randint(1, 3), 2),
                "payment_method": random.choice(["credit_card", "debit_card", "paypal"])
            }
        
        elif event_type == EventType.REVIEW:
            properties = {
                "product_id": self._generate_product_id(),
                "rating": random.randint(1, 5)
            }
        
        elif event_type == EventType.SHARE:
            properties = {
                "platform": random.choice(["facebook", "twitter", "email", "whatsapp"]),
                "content_type": random.choice(["product", "page", "article"]),
                "content_id": self._generate_product_id()
            }
        
        return properties
    
    def generate_event(self) -> Dict:
        """Generate a single user behavior event"""
        event_type = self._select_event_type()
        
        event = {
            "event_id": f"evt_{int(time.time() * 1000000)}_{random.randint(1000, 9999)}",
            "event_type": event_type.value,
            "timestamp": datetime.now().isoformat(),
            "user_id": self.user_id,
            "properties": self._generate_event_properties(event_type)
        }
        
        return event
    
    def stream_events(
        self, 
        duration: Optional[int] = None, 
        output_file: Optional[str] = None,
        kafka_bootstrap_servers: Optional[str] = None,
        kafka_topic: Optional[str] = None
    ):
        """
        Stream events in real-time with burst pattern
        
        Pattern: After 2 normal intervals, generate 5 events immediately
        
        Args:
            duration: Duration in seconds to stream (None for infinite)
            output_file: Optional file path to write events (None for stdout)
            kafka_bootstrap_servers: Kafka bootstrap servers (e.g., 'localhost:9092')
            kafka_topic: Kafka topic name to publish events to
        """
        start_time = time.time()
        event_count = 0
        interval_count = 0
        
        # Initialize Kafka producer if requested
        kafka_producer = None
        if kafka_bootstrap_servers and kafka_topic:
            if not KAFKA_AVAILABLE:
                print("[ERROR] kafka-python library not installed. Install it with: pip install kafka-python", file=sys.stderr)
                sys.exit(1)
            try:
                kafka_producer = KafkaProducer(
                    bootstrap_servers=kafka_bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',  # Wait for all replicas to acknowledge
                    retries=3
                )
                print(f"[INFO] Connected to Kafka at {kafka_bootstrap_servers}, topic: {kafka_topic}", file=sys.stderr)
            except Exception as e:
                print(f"[ERROR] Failed to connect to Kafka: {e}", file=sys.stderr)
                sys.exit(1)
        
        # Initialize file output if specified
        output = None
        if output_file and not kafka_producer:
            output = open(output_file, 'w')
        elif not kafka_producer:
            output = sys.stdout
        
        try:
            while True:
                # Burst pattern: after 2 intervals, generate 5 events immediately
                if interval_count > 0 and interval_count % 3 == 0:
                    # Burst: generate 5 events immediately
                    for _ in range(5):
                        event = self.generate_event()
                        event_json = json.dumps(event)
                        
                        if kafka_producer:
                            # Send to Kafka
                            try:
                                kafka_producer.send(kafka_topic, event)
                            except Exception as e:
                                print(f"[ERROR] Failed to send event to Kafka: {e}", file=sys.stderr)
                                return
                        else:
                            # Write to file/stdout
                            try:
                                output.write(event_json + '\n')
                                output.flush()
                            except BrokenPipeError:
                                return
                        
                        event_count += 1
                        
                        if duration and (time.time() - start_time) >= duration:
                            return
                else:
                    # Normal: generate 1 event
                    event = self.generate_event()
                    event_json = json.dumps(event)
                    
                    if kafka_producer:
                        # Send to Kafka
                        try:
                            kafka_producer.send(kafka_topic, event)
                        except Exception as e:
                            print(f"[ERROR] Failed to send event to Kafka: {e}", file=sys.stderr)
                            return
                    else:
                        # Write to file/stdout
                        try:
                            output.write(event_json + '\n')
                            output.flush()
                        except BrokenPipeError:
                            return
                    
                    event_count += 1
                    
                    if duration and (time.time() - start_time) >= duration:
                        return
                
                interval_count += 1
                
                # Sleep for the interval
                time.sleep(self.interval)
                
                # Check duration limit
                if duration and (time.time() - start_time) >= duration:
                    return
        
        except KeyboardInterrupt:
            print(f"\n[INFO] Interrupted. Generated {event_count} events total.", file=sys.stderr)
        finally:
            if kafka_producer:
                kafka_producer.flush()
                kafka_producer.close()
            if output_file and output and output != sys.stdout:
                output.close()


def main():
    parser = argparse.ArgumentParser(
        description="Generate real-time user behavior events for a single user",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate events for user_001, 2 second intervals, output to stdout
  python data_generator.py --user-id user_001 --interval 2
  
  # Generate events for 60 seconds, save to file
  python data_generator.py --user-id user_002 --interval 2 --duration 60 --output events.jsonl
  
  # Generate events with 1 second intervals
  python data_generator.py --user-id user_003 --interval 1
  
  # Send events directly to Kafka
  python data_generator.py --user-id user_001 --interval 2 --kafka-bootstrap-servers localhost:9092 --kafka-topic user-events
        """
    )
    
    parser.add_argument(
        '--user-id',
        type=str,
        default=None,
        help='User ID for this instance (default: auto-generated)'
    )
    
    parser.add_argument(
        '--interval',
        type=float,
        default=2.0,
        help='Base interval between events in seconds (default: 2.0)'
    )
    
    parser.add_argument(
        '--duration',
        type=int,
        default=None,
        help='Duration in seconds to generate events (default: infinite)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Output file path (default: stdout, JSONL format)'
    )
    
    parser.add_argument(
        '--kafka-bootstrap-servers',
        type=str,
        default=None,
        help='Kafka bootstrap servers (e.g., localhost:9092). If set, events will be sent to Kafka instead of stdout/file'
    )
    
    parser.add_argument(
        '--kafka-topic',
        type=str,
        default='user-events',
        help='Kafka topic name (default: user-events)'
    )
    
    args = parser.parse_args()
    
    # Generate user_id if not provided
    user_id = args.user_id or f"user_{random.randint(1, 999999):06d}"
    
    generator = UserBehaviorGenerator(
        user_id=user_id,
        interval=args.interval
    )
    
    print(f"[INFO] User ID: {user_id}", file=sys.stderr)
    print(f"[INFO] Interval: {args.interval} seconds", file=sys.stderr)
    
    if args.kafka_bootstrap_servers:
        print(f"[INFO] Output: Kafka ({args.kafka_bootstrap_servers}, topic: {args.kafka_topic})", file=sys.stderr)
    else:
        print(f"[INFO] Output: {args.output or 'stdout'}", file=sys.stderr)
    
    print(f"[INFO] Duration: {args.duration or 'infinite'} seconds", file=sys.stderr)
    print(f"[INFO] Pattern: After 2 intervals, generate 5 events immediately", file=sys.stderr)
    print(f"[INFO] Press Ctrl+C to stop\n", file=sys.stderr)
    
    generator.stream_events(
        duration=args.duration, 
        output_file=args.output,
        kafka_bootstrap_servers=args.kafka_bootstrap_servers,
        kafka_topic=args.kafka_topic
    )


if __name__ == "__main__":
    main()
