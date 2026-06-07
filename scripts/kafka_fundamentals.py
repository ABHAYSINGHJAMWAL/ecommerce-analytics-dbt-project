"""
Kafka Fundamentals for Data Engineering

Demonstrates the complete producer-consumer pattern
for real-time data pipeline development.

Architecture:
Order Service → Kafka Topic → Analytics Consumer → BigQuery

Why this matters:
Real-time pipelines at Swiggy, Zepto, and Razorpay
all use Kafka as the backbone for event streaming.
"""

import json
import time
import random
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
ORDER_TOPIC = 'order-placed'
PAYMENT_TOPIC = 'payment-processed'
INVENTORY_TOPIC = 'inventory-update'


# ════════════════════════════════════════════
# DATA MODELS
# Why dataclasses:
# Type safety — catches wrong field names at runtime
# Serialization — asdict() converts to dict for JSON
# Documentation — fields clearly defined with types
# ════════════════════════════════════════════

@dataclass
class OrderEvent:
    """
    Represents one order placed by a customer.

    Why this structure:
    Every field answers a business question.
    order_id: which order?
    customer_id: who placed it?
    seller_id: who is fulfilling it?
    amount: how much revenue?
    city: which market?
    payment_method: how did they pay? (fraud detection)
    timestamp: when? (for time-series analysis)
    """
    order_id: str
    customer_id: str
    seller_id: str
    product_id: str
    amount: float
    currency: str
    city: str
    state: str
    payment_method: str
    status: str
    timestamp: str
    event_type: str = 'order_placed'

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, json_str: str) -> 'OrderEvent':
        return cls(**json.loads(json_str))


@dataclass
class PaymentEvent:
    """Represents a payment processing result"""
    payment_id: str
    order_id: str
    amount: float
    method: str
    status: str  # success, failed, pending
    gateway: str  # razorpay, paytm, phonepe
    timestamp: str
    event_type: str = 'payment_processed'

    def to_json(self) -> str:
        return json.dumps(asdict(self))


# ════════════════════════════════════════════
# DATA GENERATOR
# Why realistic fake data:
# Teaches you what real Indian e-commerce data looks like
# Useful for testing without real production access
# Shows interviewers you understand the domain
# ════════════════════════════════════════════

class IndianEcommerceDataGenerator:
    """
    Generates realistic Indian e-commerce events.

    In production: replaced by real application events.
    For learning: gives you realistic data to process.
    """

    CITIES = {
        'Mumbai': 'Maharashtra',
        'Delhi': 'Delhi',
        'Bangalore': 'Karnataka',
        'Chennai': 'Tamil Nadu',
        'Kolkata': 'West Bengal',
        'Hyderabad': 'Telangana',
        'Pune': 'Maharashtra',
        'Jaipur': 'Rajasthan',
        'Ahmedabad': 'Gujarat',
        'Surat': 'Gujarat'
    }

    PAYMENT_METHODS = ['UPI', 'Credit Card', 'Debit Card', 'Net Banking', 'Cash on Delivery']
    PAYMENT_WEIGHTS = [0.45, 0.20, 0.15, 0.10, 0.10]

    PRODUCT_CATEGORIES = ['Fashion', 'Electronics', 'Home', 'Beauty', 'Sports']

    GATEWAYS = ['Razorpay', 'PayU', 'CCAvenue', 'Instamojo']

    def generate_order(self) -> OrderEvent:
        """Generate one realistic order event"""
        city = random.choice(list(self.CITIES.keys()))
        payment = random.choices(
            self.PAYMENT_METHODS,
            weights=self.PAYMENT_WEIGHTS
        )[0]
        category = random.choice(self.PRODUCT_CATEGORIES)

        # Amount varies by category — realistic
        amount_ranges = {
            'Fashion': (299, 4999),
            'Electronics': (999, 49999),
            'Home': (499, 9999),
            'Beauty': (199, 2999),
            'Sports': (399, 14999)
        }
        min_amt, max_amt = amount_ranges[category]

        return OrderEvent(
            order_id=f"ORD-{city[:3].upper()}-{random.randint(100000, 999999)}",
            customer_id=f"CUST-{random.randint(1000, 99999):05d}",
            seller_id=f"SELL-{random.randint(100, 9999):04d}",
            product_id=f"PROD-{category[:3].upper()}-{random.randint(1000, 99999)}",
            amount=round(random.uniform(min_amt, max_amt), 2),
            currency='INR',
            city=city,
            state=self.CITIES[city],
            payment_method=payment,
            status='placed',
            timestamp=datetime.now(timezone.utc).isoformat()
        )

    def generate_payment(self, order: OrderEvent) -> PaymentEvent:
        """Generate payment event for an order"""
        # 95% success rate — realistic for Indian payments
        status = random.choices(
            ['success', 'failed', 'pending'],
            weights=[0.95, 0.03, 0.02]
        )[0]

        return PaymentEvent(
            payment_id=f"PAY-{random.randint(1000000, 9999999)}",
            order_id=order.order_id,
            amount=order.amount,
            method=order.payment_method,
            status=status,
            gateway=random.choice(self.GATEWAYS),
            timestamp=datetime.now(timezone.utc).isoformat()
        )


# ════════════════════════════════════════════
# PRODUCER
# ════════════════════════════════════════════

class OrderEventProducer:
    """
    Publishes order events to Kafka topics.

    Why a class not a function:
    Producer maintains a connection to Kafka.
    Opening and closing connection per message is expensive.
    Class keeps connection open, reuses it for all messages.

    This is the connection pooling pattern — same reason
    database clients are kept open across queries.
    """

    def __init__(self, bootstrap_servers: List[str]):
        from kafka import KafkaProducer

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,

            # Serialize Python dict to JSON bytes
            # Kafka stores bytes — you choose the format
            # JSON is readable; Avro is faster; Protobuf is most efficient
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),

            # Key serializer for partitioning
            # Messages with same key go to same partition
            # Why: all events for one order_id stay ordered
            key_serializer=lambda k: k.encode('utf-8') if k else None,

            # Wait for all replicas to acknowledge
            # acks='all' = strongest durability guarantee
            # acks=1 = faster but risks data loss if broker crashes
            acks='all',

            # Retry on transient failures
            retries=3,

            # Batch messages for efficiency
            # linger_ms=10 means wait 10ms to batch messages together
            # Reduces network round trips when traffic is high
            linger_ms=10,
        )
        logger.info(f"Producer connected to Kafka: {bootstrap_servers}")

    def publish_order(self, order: OrderEvent) -> None:
        """
        Publish one order event to the order-placed topic.

        Why use order_id as the key:
        Kafka partitions messages by key.
        All events for the same order go to the same partition.
        This ensures events for one order are processed in order.
        Without a key, Kafka distributes messages round-robin —
        events for one order might be on different partitions,
        processed out of order.
        """
        future = self.producer.send(
            topic=ORDER_TOPIC,
            key=order.order_id,
            value=asdict(order)
        )

        # Block until message is confirmed
        record_metadata = future.get(timeout=10)

        logger.info(
            f"Published order {order.order_id} "
            f"→ topic={record_metadata.topic} "
            f"partition={record_metadata.partition} "
            f"offset={record_metadata.offset}"
        )

    def publish_payment(self, payment: PaymentEvent) -> None:
        """Publish payment event"""
        self.producer.send(
            topic=PAYMENT_TOPIC,
            key=payment.order_id,
            value=asdict(payment)
        )
        logger.info(f"Published payment {payment.payment_id} for order {payment.order_id}")

    def publish_batch(self, orders: List[OrderEvent]) -> None:
        """
        Publish multiple orders efficiently.

        Why batch publishing:
        Sending messages one by one = one network round trip each
        Batch sending = fewer network round trips
        At 10,000 orders/minute batching is critical for performance

        DSA concept: this is the same optimization as
        processing arrays in chunks instead of one element at a time
        """
        for order in orders:
            self.producer.send(
                topic=ORDER_TOPIC,
                key=order.order_id,
                value=asdict(order)
            )

        # Flush ensures all buffered messages are sent
        self.producer.flush()
        logger.info(f"Published batch of {len(orders)} orders")

    def close(self):
        """Always close the producer when done"""
        self.producer.close()
        logger.info("Producer closed")


# ════════════════════════════════════════════
# CONSUMER
# ════════════════════════════════════════════

class OrderEventConsumer:
    """
    Consumes and processes order events from Kafka.

    Why consumer groups:
    group_id='analytics-pipeline' means multiple instances
    of this consumer share the work automatically.
    Add more consumers to scale horizontally.
    Kafka handles the partition assignment — no coordination code needed.
    """

    def __init__(
        self,
        bootstrap_servers: List[str],
        group_id: str,
        topics: List[str]
    ):
        from kafka import KafkaConsumer

        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,

            # Deserialize JSON bytes to Python dict
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,

            # Where to start reading when consumer group is new
            # earliest = from the beginning of topic history
            # latest = only new messages from now
            # For analytics: earliest to process all historical data
            # For real-time alerts: latest to only process new events
            auto_offset_reset='earliest',

            # Manual commit instead of automatic
            # Why: automatic commit marks message as processed
            #      even if your processing code crashed
            # Manual commit: you commit AFTER successful processing
            # This ensures exactly-once processing semantics
            enable_auto_commit=False,

            # How long to wait for new messages before returning
            consumer_timeout_ms=5000,
        )

        self.group_id = group_id
        self.processed_count = 0
        self.failed_count = 0
        logger.info(
            f"Consumer '{group_id}' subscribed to {topics}"
        )

    def process_order(self, order_data: Dict[str, Any]) -> bool:
        """
        Process one order event.

        Returns True if successful, False if failed.

        Why return bool instead of raising exception:
        Failed processing should not stop the consumer.
        Log the failure, send to dead letter queue,
        continue processing next message.
        Stopping on every error means one bad message
        blocks all subsequent messages.
        """
        try:
            # Validate required fields
            required = ['order_id', 'customer_id', 'amount', 'city']
            for field in required:
                if field not in order_data:
                    logger.error(f"Missing required field: {field}")
                    return False

            # Business logic validation
            if order_data['amount'] <= 0:
                logger.error(
                    f"Invalid amount {order_data['amount']} "
                    f"for order {order_data['order_id']}"
                )
                return False

            # In production: write to BigQuery, update dashboard, trigger downstream
            logger.info(
                f"Processed order {order_data['order_id']} "
                f"| City: {order_data['city']} "
                f"| Amount: ₹{order_data['amount']:,.2f} "
                f"| Payment: {order_data['payment_method']}"
            )

            return True

        except Exception as e:
            logger.error(f"Failed to process order: {e}")
            return False

    def run(self, max_messages: Optional[int] = None) -> Dict[str, int]:
        """
        Main consumer loop.

        Why this structure:
        1. Poll for messages (blocking with timeout)
        2. Process each message
        3. Commit offset only after successful processing
        4. Handle errors without stopping the loop

        This is the standard consumer pattern used in production.
        """
        logger.info(f"Consumer starting. Max messages: {max_messages or 'unlimited'}")

        try:
            for message in self.consumer:
                # message contains:
                # message.topic: which topic
                # message.partition: which partition
                # message.offset: position in partition
                # message.key: message key
                # message.value: the actual data (already deserialized)

                logger.info(
                    f"Received message "
                    f"topic={message.topic} "
                    f"partition={message.partition} "
                    f"offset={message.offset}"
                )

                success = self.process_order(message.value)

                if success:
                    # Commit offset — marks this message as processed
                    # If consumer crashes after this, it restarts from next message
                    # If consumer crashes before this, it reprocesses this message
                    self.consumer.commit()
                    self.processed_count += 1
                else:
                    # In production: send to dead letter topic
                    # Dead letter queue = where failed messages go for investigation
                    self.failed_count += 1
                    logger.warning(
                        f"Message sent to dead letter queue: "
                        f"offset={message.offset}"
                    )

                if max_messages and self.processed_count >= max_messages:
                    logger.info(f"Reached max messages limit: {max_messages}")
                    break

        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.close()

        return {
            'processed': self.processed_count,
            'failed': self.failed_count
        }

    def close(self):
        """Always close consumer gracefully"""
        self.consumer.close()
        logger.info(
            f"Consumer closed. "
            f"Processed: {self.processed_count}, "
            f"Failed: {self.failed_count}"
        )


# ════════════════════════════════════════════
# REAL-TIME AGGREGATOR
# This is where streaming analytics happens
# ════════════════════════════════════════════

class RealTimeMetricsAggregator:
    """
    Maintains real-time metrics as events flow in.

    This is the streaming analytics layer.
    Instead of batch queries on historical data,
    metrics update with every event.

    DSA concepts used:
    - HashMap for city-level revenue tracking
    - Sliding window for rolling metrics
    - Priority queue for top-k sellers
    - Running sum for cumulative metrics

    All of these are LeetCode patterns in production context.
    """

    def __init__(self):
        # HashMap — O(1) lookup for city revenue
        # LeetCode pattern: Two Sum, Group Anagrams
        self.city_revenue: Dict[str, float] = {}
        self.city_order_count: Dict[str, int] = {}

        # HashMap for seller performance
        self.seller_revenue: Dict[str, float] = {}

        # Payment method distribution
        self.payment_counts: Dict[str, int] = {}

        # Running totals
        self.total_revenue: float = 0.0
        self.total_orders: int = 0

        # Sliding window for last N orders
        # LeetCode pattern: Sliding Window Maximum
        self.recent_amounts: List[float] = []
        self.window_size = 100

        logger.info("RealTimeMetricsAggregator initialized")

    def process_event(self, order: Dict[str, Any]) -> None:
        """
        Update all metrics with one new order event.
        O(log k) for top-k update, O(1) for everything else.
        """
        city = order['city']
        seller = order['seller_id']
        amount = order['amount']
        payment = order['payment_method']

        # Update city metrics — HashMap O(1)
        self.city_revenue[city] = self.city_revenue.get(city, 0) + amount
        self.city_order_count[city] = self.city_order_count.get(city, 0) + 1

        # Update seller metrics
        self.seller_revenue[seller] = self.seller_revenue.get(seller, 0) + amount

        # Update payment distribution
        self.payment_counts[payment] = self.payment_counts.get(payment, 0) + 1

        # Update totals
        self.total_revenue += amount
        self.total_orders += 1

        # Sliding window — O(1) append, O(1) remove from front
        # LeetCode pattern: Sliding Window
        self.recent_amounts.append(amount)
        if len(self.recent_amounts) > self.window_size:
            self.recent_amounts.pop(0)

    def get_top_cities(self, k: int = 5) -> List[Dict]:
        """
        Get top k cities by revenue.

        DSA: Heap-based top-k — O(n log k)
        LeetCode pattern: Top K Frequent Elements, Kth Largest

        Why heap not sort:
        Sort is O(n log n) — processes all cities
        Heap is O(n log k) — k << n for top-5 from 1000 cities
        At scale (millions of cities) this matters enormously
        """
        import heapq

        # Build (revenue, city) tuples for heap
        city_revenues = [
            (revenue, city)
            for city, revenue in self.city_revenue.items()
        ]

        # nlargest uses heap internally — O(n log k)
        top_k = heapq.nlargest(k, city_revenues)

        return [
            {
                'city': city,
                'revenue': revenue,
                'orders': self.city_order_count[city],
                'avg_order_value': revenue / self.city_order_count[city]
            }
            for revenue, city in top_k
        ]

    def get_rolling_average(self) -> float:
        """
        Average order value over last window_size orders.

        DSA: Sliding window average — O(1)
        LeetCode pattern: Moving Average from Data Stream
        """
        if not self.recent_amounts:
            return 0.0
        return sum(self.recent_amounts) / len(self.recent_amounts)

    def get_payment_distribution(self) -> List[Dict]:
        """Payment method breakdown as percentages"""
        total = sum(self.payment_counts.values())
        if total == 0:
            return []

        return sorted([
            {
                'method': method,
                'count': count,
                'percentage': round(count / total * 100, 1)
            }
            for method, count in self.payment_counts.items()
        ], key=lambda x: -x['count'])

    def print_dashboard(self) -> None:
        """Print current real-time metrics"""
        print("\n" + "="*50)
        print("REAL-TIME ANALYTICS DASHBOARD")
        print("="*50)
        print(f"Total Orders: {self.total_orders:,}")
        print(f"Total Revenue: ₹{self.total_revenue:,.2f}")
        print(f"Rolling Avg (last {self.window_size}): ₹{self.get_rolling_average():,.2f}")

        print("\nTOP CITIES BY REVENUE:")
        for i, city_data in enumerate(self.get_top_cities(5), 1):
            print(
                f"  {i}. {city_data['city']}: "
                f"₹{city_data['revenue']:,.0f} "
                f"({city_data['orders']} orders)"
            )

        print("\nPAYMENT DISTRIBUTION:")
        for p in self.get_payment_distribution():
            bar = "█" * int(p['percentage'] / 5)
            print(f"  {p['method']:20} {bar} {p['percentage']}%")
        print("="*50)


# ════════════════════════════════════════════
# SIMULATION — runs without real Kafka
# ════════════════════════════════════════════

def simulate_streaming_pipeline(num_events: int = 500):
    """
    Simulates complete streaming pipeline without Kafka.

    Why simulation:
    Kafka requires running containers.
    This lets you practice the patterns and see real output
    without infrastructure setup.
    When Kafka is available, replace simulate_event_stream()
    with real KafkaConsumer — same processing logic.
    """
    logger.info(f"=== STREAMING PIPELINE SIMULATION ({num_events} events) ===\n")

    generator = IndianEcommerceDataGenerator()
    aggregator = RealTimeMetricsAggregator()

    start_time = time.time()
    failed = 0

    for i in range(num_events):
        # Generate event
        order = generator.generate_order()
        order_dict = asdict(order)

        # Simulate processing delay
        # In real Kafka: this is network + deserialization time
        time.sleep(0.001)

        # Validate
        if order_dict['amount'] <= 0:
            failed += 1
            continue

        # Update real-time metrics
        aggregator.process_event(order_dict)

        # Print dashboard every 100 events
        if (i + 1) % 100 == 0:
            aggregator.print_dashboard()
            elapsed = time.time() - start_time
            throughput = (i + 1) / elapsed
            logger.info(f"Throughput: {throughput:.0f} events/second")

    # Final dashboard
    aggregator.print_dashboard()

    elapsed = time.time() - start_time
    logger.info(f"\n=== SIMULATION COMPLETE ===")
    logger.info(f"Events processed: {num_events - failed}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Total time: {elapsed:.2f}s")
    logger.info(f"Throughput: {num_events/elapsed:.0f} events/second")


def run_with_real_kafka():
    """
    Run the actual Kafka producer and consumer.
    Requires docker-compose up to be running.
    """
    generator = IndianEcommerceDataGenerator()

    # Start producer in background thread
    import threading

    def produce_orders():
        producer = OrderEventProducer(KAFKA_BOOTSTRAP_SERVERS)
        try:
            for i in range(50):
                order = generator.generate_order()
                producer.publish_order(order)
                time.sleep(0.1)
        finally:
            producer.close()

    producer_thread = threading.Thread(target=produce_orders)
    producer_thread.start()

    # Consume with real-time aggregation
    aggregator = RealTimeMetricsAggregator()
    consumer = OrderEventConsumer(
        KAFKA_BOOTSTRAP_SERVERS,
        group_id='analytics-pipeline',
        topics=[ORDER_TOPIC]
    )

    logger.info("Starting real Kafka consumer...")
    results = consumer.run(max_messages=50)

    producer_thread.join()
    logger.info(f"Results: {results}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == '--real-kafka':
        logger.info("Running with real Kafka (requires docker-compose up)")
        run_with_real_kafka()
    else:
        logger.info("Running simulation (no Kafka needed)")
        simulate_streaming_pipeline(num_events=500)