import json
import time
from confluent_kafka import Consumer, KafkaException
import duckdb

# Kafka consumer configuration
conf = {
    "bootstrap.servers": "localhost:9093",
    "group.id": "debezium-consumer-group",
    "auto.offset.reset": "earliest",
}

# Create Kafka consumer
consumer = Consumer(conf)
consumer.subscribe(["debezium.commerce.products"])

conn = duckdb.connect('../duckdb2')

def display_price_changes(result):
    """Display price changes in a readable format"""
    print("\nPrice Change History:")
    print("-" * 50)
    for row in result:
        print(f"Product ID: {row[0]}")
        print(f"Name: {row[1]}")
        print(f"Current Price: ${row[3]:.2f}")
        print(f"Valid From: {row[4]}")
        print(f"Valid Until: {row[5]}")
        print("-" * 50)

# Function to print message payload
def print_payload(payload):
    print("Before:", payload["before"])
    print("After:", payload["after"])

# Consume messages from Kafka
try:
    while True:
        msg = consumer.poll(1.0)  # Timeout of 1 second

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())

        # Parse the message
        if msg.value() is not None:
            payload = json.loads(msg.value())["payload"]
            # print_payload(payload)
            id = payload["after"]["id"] if payload["after"] else payload["before"]["id"]

            # DuckDB query to track price changes
            conn.execute(f"""
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_region='us-east-1';
            SET s3_endpoint='localhost:9000';
            SET s3_access_key_id='minio';
            SET s3_secret_access_key='minio123';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
            CREATE OR REPLACE VIEW vw_products_cdc_type_2 AS
            WITH products_create_update_delete AS (
                SELECT
                    COALESCE(CAST(json->'value'->'after'->'id' AS INT), CAST(json->'value'->'before'->'id' AS INT)) AS id,
                    json->'value'->'before' AS before_row_value,
                    json->'value'->'after' AS after_row_value,
                    CASE
                        WHEN CAST(json->'value'->'$.op' AS CHAR(1)) = '"c"' THEN 'CREATE'
                        WHEN CAST(json->'value'->'$.op' AS CHAR(1)) = '"d"' THEN 'DELETE'
                        WHEN CAST(json->'value'->'$.op' AS CHAR(1)) = '"u"' THEN 'UPDATE'
                        WHEN CAST(json->'value'->'$.op' AS CHAR(1)) = '"r"' THEN 'SNAPSHOT'
                        ELSE 'INVALID'
                    END AS operation_type,
                    CAST(json->'value'->'source'->'lsn' AS BIGINT) AS log_seq_num,
                    epoch_ms(CAST(json->'value'->'source'->'ts_ms' AS BIGINT)) AS source_timestamp
                FROM
                    read_ndjson_objects('s3://commerce/debezium.commerce.products/*/*/*.json')
                WHERE
                    log_seq_num IS NOT NULL
            )
            SELECT
                id,
                CAST(after_row_value->'name' AS VARCHAR(255)) AS name,
                CAST(after_row_value->'description' AS TEXT) AS description,
                CAST(after_row_value->'price' AS DECIMAL(10,2)) AS price,
                source_timestamp AS row_valid_start_timestamp,
                CASE 
                    WHEN LEAD(source_timestamp, 1) OVER lead_txn_timestamp IS NULL THEN CAST('9999-01-01' AS TIMESTAMP) 
                    ELSE LEAD(source_timestamp, 1) OVER lead_txn_timestamp 
                END AS row_valid_expiration_timestamp
            FROM products_create_update_delete
            WHERE id in (SELECT id FROM products_create_update_delete GROUP BY id HAVING COUNT(*) > 0)
            WINDOW lead_txn_timestamp AS (PARTITION BY id ORDER BY log_seq_num)
            ORDER BY id, row_valid_start_timestamp DESC;
            select * from vw_products_cdc_type_2 where id = {id}
            """)

            result = conn.fetchall()
            display_price_changes(result)

            time.sleep(10)
            result = conn.execute("select sum(price) from vw_products_cdc_type_2 where row_valid_expiration_timestamp > now();").fetchall()
            # print(result)
            for row in result:
                print(row)

except KeyboardInterrupt:
    pass
except Exception as e:
    print(f"Error consuming messages: {e}")
finally:
    # Close the consumer
    consumer.close()
