from confluent_kafka import Consumer
import json
import psycopg2

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'footfall_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['footfall_topic'])


def insert_into_postgres(data):
    try:
        connection = psycopg2.connect(
            host="localhost",
            database="ff_system",
            user="postgres",
            password="Tharakast@4123"
        )

        cursor = connection.cursor()
        
        if data.get("zone") == "shelf_area":
            table_name = "shelf_area"
        else:
            table_name = "main_entrance"

        insert_query = f"""
        INSERT INTO {table_name} 
        (date, time, object_id, direction, crowd, position, entry_exit, 
         time_difference_str, time_difference_sec, frames_num, actual_time, store_area)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(insert_query, (
            data.get("date"),
            data.get("time"),
            data.get("object_id"),
            data.get("direction"),
            data.get("crowd"),
            data.get("position"),
            data.get("entry_exit"),
            data.get("time_difference_str"),
            data.get("time_difference_sec"),
            data.get("frames_num"),
            data.get("actual_time"),
            data.get("store_area")
        ))

        connection.commit()
        cursor.close()
        connection.close()

        print("Inserted into PostgreSQL")

    except Exception as e:
        print("DB Error:", e)


while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error:", msg.error())
        continue

    data = json.loads(msg.value().decode('utf-8'))
    print("Received:", data)

    insert_into_postgres(data)