import csv
import pika
import json
import os
import time

def rabit_producer():
    credentials = pika.PlainCredentials('guest', 'guest')
    rabbit_host = os.environ.get('RABBITMQ_HOST', 'rabbit')
    parameters = pika.ConnectionParameters(
        host=rabbit_host,
        port=5672,
        credentials=credentials
    )

    max_retries = 10
    retry_delay = 3
    connection = None

    for attempt in range(1, max_retries + 1):
        try:
            print(f"Connecting to RabbitMQ at {rabbit_host} (Attempt {attempt}/{max_retries})...")
            connection = pika.BlockingConnection(parameters)
            print("Connected successfully!")
            break
        except pika.exceptions.AMQPConnectionError as e:
            if attempt == max_retries:
                print("Could not connect to RabbitMQ, max retries reached.")
                raise e
            print(f"Connection failed. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

    channel = connection.channel()

    queue_name = 'csv_rows_queue'
    channel.queue_declare(queue=queue_name, durable=True)

    csv_file_path = "/app/Data/data.csv"

    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)

        for row in csv_reader:
            message = json.dumps(row)

            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2
                )
            )
            print(f"[x] Sent row: {message}")

    connection.close()
    print("All rows have been sent successfully!")

if __name__ == "__main__":
    rabit_producer()