from kafka import KafkaConsumer
import json

topic_name = "Twitter_Stream_Cleaned"

consumer = KafkaConsumer(
    topic_name,
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     auto_commit_interval_ms =  5000,
     fetch_max_bytes = 128,
     max_poll_records = 100,
     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)



if __name__ == '__main__':
    print('Retrieving Twitter_Stream_Cleaned Topic...')
    for message in consumer:
        print(message)