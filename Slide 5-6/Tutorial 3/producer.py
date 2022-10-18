from kafka import KafkaProducer
topic_name='items'
kafka_sever='localhost:9092'

producer=KafkaProducer(bootstrap_servers=kafka_sever)

producer.send(topic_name, b'Hello world')
producer.flush()