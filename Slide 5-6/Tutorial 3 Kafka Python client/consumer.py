from kafka import KafkaConsumer

topic_name="items"

#this is streaming mode
consumer=KafkaConsumer(topic_name)
for message in consumer:
    print(message.value)