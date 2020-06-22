from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
from kafka import KafkaProducer, KafkaClient

access_token = '826868977364238336-eRJ4pNxuOynK6NxlA70GRPNlKRylk7e'
access_token_secret = 'yQIZ9AnpZE2AUqCruYBJVL5bodY65NutC0hJZveUFMKnR'
consumer_key =  'ZYqgHPcvybtBo2TiefHv9hy3A'
consumer_secret = 'iiIEsv3xgW7MQ7uSl6A69u5S37OU6zkUnZZznC9y4kDKxYy0cJ'




class TwitterListener(StreamListener):
    def on_data(self, raw_data):
        producer.send("politics-kafka", raw_data.encode('utf-8'))

        return True
    def on_error(self, status_code):
        print(status_code)

producer = KafkaProducer(value_serializer= lambda x: dumps(x).encode('utf-8'))
l = TwitterListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=["politics"])
