import tweepy
from kafka import KafkaProducer
import logging
# Accesing keys
consumerKey = "UTcuIbGaNqpucsc9JJwaSHwvy"
consumerSecret = "wI54P34UT5uZN18QM0wiepAgBSvJtFPoBJJgg6CYm6F7u5abXw"
accessToken = "2292420018-Dozw5QBjCBhuTtbSjRcW7nvuTK5LjSTGCQbVPCF"
accessTokenSecret = "UxQob4OFLkEuP02v2heCYwongtgdO1cGEzqCGcTHtWG6s"
# Accessing Kafka  server at localhost:9092
# producer = KafkaProducer(bootstrap_servers='localhost:9092') 
producer = KafkaProducer(bootstrap_servers='localhost:9092')
search_term = 'Bitcoin'
topic_name = 'topic1'


def twitterAuth():
    # create the authentication object
    authenticate = tweepy.OAuthHandler(consumerKey, consumerSecret)
    # set the access token and the access token secret
    authenticate.set_access_token(accessToken, accessTokenSecret)
    # create the API object
    api = tweepy.API(authenticate, wait_on_rate_limit=True)
    return api


class TweetListener(tweepy.Stream):

    def on_data(self, raw_data):
        logging.info(raw_data)
        producer.send(topic_name, value=raw_data)
        return True

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False

    def start_streaming_tweets(self, search_term):
        self.filter(track=search_term, stall_warnings=True, languages=["en"])


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    twitter_stream = TweetListener(consumerKey, consumerSecret, accessToken, accessTokenSecret)
    twitter_stream.start_streaming_tweets(search_term)


# # For getting twitter api
# import tweepy
# # To introduce kakfa producer concept
# from kafka import KafkaProducer
# # For releasing logs 
# import logging
# # For autheniticating the access keys
# from tweepy import OAuthHandler
# # For streaming data from Tweepy
# from tweepy import Stream
# # Getting credentials from stored python file 'securitykeys' 
# from securitykeys import consumer_key,consumer_secret,access_token,access_token_secret
# # Accesing keys
# consumer_key = consumer_key
# consumer_secret = consumer_secret
# access_token = access_token
# access_token_secret = access_token_secret
# # Accessing Kafka  server at localhost:9092
# producer = KafkaProducer(bootstrap_servers='localhost:9092') 
# # our cluster name is "top"
# topic_name = "top"
# class twitterauth():
# # Setting up twitter
#     def authenticateTwitterApp(self):
#         auth = OAuthHandler(consumer_key, consumer_secret)
#         auth.set_access_token(access_token, access_token_secret)
#         return auth
# class twitterstream():
# # Streaming data
#     def __init__(self):
#         self.twitterAuth = twitterauth()
#     def stream_tweets(self):
#         while True:
#             listener_data = ListenerTS('consumer_key', 'consumer_secret', 'access_token','access_token_secret') 
#             auth = self.twitterAuth.authenticateTwitterApp()
#             #stream = Stream(auth, listener)
#             listener_data.filter(track=["dollar8"], stall_warnings=True, languages= ["en"])
# class ListenerTS(tweepy.Stream):
#     def on_data(self, raw_data):
#             producer.send(topic_name, str.encode(raw_data))
#             return True
# if __name__ == "__main__":
#     TS = twitterstream()
#     TS.stream_tweets()