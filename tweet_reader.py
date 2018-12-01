# Import the necessary package to process data in JSON format
try:
    import json
except ImportError:
    import simplejson as json

# Import the necessary methods from "twitter" library
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream, TwitterError
import re
import time,string

def read_credentials():
    file_name = "credentials.json"
    try:
        with open(file_name) as data_file:
            return json.load(data_file)
    except:
        print ("Cannot load credentials.json")
        return None



def read_tweets(access_token, access_secret, consumer_key, consumer_secret):

    oauth = OAuth(access_token, access_secret, consumer_key, consumer_secret)

    # Initiate the connection to Twitter Streaming API
    twitter_stream = TwitterStream(auth=oauth)

    # Get a sample of the public data following through Twitter
    iterator = twitter_stream.statuses.sample()
    hours = 0
    counter = 0
    start_time = time.time()
    hashtags_file = open("hashtag.csv", "a")
    text_file = open("tweets.csv", "a")




    for tweet in iterator:


        # Twitter Python Tool wraps the data returned by Twitter
        # as a TwitterDictResponse object.
        try:
            # print screen_name and name
            print("TWEET JSON: ", json.dumps(tweet, indent=4))
            counter = counter + 1

            username = tweet['user']['screen_name']
            uid = tweet['user']['id_str']
            timestamp_ms = tweet['timestamp_ms']
            #print("USER NAME: ", username)
            #print("USER ID : ", uid)
            #print("TIMESTAMP (MS): ",timestamp_ms)
            #print(counter + "\n")

            emoji_pattern = re.compile("["
                                       u"\U0001F600-\U0001F64F"
                                       u"\U0001F300-\U0001F5FF"
                                       u"\U0001F680-\U0001F6FF"
                                       u"\U0001F1E0-\U0001F1FF"
                                       "]+", flags=re.UNICODE)

            text = emoji_pattern.sub(r'', tweet['text'])
            text = text.replace(',', ' ')
            text = text.replace('\n', ' ')
            text = re.sub(r"http\S+", "", text)
            text = re.sub('@[^\s]+', '', text)
            text = text.replace('"', '')
            print("TEXT: ", text)
            if tweet['extended_tweet']['full_text']:
                text = emoji_pattern.sub(r'', tweet['extended_tweet']['full_text'])
                text = text.replace(',', ' ')
                text = text.replace('\n', ' ')
                text = re.sub(r"http\S+", "", text)
                text = re.sub('@[^\s]+', '', text)
                text = text.replace('"', '')
                #print("FULL TEXT", text)

            if tweet['entities']['hashtags']:
               # print("Hashtags: ", tweet['entities']['hashtags'])
                for text in tweet['entities']['hashtags']:
                    hashtag = text['text']
                    #print(hashtag)
                    hashtags_file.write(hashtag + "," + str(hours) + "," + timestamp_ms + '\n')

            text_file.write(str(uid) + ',' + username + ',' + text + ',' + str(hours) + ',' + timestamp_ms +'\n')
            #uid_file.write(uid + ',' + str(hours) + ',' + timestamp_ms + '\n')

        except TwitterError as e:

            # If limit is reached wait 5 minutes

            print(e)

            time.sleep(20)
            pass

        except ConnectionError as e:
            time.sleep(20)

            pass

        except TwitterHTTPError as e:

            # If limit is reached wait 5 minutes

            print(e)
            time.sleep(300)
            pass

        except KeyboardInterrupt:

            print("Program killed")

            text_file.close()
            break
        except:
            pass

        elapsed_time = time.time() - start_time

        if elapsed_time > 3600:
            text_file.flush()
            hours = hours + 1
            print("HOURS ",hours)

            start_time = time.time()

        if hours > 12:
            break




if __name__ == "__main__":
    print("Stating to read tweets")
    credentials = read_credentials()
    read_tweets(credentials['ACCESS_TOKEN'], credentials['ACCESS_SECRET'],
    credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])