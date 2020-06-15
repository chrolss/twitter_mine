import tweepy
import json
import joblib
import datetime
import os

class StreamListenerHDFS(tweepy.StreamListener):
    # A stream listener that is supposed to catch statuses and write them to a dataframe

    def __init__(self, _maxtweets, _partition_size, _filename_prefix, _root_directory):
        super(StreamListenerHDFS, self).__init__()
        self.num_tweets = 0                     # Internal tweet counter
        self.max_tweets = _maxtweets            # How many tweets are read before cancel operation
        self.tweetList = []                     # Internal temporary tweet storage before writing to .pkl
        self.root_directory = _root_directory   # The root directory of the file that calls the StreamListener
        self.foldername = ''                    # Foldername for current session where .pkl are saved
        self.partition_size = _partition_size   # Used for setting how many tweets per .pkl
        self.filename_prefix = _filename_prefix # Used for naming the saved .pkl
        self.create_HDFS_folder()               # Create the folder for saving the .pkl files
        self.file_breakpoints = []              # A list which contains the number of tweets read before breakpoint

        looper = _partition_size
        while looper < _maxtweets:
            self.file_breakpoints.append(looper)
            looper += _partition_size

    def on_status(self, status):
        # Check if max number of tweets have been received
        if self.num_tweets < self.max_tweets:
            self.num_tweets += 1
            self.tweetList.append(status._json) # Add tweets' json object to the tweet list

            print("Tweet number " + str(self.num_tweets) + " out of " + str(self.max_tweets))
            # If we have read enough tweets for a breakpoint, it's time to dump to .pkl
            if self.num_tweets in self.file_breakpoints:
                self.dump_to_pickle()   # Dump the current tweetList to .pkl
                self.tweetList.clear()  # Empty the list of tweets

            return True

        else:
            # Write to pkl instead of .txt as it seems more robust in write/read operations
            if len(self.tweetList) > 0:     # If our tweet list contains some tweets, make sure to dump before exit
                self.dump_to_pickle()       # Dump the tweets in a pickle

            print("Read all tweets")
            return False

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False

        # returning non-False reconnects the stream, with backoff.

    def dump_to_pickle(self):
        dts = datetime.datetime.now()   # datetimestamp
        filename = self.filename_prefix + '_' + str(dts.year) + str(dts.month) + str(dts.day) + '-' + str(dts.hour) \
                   + str(dts.minute) + str(dts.second) + str(dts.microsecond) + '.pkl'
        joblib.dump(self.tweetList, self.foldername + '/' + filename)


    def create_HDFS_folder(self):
        dts = datetime.datetime.now()  # datetimestamp
        self.foldername = 'StreamReading_' + str(dts.year) + str(dts.month) + str(dts.day) + '-' + str(dts.hour) \
                   + str(dts.minute) + str(dts.second)

        dirname = os.path.dirname(__file__) # Current directory
        try:
            os.mkdir(self.foldername)
            print("Created folder: ", self.foldername)
        except OSError:
            print("Unable to create directory - possibly already exists?")
