from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
    
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    positive_count = []
    negetive_count = []
    
    for count in counts:
        for word in count:
            if word[0] == "positive":
                positive_count.append(word[1])
            else:
                negetive_count.append(word[1])
                
    plt.axis([-1, len(positive_count), 0 , max(max(positive_count), max(negetive_count))+5000])
    pos, = plt.plot(positive_count, 'y:', marker ='x', markersize =10)
    neg, = plt.plot(negetive_count, 'r:', marker ='x', markersize =10)
    plt.legend((pos,neg),('Positive', 'negative'), loc=2)
    plt.xticks(np.arange(0, len(positive_count),1))
    plt.xlabel('Time')
    plt.ylabel('Word Count')
    plt.show()



def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    file = open(filename, 'r')
    words =[]
    for x in file:
        words.append(x.split("\n")[0])
    return set(words)
    # YOUR CODE HERE

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])
    tweets = tweets.flatMap(lambda x:x.split(" "))
    tweets_pos = tweets.filter(lambda word:word in pwords)
    tweets_neg = tweets.filter(lambda word:word in nwords)
    tweets = tweets_pos.union(tweets_neg)
    tweets = tweets.map(lambda word: ('positive', 1) if (word in pwords) else ('negative', 1))
    
    tweets = tweets.reduceByKey(lambda x, y : x+y)
    totalCount = tweets.updateStateByKey(updateFunction)
    totalCount.pprint()

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    tweets.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)
    return counts


if __name__=="__main__":
    main()
