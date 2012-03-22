import pycurl, json
import threading
import urllib
import urllib2
import time
import sys
import getopt
import signal


# Construct the URLs and the constants. All time values are in secs
url = 'http://localhost:8080/api/1.0/'
STREAM_URL = "https://stream.twitter.com/1/statuses/sample.json"
TIME_INTERVAL = 5     # time gap between the results    
NO_OF_WORKERS = 10 
q = [[],[],[],[],[]]       # internal data structre to buffer the tweets
NO_OF_QUEUES = len(q)     
SHOW_LEADERBOARD = False
SHOW_TRENDINGTOPICS = False
count = 0
threadlist = []  # list of threads in the program

# some common words to avoid
avoid = ['with', 'like', 'your', 'have', 'they', 'when', 'love', 'what', 'need', 'some', 'good', 'don\'t', 'want', 'this', 'are', 'get', 'eu', 'just',',', 'de', 'i', 'that', '&amp;', 'it\'s', 'haha'] 
totalTweets =  0


# main calls to extract tweets from twitter
class Client(threading.Thread):
	def __init__(self, username, password, qu) :
		self.friends = []
		self.buffer = ""
		self.time = 0
		self.username = username
		self.password = password
		self.userid = None
		threading.Thread.__init__(self)
		self.conn = pycurl.Curl()
		self.q = qu

	def authenticate(self, username, password):
		self.conn.setopt(pycurl.USERPWD, "%s:%s" % (username, password))
		self.conn.setopt(pycurl.URL, STREAM_URL)
		return True

	def connect(self):
		self.conn.setopt(pycurl.URL, STREAM_URL)
		self.conn.setopt(pycurl.WRITEFUNCTION, self.on_receive)
		self.conn.perform()


	# store the tweets in the buffer
	def on_receive(self, data):
		#global q
		global totalTweets
		self.time += 1
		self.buffer += data
		if data.endswith("\r\n") and self.buffer.strip():
			content = json.loads(self.buffer)
			self.buffer = ""
			if "text" in content:
				totalTweets += 1
				self.q.append(u"{0[user][name]}: {0[text]}".format(content))

	def run(self):
		if self.authenticate(self.username, self.password):
			self.connect()


# worker thread to write to the database
class Worker(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.nextQIndex = 0

	# extracts tweet from the buffer and parses it 
	def run(self):
		global q
		while(True):
			l = len(q[self.nextQIndex])
			if(l == 0):
				time.sleep(2)
				continue 
			tweet = q[self.nextQIndex].pop(0)
			self.parseAndAdd(tweet)
			self.nextQIndex = (self.nextQIndex + 1) % NO_OF_QUEUES

	# Parses the word and invokes the procedure to write it to the database
	# To make the process faster, instead of sending individual words, a list of word is sent 
	# to the procedure which are individually written to the database 
	def parseAndAdd(self, data):
		global count

		# accept words which are more than 3 letters 	
		writeWord = [l for l in data.split() if len(l)>3 and l not in avoid]
		length  = len(writeWord)
		#print length
		voltparams = json.dumps([writeWord, length])
		httpparams = urllib.urlencode({
			'Procedure': 'addwordlist', 'Parameters' : voltparams
		})
		# Execute the request
		try:
			urllib2.urlopen(url, httpparams)
		except:
			print "Database connection not found. Exiting."
			exit(1)
		count+=length 


# class to compute and display statistics and trending topics
class statistics(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.TotalCount = 0


	# invokes the 'readstats' procedure to read the stats
	def showLeaderBoard(self):
		global count
		global totalTweets
		httpparams = urllib.urlencode({'Procedure': 'readstats'})
		data = []
		# Execute the request
		try:
			data = json.load(urllib2.urlopen(url, httpparams))
		except:
			print "Database connection not found. Exiting."
			exit(1)
		reads =  data["results"][0]["data"][0][1]
		writes = data["results"][0]["data"][1][1]
		updates = data["results"][0]["data"][2][1]
		
		
		print "=================================="
		print str(count/float(TIME_INTERVAL)) + " words per second"
		count = 0
		print "Total tweets : " + str(totalTweets)
		print "Reads per sec :  " + str(reads/float(TIME_INTERVAL))
		print "Writes per sec : " + str(writes/float(TIME_INTERVAL))
		print "Updates per sec : " + str(updates/float(TIME_INTERVAL))


	
	# invokes the 'gettopics' procedure to get the trending topics
	def showTrendingTopics(self):
		httpparams = urllib.urlencode({'Procedure': 'gettopics'})
		data = []
		# Execute the request
		try:
			data = json.load(urllib2.urlopen(url, httpparams))
		except:
			print "Database connection not found. Exiting."
			exit(1)
		trendingtopicsList = data["results"][0]["data"]
		print "********** Trending Topics *************"
		for topic in trendingtopicsList:
			print "          ", topic[0]
		print "****************************************"
		
		

	def run(self):
		global TIME_INTERVAL
		global SHOW_LEADERBOARD
		global SHOW_TRENDINGTOPICS
		while(True):
			time.sleep(TIME_INTERVAL)
			if SHOW_LEADERBOARD == True:
				self.showLeaderBoard()
			if SHOW_TRENDINGTOPICS == True:
				self.showTrendingTopics()

# Class to purge the old words. Words older than 5 minutes are purged from the database
# using the procedure 'purge'
class purge(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)

	def purgefunction(self):
		httpparams = urllib.urlencode({'Procedure': 'purge'})
		# Execute the request
		data = json.load(urllib2.urlopen(url, httpparams))
				
	def run(self):
		while(True):
			time.sleep(2*TIME_INTERVAL + 1)
			self.purgefunction()


# Function to parse the command line arguments and initializes 
# the STATS table using the 'initialize' procedure
def initialize():
	global SHOW_LEADERBOARD
	global SHOW_TRENDINGTOPICS
	global NO_OF_WORKERS
	httpparams = urllib.urlencode({'Procedure': 'initialize'})
	# Execute the request
	data = []
	# Execute the request
	try:
		data = json.load(urllib2.urlopen(url, httpparams))
	except:
		print "Database connection not found. Exiting."
		exit(1)
	optlist, args = getopt.getopt(sys.argv[1:], "ltw:")
	for item in optlist:
		if '-l' in item[0]:
			SHOW_LEADERBOARD = True
		if '-t' in item[0]:
			SHOW_TRENDINGTOPICS = True
		if '-w' in item[0]:
			if item[1] == '':
				continue
			NO_OF_WORKERS = int(item[1])


# catch the CTRL+C signal and halt the program
# very ugly way of terminating all the threads
def signal_handler(signal, frame):
	for thread in threadlist:
	    if thread.isAlive():
		try:
		    thread._Thread__stop()
		except:
		    print str(thread.getName()) + ' could not be terminated'
        sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def main():
	global threadlist
	
	initialize()
	# create the statistics thread and store it 
	threadlist.append(statistics())

	# create the purge thread and store it
	threadlist.append(purge())

	# Create 5 streams to extract tweets from Twitter
	# 4 different accounts have been used and each of the streams have 
	# been passed a buffer 
	threadlist.append(Client("__test_1", "kanjilal", q[0]))
	threadlist.append(Client("__test_2", "kanjilal", q[1]))
	threadlist.append(Client("__test_3", "kanjilal", q[2]))
	threadlist.append(Client("__test_4", "kanjilal", q[3]))
	threadlist.append(Client("__test_5", "kanjilal", q[4]))
	

	# create the required number of worker threads
	global NO_OF_WORKERS
	print "Starting " + str(NO_OF_WORKERS) + " worker threads"
	for x in xrange(NO_OF_WORKERS):
		threadlist.append(Worker())
		
	# start all the threads
	for thread in threadlist:
		thread.start()


if __name__ == '__main__':
	main()
	signal.pause()
