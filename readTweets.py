import pycurl, json
import Queue
import threading
import urllib
import urllib2
import time

# Construct the procedure name, parameter list, and URL.

url = 'http://localhost:8080/api/1.0/'

STREAM_URL = "https://stream.twitter.com/1/statuses/sample.json"
TIME_INTERVAL = 5
NO_OF_WORKERS=10
NO_OF_QUEUES = 4
#q = Queue.Queue(0)
count = 0
q = [[],[],[],[]]
avoid = ['RT', ':', '...', 'my', 'me', '.', 'so', 'is','this', 'are', 'get', 'eu', 'just',','	] 
totalTweets =  0

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



class Worker(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		print "initialized"
		self.nextQIndex = 0

	def run(self):
		print "Thread started"
		global q
		while(True):
			l = len(q[self.nextQIndex])
			if(l == 0):
				time.sleep(2)
				continue 
			tweet = q[self.nextQIndex].pop(0) #q.get(True)
			self.parseAndAdd(tweet)
			self.nextQIndex = (self.nextQIndex + 1) % NO_OF_QUEUES

	def parseAndAdd(self, data):
		global count
		
		writeWord = [l for l in data.split() if l not in avoid]
		length  = len(writeWord)
		#print length
		voltparams = json.dumps([writeWord, length])
		httpparams = urllib.urlencode({
			'Procedure': 'addwordlist', 'Parameters' : voltparams
		})
		# Execute the request
		urllib2.urlopen(url, httpparams)
		count+=length 

class statistics(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.TotalCount = 0


	def run(self):
		global count
		global totalTweets
		while(True):
			time.sleep(TIME_INTERVAL)
			httpparams = urllib.urlencode({'Procedure': 'readstats'})
			# Execute the request
			data = json.load(urllib2.urlopen(url, httpparams))
			reads =  data["results"][0]["data"][0][1]
			writes = data["results"][0]["data"][1][1]
			updates = data["results"][0]["data"][2][1]
			
			
			print "=================================="
			print str(count/float(TIME_INTERVAL)) + " words per second"
			#self.TotalCount = self.TotalCount + count 
			count = 0
			#print "Total words : " + str(self.TotalCount)
			print "Total tweets : " + str(totalTweets)
			print "Reads per sec :  " + str(reads/float(TIME_INTERVAL))
			print "Writes per sec : " + str(writes/float(TIME_INTERVAL))
			print "Updates per sec : " + str(updates/float(TIME_INTERVAL))
			#print "Queue length : " + str(len(q))


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

def initialize():
	httpparams = urllib.urlencode({'Procedure': 'initialize'})
	# Execute the request
	urllib2.urlopen(url, httpparams)
	

def main():
	initialize()
	statistics().start()
	purge().start()
	client1 = Client("__test_1", "kanjilal", q[0]).start()
	client2 = Client("__test_2", "kanjilal", q[1]).start()
	client3 = Client("__test_3", "kanjilal", q[2]).start()
	client4 = Client("__test_4", "kanjilal", q[3]).start()
	for x in xrange(NO_OF_WORKERS):
		time.sleep(1)
		Worker().start()


if __name__ == '__main__':
	main()
