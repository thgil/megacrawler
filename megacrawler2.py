import Queue
import threading
import urllib2
import time
from bs4 import BeautifulSoup
import re
import psycopg2

links = ["http://www.reddit.com/r/megalinks"]

link_queue = Queue.Queue()
chunk_queue = Queue.Queue(10)
mega_queue = Queue.Queue()
list_queue = Queue.Queue()

class ThreadUrl(threading.Thread):
  def __init__(self, link_queue, chunk_queue):
    threading.Thread.__init__(self)
    self.link_queue = link_queue
    self.chunk_queue = chunk_queue

  def run(self):
    while True:
      #grabs link from queue
      link = self.link_queue.get()
      print self.link_queue.qsize()," - ",link #should be unique and clean
      #grabs urls of links and then grabs chunk of webpage
      try:
        hdr = { 'User-Agent' : 'Just a friendly bot passing through!' }
        page = urllib2.Request(link, headers=hdr)
        url = urllib2.urlopen(page)
        chunk = url.read()
      except urllib2.URLError, e:
        print e
        self.link_queue.task_done()
        return

      #place chunk into out queue
      self.chunk_queue.put(chunk)

      #signals to queue job is done
      self.link_queue.task_done()

class ThreadDatamine(threading.Thread):
  def __init__(self, data_queue, mega_queue, list_queue):
    threading.Thread.__init__(self)
    self.chunk_queue = chunk_queue
    self.mega_queue = mega_queue
    self.list_queue = list_queue

  def run(self):
    while True:
      #grabs link from queue
      chunk = self.chunk_queue.get()
      #parse the chunk
      soup = BeautifulSoup(chunk)
      for link in soup.find_all('a'):
        s = unicode(link.get('href'))
        linkregex = re.search('(http://)(www.|)', s)
        if(linkregex):
          self.list_queue.put(s)
        else:
          megaregex = re.search('(https://)(www.|)mega.co.nz/#!.{52}$', s)
          if(megaregex):
            self.mega_queue.put(s)
      #signals to queue job is done
      self.chunk_queue.task_done()

class ThreadList(threading.Thread):
  def __init__(self, list_queue, link_queue):
    threading.Thread.__init__(self)
    self.list_queue = list_queue
    self.link_queue = link_queue
    self.allLinks = set([])

  def run(self):
    while True:
      link = self.list_queue.get()
      if(link not in self.allLinks):
        self.allLinks.add(link)
        self.link_queue.put(link)

      self.list_queue.task_done()

class ThreadStore(threading.Thread):
  def __init__(self, mega_queue):
    threading.Thread.__init__(self)
    self.mega_queue = mega_queue

  def run(self):
    while True:
      link = self.mega_queue.get()
      store(link)
      self.mega_queue.task_done()

def store(link):
  conn = psycopg2.connect("dbname=ogatest user=postgres password=1234")
  cur = conn.cursor()
  try:
    cur.execute("INSERT INTO tmplinks(link) VALUES(%s);", (link,))
  except psycopg2.IntegrityError, e:
    conn.rollback()
    return False
  cur.close()
  conn.commit()
  conn.close()
  return True

start = time.time()
def main():

  #spawn a pool of threads, and pass them queue instance
  for i in range(10):
    t = ThreadUrl(link_queue, chunk_queue)
    t.start()

  #init population of queue with data
  for link in links:
    link_queue.put(link)

  for i in range(100):
    u = ThreadDatamine(chunk_queue, mega_queue, list_queue)
    u.start()

  for i in range(1):
    s = ThreadList(list_queue, link_queue)
    s.start()

  for i in range(1):
    v = ThreadStore(mega_queue)
    v.start()

  #wait on the queue until everything has been processed
  link_queue.join()
  chunk_queue.join()
  mega_queue.join()

main()
print "Elapsed Time: %s" % (time.time() - start)