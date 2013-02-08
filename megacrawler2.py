import Queue
import threading
import urllib2
import time
from bs4 import BeautifulSoup
import re
import psycopg2

links = ["http://www.reddit.com/r/megalinks"]

link_queue = Queue.Queue()
chunk_queue = Queue.Queue()

class ThreadUrl(threading.Thread):
  """boop"""
  def __init__(self, link_queue, chunk_queue):
    threading.Thread.__init__(self)
    self.link_queue = link_queue
    self.chunk_queue = chunk_queue

  def run(self):
    while True:
      #grabs link from queue
      link = self.link_queue.get()

      #grabs urls of links and then grabs chunk of webpage
      #

      try:
        hdr = { 'User-Agent' : 'Just a friendly bot passing through!' }
        page = urllib2.Request(link, headers=hdr)
        url = urllib2.urlopen(page)
        chunk = url.read()
      except urllib2.URLError, e:
        self.link_queue.task_done()

      #place chunk into out queue
      self.chunk_queue.put(chunk)

      #signals to queue job is done
      self.link_queue.task_done()

class DatamineThread(threading.Thread):
  def __init__(self, data_queue, newlink_queue):
    threading.Thread.__init__(self)
    self.chunk_queue = chunk_queue
    self.newlink_queue = newlink_queue

  def run(self):
    while True:
      #grabs link from queue
      chunk = self.chunk_queue.get()

      #parse the chunk
      soup = BeautifulSoup(chunk)
      for link in soup.find_all('a'):
        self.newlink_queue.put(unicode(link.get('href')))
      #signals to queue job is done
      self.chunk_queue.task_done()

def store(link):
  cur = conn.cursor()
  try:
    cur.execute("INSERT INTO tmplinks(link) VALUES(%s);", (link,))
  except psycopg2.IntegrityError, e:
    conn.rollback()
    return False
  cur.close()
  return True

start = time.time()
def main():
  max_depth = 2
  depth = 0
  count = 0
  megacount = 0
  while depth<max_depth:
    depth += 1
    newlink_queue = Queue.Queue()
    #spawn a pool of threads, and pass them queue instance
    for i in range(5):
      t = ThreadUrl(link_queue, chunk_queue)
      t.setDaemon(True)
      t.start()

    #populate queue with data
    for link in links:
      link_queue.put(link)

    for i in range(5):
      dt = DatamineThread(chunk_queue, newlink_queue)
      dt.setDaemon(True)
      dt.start()

    #wait on the queue until everything has been processed
    link_queue.join()
    chunk_queue.join()
    #newlink_queue.join()

#WHAT THE FUCK DID I DO HERE
#Give DB own thread to store shit 
#make this flow better
#shouldnt be so loopy like this
    while newlink_queue.qsize():
      link = newlink_queue.get()
      #print link
      megalinkre = re.search('(https://)(www.|)mega.co.nz/#!.{52}$', link)
      nonmegalinkre = re.search('(http://)(www.|)', link)
      if megalinkre:
        if store(link): megacount+=1
      elif nonmegalinkre:
        count+=1
        link_queue.put(link)
      #link_queue.put(link)
      newlink_queue.task_done()
  print "END - %d/%d megalinks/links" % (megacount,count)

conn = psycopg2.connect("dbname=ogatest user=postgres password=1234")
main()
print "Elapsed Time: %s" % (time.time() - start)
conn.commit()
conn.close()