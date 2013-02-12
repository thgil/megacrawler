import threading
import Queue
import operator
import urllib2
from bs4 import BeautifulSoup
import re
import psycopg2
import sys

class MapReduce:
    ''' MapReduce - to use, subclass by defining these functions,
                    then call self.map_reduce():
        parse_fn(self, k, v) => [(k, v), ...]
        map_fn(self, k, v) => [(k, v1), (k, v2), ...]
        reduce_fn(self, k, [v1, v2, ...]) => [(k, v)]
        output_fn(self, [(k, v), ...])
    '''
    def __init__(self):
        self.data = None
        self.num_worker_threads = 5

    class SynchronizedDict(): # we need this for merging
        def __init__(self):
            self.lock = threading.Lock()
            self.d = {}
        def isin(self, k):
            with self.lock:
                if k in self.d:
                    return True
                else:
                    return False
        def get(self, k):
            with self.lock:
                return self.d[k]
        def set(self, k, v): # we don't need del
            with self.lock:
                self.d[k] = v
        def set_append(self, k, v): # for thread-safe list append
            with self.lock:
                self.d[k].append(v)
        def items(self):
            with self.lock:
                return self.d.items()

    def create_queue(self, input_list): # helper fn for queues
        output_queue = Queue.Queue()
        for value in input_list:
            output_queue.put(value)
        return output_queue

    def create_list(self, input_queue): # helper fn for queues
        output_list = []
        while not input_queue.empty():
            item = input_queue.get()
            output_list.append(item)
            input_queue.task_done()
        return output_list

    def merge_fn(self, k, v, merge_dict): # helper fn for merge
        if merge_dict.isin(k):
            merge_dict.set_append(k, v)
        else:
            merge_dict.set(k, [v])

    def process_queue(self, input_queue, fn_selector): # helper fn
        output_queue = Queue.Queue()
        if fn_selector == 'merge':
            merge_dict = self.SynchronizedDict()
        def worker():
            while not input_queue.empty():
                (k, v) = input_queue.get()
                if fn_selector in ['map', 'reduce']:
                    if fn_selector == 'map':
                        result_list = self.map_fn(k, v)
                    elif fn_selector == 'reduce':
                        result_list = self.reduce_fn(k, v)
                    for result_tuple in result_list: # flatten
                        output_queue.put(result_tuple)
                elif fn_selector == 'merge': # merge v to same k
                    self.merge_fn(k, v, merge_dict)
                else:
                    raise Exception, "Bad fn_selector="+fn_selector
                input_queue.task_done()
        for i in range(self.num_worker_threads): # start threads
            worker_thread = threading.Thread(target=worker)
            worker_thread.daemon = True
            worker_thread.start()
        input_queue.join() # wait for worker threads to finish
        if fn_selector == 'merge':
            output_list = sorted(merge_dict.items(), key=operator.itemgetter(0))
            output_queue = self.create_queue(output_list)
        return output_queue

    def map_reduce(self): # the actual map-reduce algoritm
        data_list = self.parse_fn(self.data)
        #print "DATA_LIST:",data_list
        data_queue = self.create_queue(data_list) # enqueue the data so we can multi-process
        # print "\033[31mDATA_QUEUE:",data_queue.queue
        map_queue = self.process_queue(data_queue, 'map') # [(k,v),...] => [(k,v1),(k,v2),...]
        # print "\033[32mMAP_QUEUE:",map_queue.queue
        merge_queue = self.process_queue(map_queue, 'merge') # [(k,v1),(k,v2),...] => [(k,[v1,v2,...]),...]
        # print "\033[33mMERGE_QUEUE:",merge_queue.queue
        reduce_queue = self.process_queue(merge_queue, 'reduce') # [(k,[v1,v2,...]),...] => [(k,v),...]
        # print "\033[34mREDUCE_QUEUE:\033[0m",reduce_queue.queue
        output_list = self.create_list(reduce_queue) # deque into list for output handling
        return self.output_fn(output_list)

class WordCount(MapReduce):

    def __init__(self):
        MapReduce.__init__(self)
        self.min_count = 1

    def parse_fn(self, data): # break string into [(k, v), ...] tuples for each line
        data_list = map(lambda line: (None, line), data.splitlines())
        return data_list

    def map_fn(self, key, str): # return (word, 1) tuples for each word, ignore key
        word_list = []
        for word in re.split(r'\W+', str.lower()):
            bare_word = re.sub(r"[^A-Za-z0-9]*", r"", word);
            if len(bare_word) > 0:
                word_list.append((bare_word, 1))
        print "WORD_LIST:",word_list
        return word_list

    def reduce_fn(self, word, count_list): # just sum the counts
        return [(word, sum(count_list))]

    def output_fn(self, output_list): # just print the resulting list
        print "Word".ljust(15), "Count".rjust(5)
        print "______________".ljust(15), "_____".rjust(5)
        sorted_list = sorted(output_list, key=operator.itemgetter(1), reverse=True)
        for (word, count) in sorted_list:
            if count > self.min_count:
                print word.ljust(15), repr(count).rjust(5)
        print

    def test_with_monty(self):
        self.data = """The Meaning of Life is:
            try and be nice to people,
            avoid eating fat,
            read a good book every now and then,
            get some walking in,
            and try and live together in peace and harmony
            with people of all creeds and nations."""
        self.map_reduce()

    def test_with_nietzsche(self):
        self.min_count = 700
        f = urllib2.Request("http://www.gutenberg.org/cache/epub/7205/pg7205.txt")
        url = urllib2.urlopen(f)
        self.data = url.read()
        url.close()
        self.map_reduce()

class Crawler(MapReduce):
  def __init__(self, depth, max_depth, done):
    MapReduce.__init__(self)
    self.done = done
    self.todo = []
    self.depth = depth
    self.max_depth = max_depth

  def parse_fn(self, links):
    #print "parsing: ",links
    data = []
    for link in links:
      print "Parsing: \033[34m%s\033[0m"%link
      sys.stdout.flush()
      data.append((link, []))
    return data


#
#
# Hey check for copies before mapping
#
#
#



  def map_fn(self, link, new_links): # load, extract, save
    links = []
    #print "Mapping:\033[32m",link, "\033[0m"
    try:
      hdr = { 'User-Agent' : 'Just a friendly bot passing through!' }
      req = urllib2.Request(link, headers=hdr)
      html = urllib2.urlopen(req).read()
      soup = BeautifulSoup(html)
      for x in soup.find_all('a'):
          #regex
          s = unicode(x.get('href'))
          megalinkre = re.search('(https://)(www.|)mega.co.nz/#!.{52}$', s)
          nonmegalinkre = re.search('(http://)(www.|)', s)
          #print "PARENT:",link,"  LINK:",s
          if megalinkre:
            store(s)
          elif nonmegalinkre and s not in self.done:
            links.append((link,s))
    except urllib2.URLError, e:
      print "\033[31m%s\033[0m"%link
      print e
    except IndexError, e:
      print "\033[31m%s\033[0m"%link
      print e
    except:
      print "\033[31m%s\033[0m"%link
      print "\033[31mUnexpected error:\033[0m", sys.exc_info()[0]
    return links

  def reduce_fn(self, link, new_links): # Fix our lists
    #print "reducing: ",links," - ",new_links
    self.todo = new_links
    self.done.append(link) #ffix
    return (link,new_links)

  def output_fn(self, links): # just print the resulting list
    #print links," - todo:",self.todo," - done:",self.done
    print "\033[31;1mDEPTH:\033[0m",self.depth
    sys.stdout.flush()
    print "self done = ",self.done
    if(self.depth<self.max_depth): # should this be in reduce :3?
      # self.data = self.todo
      # self.todo = []
      # self.map_reduce()
      x = Crawler(self.depth+1, self.max_depth, self.done)
      print "asd",x.test(self.todo)
    else:
      return self.done

  def test(self, data):
    self.data = data
    self.map_reduce()
    print "\033[31;1mI have %d done!\033[0m" % len(self.done)
    print "\033[33;1mI have %d todo!\033[0m" % len(self.todo)
    print "\033[36;1mI have %d found(not mega)!\033[0m" % (len(self.done)+len(self.todo))

def store(link):
  conn = psycopg2.connect("dbname=ogatest user=postgres password=1234")
  cur = conn.cursor()
  try:
    cur.execute("INSERT INTO tmplinks(link) VALUES(%s);", (link,))
  except psycopg2.IntegrityError, e:
    print "\033[37m.\033[0m",
    conn.rollback()
    conn.close()
    return
  cur.close()
  conn.commit()
  conn.close()

def main():
  done = []
  depth = 0
  max_depth = 2
  c = Crawler(depth, max_depth, done)
  c.test(["http://reddit.com/r/megalinks","http://reddit.com/","http://notmega.com/"])

if __name__ == "__main__":
  main()