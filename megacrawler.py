from bs4 import BeautifulSoup
import urllib2
import re
import psycopg2
import sys
import threading

megaLinks = set()
allLinks = set() # Adding this makes each parse slower but does less parses
count = 0
maxDepth = 2
conn = psycopg2.connect("dbname=ogatest user=postgres password=1234")

def crawl(start, megaLinks, allLinks, depth):
  if(depth>maxDepth): return
  links = set()
  parse(start, megaLinks, links)

  global count
  depthstring = ""
  for i in range(depth): 
    depthstring+="--|"

  #threads = []

  explore = [[link,parse(link, megaLinks,set())] for link in links]
  #t= threading.Thread(target=parse, args=(link, megaLinks, allLinks, newLinks))
  # parse(link, megaLinks, allLinks, newLinks)
  # [x.start() for x in threads]
  # [x.join() for x in threads]

  print explore
  #explore = [link for link in newLinks]

def parse(page, megaLinks, links):
  score = 0

  try:
    hdr = { 'User-Agent' : 'Just a friendly bot passing through!' }
    req = urllib2.Request(page, headers=hdr)
    html = urllib2.urlopen(req).read()
    soup = BeautifulSoup(html)
    for link in soup.find_all('a'):
        s = unicode(link.get('href'))
        megalinkre = re.search('(https://)(www.|)mega.co.nz/#!.{52}$', s)
        nonmegalinkre = re.search('(http://)(www.|)', s)
        if megalinkre:
          score += 1
          megaLinks.add(s)
          store(s) # make this async else it will slow it down even MORE
        elif nonmegalinkre and s not in links:
          links.add(s)
        elif s!=None and s[0]=='/' and (page+s) not in links:
          links.add(page+s)
  except urllib2.URLError, e:
    print e
    return 0
  except IndexError, e: 
    print e

  #Only add NEW links to the collection of all the links
  links = {link for link in links if link not in allLinks} # See set comprehensions when I forget what this is.
  for link in links:
    allLinks.add(link)
  print score
  return score

def store(link):
  cur = conn.cursor()
  try:
    cur.execute("INSERT INTO tmplinks(link) VALUES(%s);", (link,))
  except psycopg2.IntegrityError:
    conn.rollback()
  cur.close()

crawl("http://www.reddit.com/r/megalinks", megaLinks, allLinks, 0)

print len(megaLinks)

print "*********************************"
print "Out of %d links, %d were megalinks." % (len(allLinks),len(megaLinks))

conn.commit()
conn.close()