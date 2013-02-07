from bs4 import BeautifulSoup
import urllib2
import re
import psycopg2
import sys

megaLinks = set()
allLinks = set() # Adding this makes each parse slower but does less parses
count = 0
maxDepth = 2

def crawl(start, megaLinks, allLinks, depth):
  links = set()

  parse(start, megaLinks, allLinks, links)
  links = {link for link in links if link not in allLinks} # See set comprehensions when I forget what this is.
  for link in links:
    allLinks.add(link)
  if(depth>maxDepth): return

  global count
  localcount = 0
  linkcount = len(links)
  depthstring = ""
  for i in range(depth): depthstring+="--|"

  for link in links:
    count+=1
    localcount+=1
    print "%s %d - %d - %d/%d Crawling: %s" % (depthstring, count, len(megaLinks), localcount, linkcount, link)
    sys.stdout.flush()
    crawl(link, megaLinks, allLinks, depth+1)

def parse(page, megaLinks, allLinks, links):
  try:
    hdr = { 'User-Agent' : 'Just a friendly bot passing through!' }
    req = urllib2.Request(page, headers=hdr)
    html = urllib2.urlopen(req).read()
    soup = BeautifulSoup(html)
    for link in soup.find_all('a'):
        s = unicode(link.get('href'))
        megalinkre = re.search('(https://)(www.|)mega.co.nz/#!.{52}$', s)
        nonmegalinkre = re.search('(http://)(www.|)', s)
        if megalinkre and s not in megaLinks:
          megaLinks.add(s)
        elif nonmegalinkre and s not in links:
          links.add(s)
        elif s!=None and s[0]=='/' and (page+s) not in links:
          links.add(page+s)
  except urllib2.URLError, e:
    print e
  except: 
    print sys.exc_info()[0]

crawl("http://www.reddit.com/", megaLinks, allLinks, 0)

print len(megaLinks)
print "Adding links to DB"

conn = psycopg2.connect("dbname=ogatest user=postgres password=1234")
cur = conn.cursor()

for link in megaLinks:
  try:
    cur.execute("INSERT INTO tmplinks(link) VALUES(%s);", (link,))
  except psycopg2.IntegrityError:
    conn.rollback()
print "*********************************"
print "Out of %d links, %d were megalinks." % (len(allLinks),len(megaLinks))

cur.close()
conn.commit()
conn.close()