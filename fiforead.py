#!/usr/bin/python3
import os, sys, psutil
import errno
import ssl
import json, pymemcache
from colorama import Fore, Back, Style
import plannatech_func_live as pfl
import config as app_config
context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE
app_config.load_env()
mcc = pymemcache.Client(app_config.memcache_address())
import time
import threading
import queue
from concurrent.futures import ThreadPoolExecutor
import logging
import dbcon

'''isTest=0

mydb = dbcon.mydb
curs_prep = mydb.cursor(prepared=True)  
curs_dict = mydb.cursor(dictionary=True)

ptuple=('20703604_prop','1526189')
ptuple=()
sql = " select count(*) from `contestant_ff` where 1 and sbid=31 limit 10"

curs_dict.execute(sql, ptuple)
#print("bla")
res2 = curs_dict.fetchall()
mydb.commit()					
for r in res2:
	print(r)
sys.exit(0)
'''
FIFO = 'FIFO'

try:
	os.mkfifo(FIFO)
except OSError as oe: 
	if oe.errno != errno.EEXIST:
		raise


print("Opening FIFO...")
nowTS=time.time()
workers = ThreadPoolExecutor(max_workers=50)


while True:
	elapsedTS=time.time()-nowTS    
	if elapsedTS>5:
		#print(elapsedTS)
		nowTS=time.time()
		#print(Fore.CYAN,"q.qsize():",q.qsize())
		process = psutil.Process(os.getpid())
		print("mem:",round(process.memory_info().rss / (1024 ** 2),2),"MB")
		#print("ch.is_open:",ch.is_open)
		#print("qid:",qid)        
		#print("t.isAlive:",t.isAlive())
		#print("qsize bytes",sys.getsizeof(q))
		print('pending:', workers._work_queue.qsize(), 'jobs')
		print('threads:', len(workers._threads))
    
	with open(FIFO) as fifo:
		#print("FIFO opened")
		try:
			while True:
				data = fifo.read()
				if len(data) == 0:
					#print("Writer closed")
					break
				#print('Read: "{0}"'.format(data))
				if "\n\n" in data:
					msgs=data.split("\n\n")
					for m in msgs:
						if(len(m)>0):
							pfl.processChunk(m)
							
				else:
					pfl.processChunk(data)
		except Exception as ex:
			print(ex)
