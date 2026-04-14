#!/usr/bin/python3
import os,sys,psutil
import pika
import ssl
import json, pymemcache
from colorama import Fore, Back, Style
import plannatech_func_live as pfl
import time
import threading
import queue
from datetime import timedelta, date, datetime
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
import schedule

import logging
import broker as mqb
import config as app_config
#logger = logging.Logger('catch_all')


q={}
manager = mp.Manager()
gameIdList = manager.list()
missList = manager.list()
pstatus = manager.dict()
pstatus_p=pfl.pstatus_p
oddict = manager.dict()
oddict_ff = manager.dict()

app_config.load_env()
context = app_config.remote_ssl_context()
mcc = pymemcache.Client(app_config.memcache_address())
mq_client=mqb.init_client("plannatech_client")

def connect_amq():
    try:
        c = pika.BlockingConnection(app_config.remote_rabbitmq_params(heartbeat=20))
    except Exception as ex:
        print(ex)
        print(Fore.RED, "Failed to connect")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return None
    return c


def loadGameIdList_memcached():
    try:
        gameIdListStoredj = mcc.get("gameIdList")
        #print("Scheduled start, gameIdList size:",len(gameIdList))
        gameLoadCnt = 0
        if gameIdListStoredj is not None:
            gameIdListStored = json.loads(gameIdListStoredj)
            diffCnt=0	
            for idGame in  gameIdListStored:					
                if idGame not in gameIdList:
                    gameLoadCnt += 1
                    diffCnt+=1
                    #print(idGame)
                    gameIdList.append(idGame)
            newList=list(gameIdList)
        mcc.set("gameIdList",json.dumps(newList),36000)
        print("loaded",gameLoadCnt)
    except:
        pass

            
    #print("Scheduled finish, gameIdList size:",len(gameIdList))
        

print("startup");
loadGameIdList_memcached()
print("load memcached")

schedule.every(60).seconds.do(loadGameIdList_memcached)

msgCount=0
msgTypeCount={}

executor = ProcessPoolExecutor(max_workers=5)
#q = queue.Queue(10000)
'''bigdocQFF=manager.dict()
bigdocQ=manager.dict()
'''

for i in range(0,4):
    qid=i
    q[i] = mp.Queue(10000)
    print("Creating queue and process",i)
    p4=mp.Process(target=pfl.qreader,args=((q[qid],qid,gameIdList,missList,pstatus),),daemon=True)
    p4.start()



c=connect_amq()
ch = None
try:
    ch = c.channel()
except Exception as ex:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print(exc_type, fname, exc_tb.tb_lineno)
    pass

threads = []
elapsed=0

def on_message_local(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)
    global msgCount,msgTypeCount
    
    msgCount+=1
    #### new, local difference
    msgObj = json.loads(body)
    routing_key="other"
    try:
        routing_key = msgObj["routing_key"]
        #print(routing_key)
    except:
        print(body)
        pass

    if routing_key in msgTypeCount:
        msgTypeCount[routing_key]+=1
    else:
        msgTypeCount[routing_key]=1	
    #### new, local difference
    msgJ=json.dumps(msgObj)
    #q.put_nowait(msgJ)
    try:
        idx=msgCount%4
        qIDX = pfl.findSmallestQueue(q,0)
        #thisQ=q[msgCount%4]
        thisQ = q[qIDX]
        #print(idx,thisQ.qsize())
        if routing_key in ['TNT','l','3']:
            #for i in range(0,110):
            thisQ.put(msgObj)
        #with open('FIFO', 'w') as fifo:
        #    fifo.write(msgObj)
        

    except Exception as e:
        #logger.error(e, exc_info=False)		
        print(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        pass


def on_message(ch, method, properties, body):
    #print(method)
    #t = threading.Thread(target=pfl.processFeedLineMessage, args=(json.loads(body)))
    #t.start()
    #threads.append(t)
    #print(Fore.RED,len(threads))	
    #print(method.routing_key)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    global msgCount,msgTypeCount
    msgCount+=1
    #mcc.add("bla","blu",3600)
    msgObj={}
    routing_key=method.routing_key
    msgObj["routing_key"]=routing_key
    if routing_key in msgTypeCount:
        msgTypeCount[routing_key]+=1
    else:
        msgTypeCount[routing_key]=1
    msgObj["headers"]=properties.headers
    msgObj["body"]=json.loads(body)
    #print("headers:",type(properties.headers))
    #print("body:",type(body))
    #print("msgObj:",type(msgObj))
    #print(msgObj)
    
    msgJ=json.dumps(msgObj)
    #q.put_nowait(msgJ)
    try:
        idx=msgCount%4
        thisQ=q[msgCount%8]
        #print(idx,thisQ.qsize())
        if routing_key in ['TNT','l','3']:
            #for i in range(0,110):
            thisQ.put(msgObj)
        #with open('FIFO', 'w') as fifo:
        #    fifo.write(msgObj)
        

    except Exception as e:
        #logger.error(e, exc_info=False)		
        print(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        pass

def runClient():
    ch.basic_consume(queue=app_config.remote_queue_name(),
                    on_message_callback=on_message, auto_ack=True)
                    
    '''ch.basic_consume(queue="plannatech",
                    on_message_callback=on_message, auto_ack=True)'''
    ch.start_consuming()

def runClient_new_local():
    
    while(True):
        
        time.sleep(5)        
        try:            
            now = datetime.now()
            dt_sting = now.strftime("%d/%m/%Y %H:%M:%S")
            print(Fore.BLUE,dt_sting,"Connecting localhost...")
            connection = pika.BlockingConnection(app_config.local_rabbitmq_params(heartbeat=20))
            print(Fore.LIGHTGREEN_EX,dt_sting,"Connected")
            channel = connection.channel()
            channel.basic_qos(prefetch_count=100)
            channel.basic_consume(queue=app_config.local_queue_name(),on_message_callback=on_message_local)			
            print(Fore.GREEN,dt_sting,"Consuming")
            try:
                channel.start_consuming()
                
            except KeyboardInterrupt:
                channel.stop_consuming()
                connection.close()
                break
        except pika.exceptions.ConnectionClosedByBroker:
            # Uncomment this to make the example not attempt recovery
            # from server-initiated connection closure, including
            # when the node is stopped cleanly
            #
            # break
            continue
        # Do not recover on channel errors
        except pika.exceptions.AMQPChannelError as err:
            print("Caught a channel error: {}, stopping...".format(err))
            break
        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:
            print("Connection was closed, retrying...")
            continue

def runClient_new():
    
    while(True):
        
        time.sleep(5)        
        try:            
            now = datetime.now()
            dt_sting = now.strftime("%d/%m/%Y %H:%M:%S")
            print(Fore.BLUE,dt_sting,"XXXXXXXXXXXXXXXXXXXXXX Connecting...")
            #random.shuffle(all_endpoints)
            '''connection = pika.BlockingConnection(pika.ConnectionParameters(
                    # host="inplay-rmq.lsports.eu",
                    host="prematch-rmq.lsports.eu",
                    port=5672,
                    virtual_host="Customers",
                    credentials=pika.PlainCredentials(
                        username="devops@datadrivesports.com", password="ke4@he7Res", erase_on_connect=False),
                    # ssl_options=pika.SSLOptions(context),
                    heartbeat=20
                ))'''
            connection = pika.BlockingConnection(app_config.remote_rabbitmq_params(heartbeat=20))
            print(Fore.LIGHTGREEN_EX,dt_sting,"Connected")
            channel = connection.channel()
            channel.basic_qos(prefetch_count=100)
            channel.basic_consume(queue=app_config.remote_queue_name(),on_message_callback=on_message)			
            print(Fore.GREEN,dt_sting,"Consuming")
            try:
                channel.start_consuming()
                
            except KeyboardInterrupt:
                channel.stop_consuming()
                connection.close()
                break
        except pika.exceptions.ConnectionClosedByBroker:
            # Uncomment this to make the example not attempt recovery
            # from server-initiated connection closure, including
            # when the node is stopped cleanly
            #
            # break
            continue
        # Do not recover on channel errors
        except pika.exceptions.AMQPChannelError as err:
            print("Caught a channel error: {}, stopping...".format(err))
            break
        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:
            print("Connection was closed, retrying...")
            continue
                
    



'''t = threading.Thread(target=runClient_new) #, args=(json.loads(body))
t.start()
threads.append(t)'''

t = threading.Thread(target=runClient_new_local) #, args=(json.loads(body))
t.start()
threads.append(t)


t2 = threading.Thread(target=pfl.missingHandler, args=(missList,oddict,oddict_ff,)) #, args=(json.loads(body))
t2.start()
threads.append(t2)



#qconsumers = ThreadPoolExecutor(max_workers=5)
qconsumers = ProcessPoolExecutor(max_workers=5)


print("XXXXXXXXXXXXXXXXX")
nowTS=time.time()
qid=0

'''for i in range(0,4):
    bigdocQFF[i] = mp.Queue(100000)
    bigdocQ[i] = mp.Queue(100000)
    
    p1=mp.Process(target=pfl.publishHandler, args=(bigdocQFF[i],True,i))
    p1.start()
    p2=mp.Process(target=pfl.publishHandler, args=(bigdocQ[i],False,i))
    p2.start()
'''


while True:
    time.sleep(0.001)
    schedule.run_pending()
    try:
        elapsedTS=time.time()-nowTS    
        if elapsedTS>1:
            #print(elapsedTS)
            nowTS=time.time()
            process = psutil.Process(os.getpid())
            #missList = json.loads(mcc.get("missList"))	
            #print(missList)		
            #print(gameIdList)
            
            d_msg={
                "main":{
                "mem":round(process.memory_info().rss / (1024 ** 2),2),
                "qid":qid,
                "t.isAlive":t.is_alive(),
                #"qsize MB":sys.getsizeof(q)/1024,2),
                "idlist MB":round(sys.getsizeof(json.dumps(list(gameIdList)))/1024/1024,2),
                "msgCount":msgCount,
                "missList":len(missList),
                "gameList":len(gameIdList),
                "nowTS":time.time(),				
                
                #"jobs pending": qconsumers._work_queue.qsize(),
                #"threads:":len(qconsumers._threads)
                #"processes:":len(qconsumers._processes)
                },
                
                "msgTypeCount":msgTypeCount
                
            }

            pstatus_msg={}
            
            for s in pstatus:
                pstatus_msg[s]=pstatus[s]
            for s in pstatus_p:
                pstatus_msg[s]=pstatus_p[s]
            qsizes={}
            d_msg["processes"]=pstatus_msg
            
            for i in q:
                thisQ=q[i]
                qsizes[i]=thisQ.qsize()
            d_msg["queue size"]=qsizes
            
            try:
                d_msg["ch.is_open"]=ch.is_open
                d_msg["ch.connection.is_open"]=ch.connection.is_open
            except:
                pass
            try:
                mq_client.publish('plannatech_status',json.dumps(d_msg,indent=2),retain=True)
            except Exception as ex_mq:
                print(ex_mq)


            '''print(Fore.BLUE+"----------------------------------------------------")
            print(Fore.CYAN+"q.qsize():",q.qsize())
            process = psutil.Process(os.getpid())
            print("mem:",round(process.memory_info().rss / (1024 ** 2),2),"MB")
            try:
                print("ch.is_open:",ch.is_open)
                print("ch.connection.is_open:",ch.connection.is_open)
            except:
                pass
            print("qid:",qid)        
            print("t.isAlive:",t.isAlive())
            print("qsize bytes",sys.getsizeof(q))
            print("msgCount:",msgCount)
            print('pending:', qconsumers._work_queue.qsize(), 'jobs')
            print('threads:', len(qconsumers._threads))
            
            #print(type(ch))'''
            #NEW RECONNECT
            '''if ch is None:
                print("connection closed")
                c = connect_amq()
                ch = c.channel()
                print("ch.is_open:",ch.connection.is_open)
            elif not ch.connection.is_open:
                print("connection closed")
                c = connect_amq()
                ch = c.channel()
                print("ch.is_open:",ch.connection.is_open)
            else:
                pass
            '''

            if not t.is_alive():
                print(Fore.RED,"Thread is dead, starting new")
                #t = threading.Thread(target=runClient) #, args=(json.loads(body))
                #t.start()
                #threads.append(t)
    except Exception as ex:
        print(ex)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
    
    #if q.qsize()>0 and  len(qconsumers._threads)<2:                     
    #if q.qsize()>0 and  len(qconsumers._processes)<5:                     
    #	time.sleep(1)		
    #	qid+=1
    #	thisQ=q[qid%4]
    #	qconsumers.submit(pfl.qreader,(thisQ,qid))
        
'''for qid in range[0,4]:
    qid+=1
    # thisQ=qs[]
    qconsumers.submit(pfl.qreader,(thisQ,qid))
sys.exit()
'''
    
