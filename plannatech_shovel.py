#!/usr/bin/python3
import os,sys,psutil
import pika
import ssl
import json, pymemcache
from colorama import Fore, Back, Style
from queue import Queue
import time
import threading
import queue
from concurrent.futures import ThreadPoolExecutor
import logging
#logger = logging.Logger('catch_all')
import random
from datetime import timedelta, date, datetime
import broker as mqb
import multiprocessing as mp
import config as app_config

app_config.load_env()

manager = mp.Manager()

chandict=manager.dict()
mq_client=mqb.init_client("plannatech_shovel")

#q = Queue(maxsize = 999999999)
q = mp.Queue(1000000)
q2 = mp.Queue(1000000)

threads = []
lcon2 = None
lchan2 = None
lcon1 = None
lchan1 = None

rcon = None
rchan = None
reconTotalCount = 0
reconCount = 0
channels = {}

def amq_connect(hostName):
    params = app_config.local_rabbitmq_params(heartbeat=20)
    params.host = hostName
    con = pika.BlockingConnection(params)
    return con

primary_host, secondary_host = app_config.shovel_publish_hosts()
ch1 = amq_connect(primary_host).channel()
ch2 = amq_connect(secondary_host).channel()

def on_command_message(ch, method, properties, body):
    pass
    
def on_discard_message(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)

def on_message(ch, method, properties, body):
    global channels, q,q2,ch1,ch2,chandict
    
    #msgCount+=1
    #mcc.add("bla","blu",3600)
    msgObj={}
    routing_key=method.routing_key
    #print(method.routing_key)
    msgObj["routing_key"]=routing_key
    '''if routing_key in msgTypeCount:
        msgTypeCount[routing_key]+=1
    else:
        msgTypeCount[routing_key]=1'''
    msgObj["headers"]=properties.headers
    msgObj["body"]=json.loads(body)
    #print("headers:",type(properties.headers))
    #print("body:",type(body))
    #print("msgObj:",type(msgObj))
    #print(msgObj)    
    msgJ=json.dumps(msgObj)
    #print(Fore.MAGENTA,'.',end='')
    try:
        q.put_nowait(msgJ)
    except Exception as ex:
        pass
        #print(ex)
    '''try:
        q2.put_nowait(msgJ)
    except Exception as ex:
        pass
        #print(ex)
    '''
        
    ch.basic_ack(delivery_tag=method.delivery_tag)
    '''ch1.basic_publish('',
            'plannatech',
            msgJ,
            pika.BasicProperties(content_type='text/plain'
            )
        )
    ch2.basic_publish('',
            'plannatech',
            msgJ,
            pika.BasicProperties(content_type='text/plain'
            )
        )'''
    '''try:
        #print(channels[1])
        #print(type(channels[1]))
        channels[1].basic_publish('',
            'plannatech',
            msgJ,
            pika.BasicProperties(content_type='text/plain'
            )
        )
    except Exception as ex:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
    try:
        #print(type(channels[2]))
        channels[2].basic_publish('',
            'plannatech',
            msgJ,
            pika.BasicProperties(content_type='text/plain'
            )
        )
    except Exception as ex:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
    ch.basic_ack(delivery_tag=method.delivery_tag)	'''

def runClient_plannatech():
    global rcon, rchan, reconTotalCount, reconCount
    context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    firstRun = True
    while(True):
        try:
            reconTotalCount += 1
            reconCount +=1
            if reconCount >5 or firstRun:
                print("reconCount:",reconCount)                        
                reconCount = 0
                firstRun = False
            time.sleep(5)        
            try:            
                now = datetime.now()
                dt_sting = now.strftime("%d/%m/%Y %H:%M:%S")
                print(Fore.BLUE,dt_sting,"XXXXXXXXXXXXXXXXXXXXXX Connecting...")			
                rcon = pika.BlockingConnection(app_config.remote_rabbitmq_params(heartbeat=20))
                print(Fore.LIGHTGREEN_EX,dt_sting,"Connected")
                rchan = rcon.channel()
                rchan.basic_qos(prefetch_count=100)
                rchan.basic_consume(queue=app_config.remote_queue_name(),on_message_callback=on_message)			
                print(Fore.GREEN,dt_sting,"Consuming")
                try:
                    rchan.start_consuming()
                    
                except KeyboardInterrupt:
                    rchan.stop_consuming()
                    rcon.close()
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
        except pika.exceptions.AMQPConnectionError:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)

def publisher(params):
    (hostName,q,idx,target_qsize) = params
    nowTS=0
    try:
        con = amq_connect(hostName)
        ch = con.channel()
        q2 = ch.queue_declare(queue='plannatech',passive=True)
    except Exception as ex1:
        pass
    try:
        while True:        
            try:
                time.sleep(0.0001)
                elapsed=time.time()-nowTS
                if elapsed > 5:
                    nowTS = time.time()
                    print(idx,"queue:",q.qsize())
                connected = False
                try:
                    if ch.connection.is_open:                
                        connected = True
                    else:
                        print(Fore.RED, "connection closed to",hostName)
                except Exception as ex:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)
                try:
                    if not connected:
                        print(Fore.RED,"Not connected")
                        con = amq_connect(hostName)
                        ch = con.channel()
                        ch.confirm_delivery()
                        q2 = ch.queue_declare(queue='plannatech',passive=True)
                    if not q.empty():
                        tit = q.get()

                        ch.basic_publish('exch_plannatech',
                            'plannatech',
                            tit,
                            pika.BasicProperties(content_type='text/plain'
                            )
                        )
                        target_qsize=q2.method.message_count                     
                except Exception as ex:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)
            except Exception as ex:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                
                #print(".",end='')
    except Exception as e2:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)





def runClient_plannatech_test():
    global rcon, rchan
    context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    #ch.basic_consume(queue="dds-BetSlipRTv4",
    #				on_message_callback=on_message, auto_ack=True)
    firstRun=True
    while(True):
        time.sleep(1)        
        try:            
            now = datetime.now()
            dt_sting = now.strftime("%d/%m/%Y %H:%M:%S")
            print(Fore.BLUE,dt_sting,"XXXXXXXXXXXXXXXXXXXXXX Connecting plannatech test...")			
            rcon = pika.BlockingConnection(app_config.local_rabbitmq_params(heartbeat=20))

            print(Fore.LIGHTGREEN_EX,dt_sting,"Connected")
            rchan = rcon.channel()
            rchan.basic_qos(prefetch_count=10)
            #channel.basic_consume('_4620_', on_message)
            rchan.basic_consume(queue="garbagetest",on_message_callback=on_message)			
            #channel.basic_consume(queue="_4620_",on_message_callback=on_message_client, auto_ack=True) 
            print(Fore.GREEN,dt_sting,"Consuming")
            try:
                rchan.start_consuming()
                
            except KeyboardInterrupt:
                rchan.stop_consuming()
                rcon.close()
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

#################################################################################3
### FUCK FUCK FUCK THIS
#################################################################################3

def runClient_local_old(params):
    (hostName,channels,idx,chandict) = params
    
    #ch.basic_consume(queue="dds-BetSlipRTv4",
    #	
    #			on_message_callback=on_message, auto_ack=True)
    try:
        con = amq_connect(hostName)
        channels[idx] = con.channel()
        channels[idx].basic_consume(queue="shovel_command_plannatech",on_message_callback=on_command_message)
        channels[idx].start_consuming()

    except Exception as ex:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
    while(True):        
        time.sleep(1)
        
        connected = False 
               
        try:
            if channels[idx].connection.is_open:                
                connected = True
            else:
                print(Fore.RED, "connection closed to",hostName)
        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
        try:
            if not connected:
                print(Fore.RED,"Not connected")
                con = amq_connect(hostName)
                channels[idx] = con.channel()
                channels[idx].confirm_delivery()
            
            
        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)


def runClient_local1(params):
    (hostName,channels,idx,chandict) = params
    global ch1
    #ch.basic_consume(queue="dds-BetSlipRTv4",
    #	
    #			on_message_callback=on_message, auto_ack=True)
    try:    
        con = amq_connect(hostName)
        ch1 = con.channel()
    except Exception as ex:
        pass
    while(True):        
        time.sleep(0.01)
        connected = False 
               
        try:
            if ch1.connection.is_open:                
                connected = True
            else:
                print(Fore.RED, "connection closed to",hostName)
        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
        try:
            if not connected:
                print(Fore.RED,"Not connected")
                con = amq_connect(hostName)
                ch1 = con.channel()
                ch1.confirm_delivery()
            
            
        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)

def runClient_local2(params):
    (hostName,channels,idx,chandict) = params
    global ch2
    #ch.basic_consume(queue="dds-BetSlipRTv4",
    #	
    #			on_message_callback=on_message, auto_ack=True)
    try:
        con = amq_connect(hostName)
        ch2 = con.channel()
    except Exception as ex:
        pass
    while(True):        
        time.sleep(0.01)
        connected = False 
               
        try:
            if ch2.connection.is_open:                
                connected = True
            else:
                print(Fore.RED, "connection closed to",hostName)
        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
        try:
            if not connected:
                print(Fore.RED,"Not connected")
                con = amq_connect(hostName)
                ch2 = con.channel()
                ch2.confirm_delivery()
            
            
        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)




def runClient_discard(params):
    (hostName,queueName) = params
    context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    #ch.basic_consume(queue="dds-BetSlipRTv4",
    #				on_message_callback=on_message, auto_ack=True)
    while(True):        
             
        try:                        
            now = datetime.now()
            dt_sting = now.strftime("%d/%m/%Y %H:%M:%S")
            print(Fore.BLUE,dt_sting,"XXXXXXXXXXXXXXXXXXXXXX Connecting...")			
            params = app_config.local_rabbitmq_params(heartbeat=20)
            params.host = hostName
            lcon = pika.BlockingConnection(params)
            print(Fore.LIGHTGREEN_EX,dt_sting,"Connected")
            lchan = lcon.channel()
                    
            lchan.basic_qos(prefetch_count=100)
            lchan.basic_consume(queue=app_config.local_queue_name(),on_message_callback=on_discard_message)			
            print(Fore.GREEN,dt_sting,"Consuming discard channel")
            try:
                lchan.start_consuming()
                
            except KeyboardInterrupt:
                lchan.stop_consuming()
                lcon.close()
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
        time.sleep(100)     


target_qsize = 0

if sys.argv[1] == 'shovel':
    
    hostName1, hostName2 = app_config.shovel_publish_hosts()
    
    '''t = threading.Thread(target=runClient_local_old,args=((hostName1,channels,1,chandict),)) #, args=(json.loads(body))
    t.start()
    threads.append(t)

    
    t = threading.Thread(target=runClient_local_old,args=((hostName2,channels,2,chandict),)) #, args=(json.loads(body))
    t.start()
    threads.append(t)
    time.sleep(1)
    '''
    '''t = threading.Thread(target=publisher,args=((hostName1,q,1,target_qsize),)) #, args=(json.loads(body))
    t.name = "publisher 2"
    t.start()
    threads.append(t)
    time.sleep(1)'''
    
    t = threading.Thread(target=publisher,args=((hostName1,q,2,target_qsize),)) #, args=(json.loads(body))
    t.name = "publisher 2"
    t.start()
    threads.append(t)
    time.sleep(1)
   

    '''t = threading.Thread(target=publisher,args=((hostName2,q2,2),)) #, args=(json.loads(body))
    t.start()
    threads.append(t)
    time.sleep(1)


    t = threading.Thread(target=publisher,args=((hostName2,q2,2),)) #, args=(json.loads(body))
    t.start()
    threads.append(t)
    time.sleep(1)
    '''
    

    '''   t = threading.Thread(target=publisher,args=((hostName1,q),)) #, args=(json.loads(body))
    t.start()
    threads.append(t)
    time.sleep(1)
    '''



    '''time.sleep(2)
    t = threading.Thread(target=runClient_plannatech_test) #, args=(json.loads(body))
    t.start()
    threads.append(t)'''

    time.sleep(2)
    t = threading.Thread(target=runClient_plannatech) #, args=(json.loads(body))
    t.name = "runClient_plannatech"
    t.start()
    threads.append(t)
    nowTS = 0
    while True:
        time.sleep(0.01)
        try:
            elapsedTS=time.time()-nowTS	
            if elapsedTS>5:
                stat = {}
                tstat = []
                nowTS = round(time.time())
                #print(elapsedTS)
                for t in threads:            
                    print(t.name,t.is_alive())
                    nowTS = time.time()
                    thisThread = {}
                    thisThread["name"] = t.name 
                    thisThread["status"] = t.is_alive()
                    thisThread["lastSeen"] = round(time.time())
                    tstat.append(thisThread)
                stat["qsize"] = q.qsize()
                stat["target_qsize"] = target_qsize
                stat["tstat"] = tstat
                stat["reconCount"] = reconCount
                stat["reconTotalCount"] = reconTotalCount
                #print(json.dumps(tstat))
                mq_client.publish('shovel/plannatech',json.dumps(stat,indent=2),retain=True)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)


if sys.argv[1] == 'discard':
    t = threading.Thread(target=runClient_discard,args=(('localhost',sys.argv[2]),)) #, args=(json.loads(body))
    t.start()
    threads.append(t)

    '''t = threading.Thread(target=runClient_discard,args=(('s50.sandstonesourcing.com',sys.argv[2]),)) #, args=(json.loads(body))
    t.start()
    threads.append(t)'''
