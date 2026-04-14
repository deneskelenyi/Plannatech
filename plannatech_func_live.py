import http.client
from inspect import trace
import json, os, sys, datetime, urllib.request, requests, time
import multiprocessing

from colorama import Fore, Back, Style
import re
import hashlib
import pymemcache
import threading
import time
import random
import queue
import mysql.connector, os, time, sys,re
#import dbcon
import plannatech_func as pf
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
import broker as mqb
import pika
import config as app_config

app_config.load_env()
mcc = pymemcache.Client(app_config.memcache_address())
import traceback
from  multiprocessing import Process
max_worker_count=8
chunk_thread = ThreadPoolExecutor(max_workers=max_worker_count)


#chunk_thread = ProcessPoolExecutor(max_workers=max_worker_count)


#dbcon.reconnect(1)
#mydb = dbcon.mydb
#myconn=mydb.get_connection()
#curs_prep = mydb.cursor(prepared=True)  
#curs_dict = mydb.cursor(dictionary=True)
#curs_prep = myconn.cursor(prepared=True)  
#curs_dict = myconn.cursor(dictionary=True)
threads = []
gameList = []

def connect_amq_local(clientName):
    print("reconnect ... ",clientName)
    try:
        c = pika.BlockingConnection(app_config.local_rabbitmq_params(clientName, heartbeat=5))
        return c
    except Exception as ex:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)



def qreader(params):
    (q,qid,gameIdList,missList,pstatus) = params
    #mq_client={}
    #mq_client[qid]=mqb.init_client("plannatech_client_"+str(qid))
    thisId="qreader_"+str(qid)
    print("running",qid,end='')
    #print(q.qsize())
    #return 0
    nowTS=time.time()
    last_proctime = 0
    msgCnt=0
    msgCntHold=0
    discardCNT=0
    while True:		
        
        time.sleep(0.001)
        tid=0
        elapsedTS=time.time()-nowTS    
        if elapsedTS>5:
            #print(elapsedTS)
            msgPersec=(msgCnt-msgCntHold)/elapsedTS
            msgCntHold = msgCnt
            
            nowTS=time.time()
            d_msg={
                
                "msgCnt":msgCnt,
                "msgPersec":round(msgPersec,3),
                "queue size":q.qsize(),				
                "process id":qid,				
                "last proctime":round(last_proctime,3),																
            }
            pstatus[thisId]=d_msg
            '''try:
                mq_client[qid].publish('plannatech_status',json.dumps(d_msg))
            except Exception as ex_mq:
                print(ex_mq)
            '''
            t_msg={}
            tc=0
            aliveCount=0
            '''try:
                for ti in threads:
                    #print(ti)
                    tc+=1										
                    
                    aliveList=[]
                    if ti.isAlive():
                        aliveCount+1
                        aliveList.append(ti.ident)						
                t_msg["aliveList"] = aliveList
                t_msg["aliveCount"]=aliveCount
                    #t_msg["t.isAlive."+str(tc)]["ident"]=ti.ident
                    #t.isAlive()
                    #print(t_msg)
                try:
                    mq_client.publish('plannatech_threads',json.dumps(t_msg,indent=2))
                    pass
                except Exception as ex_mq:
                    print(ex_mq)
            except:
                print(traceback.format_exc())
                pass
            '''


        try:
            #print(qid,q.qsize())
            #if not q.empty() and chunk_thread._work_queue.qsize()<max_worker_count:
            #if not q.empty() and len(threads)<30:
            aliveCount=0		
            #if not q.empty() and aliveCount < 50:
            #if not q.empty() and len(chunk_thread._processes)<max_worker_count:
            #if not q.empty() and chunk_thread._work_queue.qsize()<max_worker_count:
            if not q.empty():
                #time.sleep(0.01)
                #msgCount+=1
                #print("msgCount:",msgCount)
                try:
                    tit=q.get()		
                    msgCnt+=1			
                    #print(qid)
                    #print(type(tit))
                    '''with open('FIFO', 'w') as fifo:
                        try:
                            fifo.write(tit)
                            fifo.write("\n\n")
                            fifo.close()
                        except Exception as e:
                            print(e)
                    '''
                    time_now=time.time()
                    #gameIdList.append(1234)

                    
                    #print(gameIdList)
                    ret = processChunk(tit,qid,gameIdList,missList)
                    if ret ==9:
                        discardCNT+=1
                    #chunk_thread.submit(processChunk,(tit))
                    
                    #t=threading.Thread(target=processChunk,args=(tit,))
                    #t.start()
                    #threads.append(t)					
                    last_proctime=time.time()-time_now
                except:
                    pass
            else:
                #print(Fore.WHITE+".",end="")
                pass

                    
                    
        except Exception as e2:
            print("e2",e2)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
        
    return 0 


def update_odd(sb_offer_id,sb_contestant_id,odd,printit):
    ptuple=(sb_offer_id,sb_contestant_id)
    #ptuple=('20703604_prop','1526189')
    #ptuple=()
    if "_prop" not in sb_offer_id:
        tableTuple=('contestant','contestant_ff')
    else:
        tableTuple=('contestant_ff',)
    for table in tableTuple:
        #print(sb_offer_id,sb_contestant_id,table)
        sql = '''
        select * from '''+table+''' 
        where 
        sbid=31
        and sb_offer_id=%s
        and sb_contestant_id=%s
        limit 1
        '''
        #print(sql,ptuple)
        curs_dict.execute(sql, ptuple)
        #print("bla")
        res2 = curs_dict.fetchall()
        myconn.commit()
        #print(Fore.YELLOW,type(res2))										
        #print(Fore.GREEN,res2)
        if type(res2) is list:
            if(len(res2)==1):
                #print(len(res2))	
                row=res2[0]
                #print(Fore.CYAN,row)
                old_odd=row["odd"]
                
                #print(float(old_odd)==float(odd))
                if float(old_odd)==float(odd):
                    if(printit ):
                        print(Fore.WHITE, "No change")
                        print(Fore.WHITE,"compare" ,old_odd*1,odd*1)
                else:
                    sqlup = '''
                    update '''+table+'''
                    set odd=%s
                    where
                    sbid=31
                    and sb_offer_id=%s
                    and sb_contestant_id=%s
                    limit 1
                    '''
                    uptuple=(odd,sb_offer_id,sb_contestant_id)

                    #if not myconn.is_connected():
                    #	mydb.reconnect(1)
                    curs_prep.execute(sqlup,uptuple)
                    myconn.commit()
                    
                    if printit:
                        curs_dict.execute(sql, ptuple)
                        res2 = curs_dict.fetchall()
                        myconn.commit()
                        newest_odd=res2[0]["odd"]
                        print(Fore.GREEN,"changed:",sb_offer_id,sb_contestant_id,old_odd,odd,newest_odd)
                        #print(Fore.GREEN,)




def checkIDGame(idGame,missList,gameIdList):
    idx="pnt_i_"+str(idGame)	
    #global missList,gameIdList
    #missList.append(idGame)
    #missingQueue.put_nowait(idGame)
    #print(idGame)
    #print(len(gameIdList))
    try:
        #print(len(gameIdList))
        if idGame in gameIdList:
            #print("found",idGame)
            pass
        else:
            gameIdList.append(idGame)
            if idGame in missList:
                pass
                #print("+",end='')
            else:	
                #print("-",end='')
                missList.append(idGame)

        '''mcrow=mcc.get(idx)
        if(mcrow is None):					
            #missList[idGame]=idGame			
            #print("before",len(missList))
            if idGame in gameIdList:
                print("XXXX")
            missList.append(idGame)
            if(len(missList)>5):
                mlstored=missListStored=(mcc.get("missList"))
                if mlstored is None:
                    mlstored=[]
                newMissList=list(set(missList+mlstored))
                mcc.set("missList",json.dumps(newMissList))			
            #print("after",len(missList))
            pass
            #print(Fore.RED,".",end='')                        
        else:
            #print(idx)
            pass'''
    except:
        pass
    #print("misslist:",len(missList))
    
        
def missingHandler(missList,oddict,oddict_ff):
    
    nowTS=time.time()
    while True:
        time.sleep(1)
        #print("Ran miss:",len(missList))
        #print(missingQueue.qsize())
        #print(missList,missList)
        try:
            elapsedTS=time.time()-nowTS    
            if elapsedTS>60:
                nowTS=time.time()
                #print(Fore.YELLOW,"missList,",len(missList))
                if(len(missList)>5):				
                    idGameList=[]
                    for key in missList:
                        idGameList.append(key)
                    missList[:]=[]

                    print("pulling getgamelistinfo")
                    gameListInfo = pf.pull_GetGameListInfo(idGameList)
                    #print(gameListInfo)
                    print(type(gameListInfo["Games"]))
                    #print("gameListInfo",len(gameListInfo["Games"]))
                    pf.gameloop(False,True,gameListInfo,oddict,oddict_ff)
                    #pf.gameloop(True,True,gameListInfo,oddict,oddict_ff) #ff disabled
        except Exception as e:
            print("missingHandler",e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
        #print(missList)
        #if(len(missList)%10 == 0 ):
        #	print("misslist:",len(missList))
            


#bigdocQ = mp.Queue(500000)
#bigdocQFF = mp.Queue(500000)


def publishHandler(thisQue,isFF,phqid,pstatus_p):
    
    if isFF:
        thisId="publisher_ff_"+str(phqid)
    else:
        thisId="publisher_"+str(phqid)
    #mq_client_ph=mqb.init_client(clientName)
    c = None
    try:
        c=connect_amq_local(thisId)
        ch = c.channel()
        
    except Exception as ex:
        pass
    bigdoc=[]
    nowTS=time.time()
    nowTS_2=time.time()
    last_proctime = 0
    msgCnt=0
    msgCntHold=0
    time.sleep(0.01)
    lastPush=0
    isPushing=0
    lastPushSize=0
    pushTS=0
    while True:		
        
        tid=0
        elapsedTS=time.time()-nowTS    
        elapsedTS_2=time.time()-nowTS_2	
        if elapsedTS>5:
            #print(elapsedTS)
            msgPersec=(msgCnt-msgCntHold)/elapsedTS
            msgCntHold = msgCnt
            nowTS=time.time()
            d_msg={
                            
                "msgCnt":msgCnt,
                "msgPersec":round(msgPersec,2),
                "queue size":thisQue.qsize(),				
                "process id":phqid,				
                "last proctime":round(last_proctime),
                "bigdoc pending":len(bigdoc),	
                "lastPush ago":round(lastPush),
                "push in": round(elapsedTS_2),
                "lastPushSize":lastPushSize,
                "isPushing":isPushing,
                "now":time.time(),
                "pushTS":pushTS
            }
            pstatus_p[thisId]=d_msg

            '''try:
                mq_client_ph.publish('plannatech_status',json.dumps(d_msg))
            except Exception as ex_mq:
                print(ex_mq)
                '''

        if not thisQue.empty():                                
            try:
                qitem=thisQue.get()
                if isFF:
                    #print(Fore.RED,".",end="")
                    pass
                else:
                    msgCnt+=1
                    #print(Fore.BLUE,".",end='')
                    try:
                        while not ch.connection.is_open:
                            try:
                                print("reconnecting",thisId)
                                c=connect_amq_local(thisId)
                                ch = c.channel()
                                time.sleep(1)
                            except Exception as exrecon:
                                exc_type, exc_obj, exc_tb = sys.exc_info()
                                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                print(exc_type, fname, exc_tb.tb_lineno)
                            #print(type(qitem))
                        ch.basic_publish('',
                            'lines_db_contest',
                            json.dumps(qitem),
                            pika.BasicProperties(content_type='text/plain',
                            delivery_mode=1)
                            )
                    except Exception as exPika:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        print(exc_type, fname, exc_tb.tb_lineno)
                    #print(Fore.GREEN,".",end='')
                    
                    #for item in qitem:
                    #    bigdoc.append(item)
            except:                
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
        else:
            time.sleep(0.0001)


def publishHandler_(thisQue,isFF,phqid,pstatus_p):
    
    if isFF:
        thisId="publisher_ff_"+str(phqid)
    else:
        thisId="publisher_"+str(phqid)
    #mq_client_ph=mqb.init_client(clientName)
    c = None
    try:
        c=connect_amq_local(thisId)
        ch = c.channel()
        
    except Exception as ex:
        pass
    bigdoc=[]
    nowTS=time.time()
    nowTS_2=time.time()
    last_proctime = 0
    msgCnt=0
    msgCntHold=0
    time.sleep(0.01)
    lastPush=0
    isPushing=0
    lastPushSize=0
    pushTS=0
    while True:		
        time.sleep(0.001)
        tid=0
        elapsedTS=time.time()-nowTS    
        elapsedTS_2=time.time()-nowTS_2	
        if elapsedTS>5:
            #print(elapsedTS)
            msgPersec=(msgCnt-msgCntHold)/elapsedTS
            msgCntHold = msgCnt
            nowTS=time.time()
            d_msg={
                            
                "msgCnt":msgCnt,
                "msgPersec":round(msgPersec,2),
                "queue size":thisQue.qsize(),				
                "process id":phqid,				
                "last proctime":round(last_proctime),
                "bigdoc pending":len(bigdoc),	
                "lastPush ago":round(lastPush),
                "push in": round(elapsedTS_2),
                "lastPushSize":lastPushSize,
                "isPushing":isPushing,
                "now":time.time(),
                "pushTS":pushTS
            }
            pstatus_p[thisId]=d_msg

            '''try:
                mq_client_ph.publish('plannatech_status',json.dumps(d_msg))
            except Exception as ex_mq:
                print(ex_mq)
                '''

        #time.sleep(0.001)
        if not thisQue.empty():			
            try:
                qitem=thisQue.get()
                msgCnt+=1
                for item in qitem:
                    bigdoc.append(item)
            except:
                
                pass
        else:
            #print(thisQue.qsize())
            pass

        '''print("len",len(bigdoc),(len(bigdoc)>200))
        print("elapsed",elapsedTS,(elapsedTS>30))
        print
        print("wtf",(len(bigdoc)>200 or elapsedTS_2>30))'''
        if(elapsedTS_2 > 30 and len(bigdoc)==0):
            elapsedTS_2=0
            nowTS_2=time.time()
        if (len(bigdoc)>100 or (elapsedTS_2>30 and len(bigdoc)>0)):
            lastPush=elapsedTS_2
            nowTS_2=time.time()
            bigdoc_copy=bigdoc
            lastPushSize=len(bigdoc_copy)
            bigdoc=[]
            #print(len(bigdoc_copy))
            pushTS=time.time()
            isPushing=1
            #d_msg["pushStart"]=
            d_msg["bigdoc pending"]=len(bigdoc)
            d_msg["isPushing"]=isPushing
            d_msg["pushTS"]=pushTS
            pstatus_p[thisId]=d_msg
            if(thisQue.qsize()>1000):
                p3=mp.Process(target=pf.publishData, args=(bigdoc_copy,False,31,isFF)) #notrunning
                p3.start()
            else:
                pf.publishData(bigdoc_copy,False,31,isFF)			#notrunning
            isPushing=0
            d_msg["isPushing"]=isPushing			
            pstatus_p[thisId]=d_msg

            last_proctime=time.time()-pushTS

            #print("bigdocQ",thisQue.qsize(),"bigdoc",len(bigdoc),"bigdoc_copy",len(bigdoc_copy))
        #print("bigdocQ",bigdocQ.qsize(),"bigdoc",len(bigdoc))




'''t = threading.Thread(target=publishHandler, args=(bigdocQ,False)) #, args=(json.loads(body))
t.start()
threads.append(t)

t = threading.Thread(target=publishHandler, args=(bigdocQFF,True)) #, args=(json.loads(body))
t.start()
threads.append(t)

t = threading.Thread(target=publishHandler, args=(bigdocQ,False)) #, args=(json.loads(body))
t.start()
threads.append(t)

t = threading.Thread(target=publishHandler, args=(bigdocQFF,True)) #, args=(json.loads(body))
t.start()
threads.append(t)

t = threading.Thread(target=publishHandler, args=(bigdocQ,False)) #, args=(json.loads(body))
t.start()
threads.append(t)

t = threading.Thread(target=publishHandler, args=(bigdocQFF,True)) #, args=(json.loads(body))
t.start()
threads.append(t)
'''

manager = mp.Manager()
pstatus_p=manager.dict()

bigdocQFF={}
bigdocQ={}

for i in range(0,2):
    bigdocQFF[i] = mp.Queue(10000)
    bigdocQ[i] = mp.Queue(10000)
    print("start publishHandler FF",i)
    p1=mp.Process(target=publishHandler, args=(bigdocQFF[i],True,i+8,pstatus_p))
    p1.start()
    print("start publishHandler",i)
    p2=mp.Process(target=publishHandler, args=(bigdocQ[i],False,i,pstatus_p))
    p2.start()


missingQueue = mp.Queue(1000)


def findSmallestQueue(qs,qid):
    smallest=99999999	
    smallestId=qid
    #for qi in qs:
    for qi in sorted(qs,key=lambda x: random.random()):
        #print(qi,qs[qi].qsize())
        thisSize=qs[qi].qsize()
        if thisSize<smallest:
            smallest=thisSize
            smallestId=qi
    if qid!=smallestId:
        #print(qid,smallestId,qs[qid].qsize(),smallest)
        pass

    return smallestId



def processBodyChunk(b,qid,missList,gameIdList):
    ret = 0
    try:
        if "mkt" in b:
            bigdoc = []
            mkt = b["mkt"]
            gid = b["gid"]								
            checkIDGame(gid,missList,gameIdList)
            ins={}
            ins["sbid"]=31
            ins["sb_contest_id"]=gid
            ins["bo_only"]=1

            ins["bo"] = pf.insertBetOfferFromLiveData(gid,mkt,mcc)
            
            if type(ins["bo"]) is list:
                if len(ins["bo"])>0:
                    bigdoc.append(ins)
                    #print("publishing")
                    pushindex=findSmallestQueue(bigdocQ,qid%4)
                    if not bigdocQ[pushindex].full():
                        bigdocQ[pushindex].put_nowait(bigdoc)
                        ret = 1
                    else:
                        ret = 9			
                    pushindex=findSmallestQueue(bigdocQFF,qid%4)
                    if not bigdocQFF[pushindex].full():
                        bigdocQFF[pushindex].put_nowait(bigdoc)
                        ret = 1
                    else:
                        ret = 9
                    #print(bigdocQFF.qsize())
                    #pf.publishData(bigdoc,False,31,True)
                    #pf.publishData(bigdoc,False,31,False)
                
                else:
                    #print(mkt)
                    pass
                return 2
                    
    except Exception as ex1:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        #print(exc_type, fname, exc_tb.tb_lineno)
        #print(b)
        return 3
    return ret

def processChunk(m,qid,gameIdList,missList):
    
    #print(type(m))
    #print("---------------------------------------------------")
    #print(m)
    #print("---------------------------------------------------")
    try:
        #print(len(m))		
        #j = json.loads(m)
        j=m
        #print(Fore.GREEN,"loaded")
        #print(j["routing_key"])
        #print(j)
        if(j["routing_key"] == 'TNT'):
            ret = 0
            #print(Fore.WHITE,"TNT")
            h=j["headers"]
            b=j["body"]
            #print(b)
            try:
                if b is None:
                    return 3
                gidh=0
                
                #print("===========================")
                for i in b:					
                    
                    gid=i["gid"]
                    #print(gameIdList)
                    #print(len(gameIdList))
                    #gameIdList.append(gid)
                    #missList.append(gid)
                    if gidh!=gid:
                        bigdoc = []
                        #print(Fore.YELLOW,"-----------------")						
                        gidh=gid											
                        checkIDGame(gid,missList,gameIdList)
                        ins={}
                        ins["sbid"]=31
                        ins["sb_contest_id"]=str(gid)
                        ins["bo_only"]=1
                        ins["update_only"]=1
                        ins["bo"] = pf.insertBetOfferFromLiveDataPROP(gid,b,mcc)
                        #print(Fore.WHITE,b["mkt"])
                        #print(Fore.BLUE,"Bo length",len(ins["bo"]))
                        if type(ins["bo"]) is list:
                            if len(ins["bo"])>0:
                                bigdoc.append(ins)
                                #pf.publishData(bigdoc,False,31,True)						
                                try:
                                    pushindex=findSmallestQueue(bigdocQFF,qid%4)
                                    if not bigdocQFF[pushindex].full():
                                        bigdocQFF[pushindex].put_nowait(bigdoc)
                                        ret=1
                                    else:
                                        ret=9
                                except Exception as x0:
                                    print("x0",x0)	
                                    exc_type, exc_obj, exc_tb = sys.exc_info()
                                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                    print(exc_type, fname, exc_tb.tb_lineno)
                                    print(bigdocQFF[qid%4].qsize())
                                    ret = 3
                                #print(bigdocQFF.qsize())
                            #print("publishing")
                            #print(json.dumps(ins,indent=2))						
                            
                        
                        #pf.publishData(bigdoc,False,31,False)


                    
            
            except Exception as x1:
                print(x1)
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                ret = 3

        elif j["routing_key"] == "GAME":
            return 10
        elif j["routing_key"] in ("3","l"):	
            
            h=j["headers"]			
            b=j["body"]
            #print(type(b))
            if type(b) is dict:
                ret = processBodyChunk(b,qid,missList,gameIdList)
            if type(b) is list:
                for bi in b:
                    ret = processBodyChunk(bi,qid,missList,gameIdList)
                    
            #print(type(b))
            #print(Fore.YELLOW, h)
            #for bi in b:
            #print(Fore.CYAN,"-----------------\n")
            #print(json.dumps(j,indent=1))
        return ret
    except Exception as poop:
        print("processchunk",poop)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

        return 11


