
import http.client
import json, os, sys, datetime, urllib.request, requests, time
from colorama import Fore, Back, Style
import re
import hashlib
import pymemcache
from datetime import timedelta, date, datetime
import traceback
import pika
import config as app_config

app_config.load_env()

sports = {
"AUSSIE RULES":"Australian Rules",
"BASEBALL":"Baseball",
"BASKETBALL":"Basketball",
"BOXING":"Boxing",
"CRICKET":"Cricket",
"DARTS":"Darts",
"ESPORTS":"e-Sports",
"FOOTBALL":"American Football",
"Formula 1":"Formula ",
"GOLF":"Golf",
"HANDBALL":"Handball",
"Horse racing":"Horse Racing",
"ICE HOCKEY":"Hockey",
"MMA":"Mixed Martial Arts",
"MOTORSPORT":"Motor Sports",
"RUGBY":"Rugby",
"SNOOKER":"Snooker",
"SOCCER":"Football",
"TABLE TENNIS":"Table Tennis",
"Tennis":"Tennis",

}
'''
0 - Game
· 1 - 1st Half
· 2 - 2nd Half
· 3 - 1st Quarter
· 4 - 2nd Quarter
· 5 - 3rd Quarter
· 6 - 4th Quarter
· 7 - 1st Period
· 8 - 2nd Period
· 9 - 3rd Period
· 10 - Overtime
· 11 - 1st Inning
· 12 - 2nd Inning
· 13 - 3rd Inning
· 14 - 4th Inning
· 15 - 5th Inning
· 16 - 6th Inning
· 17 - 7th Inning
· 18 - 8th Inning
· 19 - 9th Inning
· 20 - Extra Inning
'''
def convertUS2DEC(oddsus):
    #oddsus = 0
    '''try:
        oddsus = int(re.sub("\+","",oddstr).strip())
    except Exception as e2:
        print(e2)
        print("odds parse failed")
        return "0"    	'''

    #tmp
    retval = ""
    if oddsus > 0:
        tmp = float((oddsus / 100) + 1);		
        retval = tmp
    elif oddsus == 0:
        retval = 0
    else:
        tmp = (100 / float(abs(oddsus))) + 1
        #tmp = float((oddsus / 100) + 1)		
        #out.println("kisebb (100/ " + Double.valueOf(oddsus) + ") + 1 =" + tmp);
        retval = tmp #.toString();	
    #//System.out.println(Config.ANSI_BLUE + "input:" + oddsus + ", output:" + retval);
    return str(round(retval,3))

def checkOdd_old(oddict,isFF,key,ocitem):
    ret = True
    
    if isFF:
        idx=0
    else:
        idx=1
    if idx not in oddict:
        oddict[idx]={}
        
    oddict_item=oddict[idx]

    if key in oddict_item:
        ret = True		
        lastTS_ts=ocitem["lastTS_ts"]
        odds=ocitem["odds"]
        odds_spread=ocitem["odds_spread"]
        thisItem=oddict_item[key]				
        thisitem_lastTS_ts=thisItem[0]
        thisItem_odds=thisItem[1]
        thisItem_odds_spread=thisItem[2]

        if thisItem_odds!=odds:
            print(Fore.RED)
            print(key,thisItem,odds)
        else:
            print(Fore.GREEN)
            print(key,thisItem,odds)

    else:
        lastTS_ts=ocitem["lastTS_ts"]
        odds=ocitem["odds"]
        odds_spread=ocitem["odds_spread"]		
        oddict_item[key]=(lastTS_ts,odds,odds_spread)
        
        ret = True	
        
    #print(oddict_item)
    oddict[idx]=oddict_item
    return ret

def checkOdd_doWork(oddict_,isFF,key,ocitem):
    if key in oddict_:
        ret = True		
        lastTS_ts=ocitem["lastTS_ts"]
        odds=ocitem["odds"]
        odds_spread=ocitem["odds_spread"]
        thisItem=oddict_[key]				
        thisitem_lastTS_ts=thisItem[0]
        thisItem_odds=thisItem[1]
        thisItem_odds_spread=thisItem[2]

        if thisItem_odds!=odds:
            print(Fore.RED)
            print(key,thisItem,odds)
        else:
            print(Fore.GREEN)
            print(key,thisItem,odds)

    else:
        lastTS_ts=ocitem["lastTS_ts"]
        odds=ocitem["odds"]
        odds_spread=ocitem["odds_spread"]		
        oddict_[key]=(lastTS_ts,odds,odds_spread)
        
        ret = True	
        
    #print(oddict_item)
    #oddict[idx]=oddict_item
    return ret


def checkOdd(oddict,oddict_ff,isFF,key,ocitem,):	
    if isFF:	
        return checkOdd_doWork(oddict_ff,isFF,key,ocitem)
    else:
        return checkOdd_doWork(oddict,isFF,key,ocitem)


def insertBetoffers(l,idGame,isFF,g,mcc,oddict,oddict_ff):
    boIns=[]
    if type(l) is list and isFF:
        #print(Fore.MAGENTA, "List detected")
        validCount=0
        for li in l:
            if li["ValidMoney"]:
                validCount+=1
        if validCount>1:
            item={}		
            item["type"]=g["Description"]
            #id=str(idGame)+"_"+item["type"]
            id=hashlib.md5((g["HomeTeam"]+g["VisitorTeam"]+item["type"]).encode('utf-8')).hexdigest()
            id=str(g["IdGame"])+"_prop"
            item["id"]=id
            item["oc"]=[]
            #print(Fore.GREEN,"YYYYYYY",item)
            for li in l:
                #print(Fore.CYAN,"\n",li,"\n-------------------------------")
                if li["ValidMoney"]:
                    ocitem={}
                    ocitem["id"]=li["TeamNumber"]
                    ocitem["name"]=li["TeamName"]
                    ocitem["lastTS"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    ocitem["lastTS_ts"]=round(time.time())
                    ocitem["odds"]=convertUS2DEC(li["Odds"])
                    ocitem["odds_spread"]=0

                    key=id+"_"+str(ocitem["id"])
                    if checkOdd(oddict,oddict_ff,isFF,key,ocitem):
                        item["oc"].append(ocitem)

                    
                    
                    #mcc.set("ptn_ctt_"+mcckey,json.dumps(ocitem),36000)
                else:
                    #print(Fore.RED,"Notvalidmoney")
                    pass
            #print(Fore.MAGENTA,item)
            boIns.append(item)
        else:
            #print(Fore.YELLOW,"\n-----------------------\nno valid offers found\n-------------------------\n")
            pass



    if type(l) is dict:
        if l["ValidMoney"]:
            #print(Fore.RED,"Money Detected")
            item={}		
            item["type"]="1x2"
            id=str(idGame)+"_"+item["type"]
            item["id"]=id
            item["oc"]=[]
            
            mcckey_bo=id
            #mcc.set("ptn_bo_"+mcckey_bo,json.dumps(item),36000)
            ocitem={}
            ocitem["id"]=1
            ocitem["name"]=1
            ocitem["lastTS"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ocitem["lastTS_ts"]=round(time.time())
            ocitem["odds"]=convertUS2DEC(l["HomeOdds"])
            ocitem["odds_spread"]=0
            
            key=id+"_"+str(ocitem["id"])
            if checkOdd(oddict,oddict_ff,isFF,key,ocitem):
                item["oc"].append(ocitem)
            #mcc.set("ptn_ctt_"+mcckey,json.dumps(ocitem),36000)

            ocitem={}
            ocitem["id"]=2
            ocitem["name"]=2
            ocitem["lastTS"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ocitem["lastTS_ts"]=round(time.time())
            ocitem["odds"]=convertUS2DEC(l["VisitorOdds"])
            ocitem["odds_spread"]=0

            key=id+"_"+str(ocitem["id"])
            if checkOdd(oddict,oddict_ff,isFF,key,ocitem):
                item["oc"].append(ocitem)
            
            #mcc.set("ptn_ctt_"+mcckey,json.dumps(ocitem),36000)

            if l["DrawOdds"] != 0:
                #print(Fore.RED,"Draw Detected")
                ocitem={}
                ocitem["id"]="X"
                ocitem["name"]="X"
                ocitem["lastTS"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ocitem["lastTS_ts"]=round(time.time())
                ocitem["odds"]=convertUS2DEC(l["DrawOdds"])
                ocitem["odds_spread"]=0
                key=id+"_"+str(ocitem["id"])
                if checkOdd(oddict,oddict_ff,isFF,key,ocitem):
                    item["oc"].append(ocitem)

            
            boIns.append(item)

        if l["ValidTotal"]:
            #print(Fore.RED,"Total Detected")
            item={}		
            item["type"]="total"
            id=str(idGame)+"_"+str(l["TotalOver"])+"_"+item["type"]
            item["id"]=id
            item["oc"]=[]
            mcckey_bo=id
            #mcc.set("ptn_bo_"+mcckey_bo,json.dumps(item),36000)

            ocitem={}
            ocitem["id"]="over"
            ocitem["name"]="over"
            ocitem["lastTS"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ocitem["lastTS_ts"]=round(time.time())
            ocitem["odds"]=convertUS2DEC(l["OverOdds"])
            ocitem["odds_spread"]=l["TotalOver"]

            key=id+"_"+str(ocitem["id"])
            if checkOdd(oddict,oddict_ff,isFF,key,ocitem):
                item["oc"].append(ocitem)

            ocitem={}
            ocitem["id"]="under"
            ocitem["name"]="under"
            ocitem["lastTS"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ocitem["lastTS_ts"]=round(time.time())
            ocitem["odds"]=convertUS2DEC(l["UnderOdds"])
            ocitem["odds_spread"]=l["TotalUnder"]
            key=id+"_"+str(ocitem["id"])
            if checkOdd(oddict,oddict_ff,isFF,key,ocitem):
                item["oc"].append(ocitem)

            boIns.append(item)
        
        if l["ValidSpread"]:
            #print(Fore.RED,"Spread Detected")
            item={}		
            item["type"]="spread"
            id=str(idGame)+"_"+str(l["HomeSpread"])+"_"+item["type"]
            item["id"]=id
            item["oc"]=[]
            mcckey_bo=id
            #mcc.set("ptn_bo_"+mcckey_bo,json.dumps(item),36000)

            ocitem={}
            ocitem["id"]="1"
            ocitem["name"]="1s"
            ocitem["lastTS"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ocitem["lastTS_ts"]=round(time.time())
            ocitem["odds"]=convertUS2DEC(l["HomeSpreadOdds"])
            ocitem["odds_spread"]=l["HomeSpread"]

            key=id+"_"+str(ocitem["id"])
            if checkOdd(oddict,oddict_ff,isFF,key,ocitem):
                item["oc"].append(ocitem)
            


            ocitem={}
            ocitem["id"]="2"
            ocitem["name"]="2s"
            ocitem["lastTS"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ocitem["lastTS_ts"]=round(time.time())
            ocitem["odds"]=convertUS2DEC(l["VisitorSpreadOdds"])
            ocitem["odds_spread"]=l["VisitorSpread"]
            key=id+"_"+str(ocitem["id"])
            if checkOdd(oddict,oddict_ff,isFF,key,ocitem):
                item["oc"].append(ocitem)

            boIns.append(item)
            mcckey=id+"_"+str(ocitem["id"])
            #mcc.set("ptn_ctt_"+mcckey,json.dumps(ocitem),36000)
            
    else:
        pass
        #print(Fore.RED, isFF,type(l))

    return boIns

def insertBetOfferFromLiveDataPROP(idGame,mkt,mcc):
    #print(mkt)	
    boIns=[]
    try:
        item={}		
        item["type"]="prop"
        #id=str(idGame)+"_"+item["type"]	
        id=str(idGame)+"_prop"
        item["id"]=id
        item["oc"]=[]
        #print(Fore.GREEN,"YYYYYYY",item)
        for li in mkt:
            #print(Fore.CYAN,"\n",li,"\n-------------------------------")	
            ocitem={}
            ocitem["id"]=li["tnm"]
            ocitem["name"]=""
            ocitem["lastTS"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ocitem["lastTS_ts"]=round(time.time())
            if li["odd"] is not None:
                ocitem["odds"]=convertUS2DEC(li["odd"])
            else:
                ocitem["odds"]=0
            ocitem["odds_spread"]=0
            item["oc"].append(ocitem)
            '''try:
                mcckey=id+"_"+str(ocitem["id"])
                mcc.set("ptn_ctt_"+mcckey,json.dumps(ocitem),36000)
            except:
                pass
            '''
        if len(item["oc"]) > 0:
            boIns.append(item)
    except Exception as boErr:
        print(boErr)
        print(li["odd"])
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
    return boIns

def insertBetOfferFromLiveData(idGame,mkt,mcc):
    #print(Fore.GREEN)
    boIns=[]
    try :
        if "m" in mkt:
            for mli in mkt["m"]:
                #print(mli)
                #print(Fore.RED,"Money Detected")
                item={}		
                item["type"]="1x2"
                id=str(idGame)+"_"+item["type"]
                item["id"]=id
                item["oc"]=[]
                '''try:
                    mcckey_bo="ptn_bo_"+str(id)
                    mcrow=mcc.get(mcckey_bo)		
                    if mcrow is None:
                        #print(Fore.RED,mcckey_bo)
                        mcc.set(mcckey_bo,json.dumps(item),36000)
                        pass
                    else:
                        #print(Fore.GREEN,mcckey_bo)
                        pass
                except:
                    pass
                '''

                if "h" in mli:
                    l=mli["h"]
                    ocitem={}
                    ocitem["id"]=1
                    ocitem["name"]=1
                    ocitem["lastTS"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    ocitem["lastTS_ts"]=round(time.time())
                    ocitem["odds"]=convertUS2DEC(l)
                    ocitem["odds_spread"]=0
                    item["oc"].append(ocitem)
                    '''try:
                        mcckey=id+"_"+str(ocitem["id"])
                        mcc.set("ptn_ctt_"+mcckey,json.dumps(ocitem),36000)
                    except:
                        pass
                    '''

                if "v" in mli:
                    l=mli["v"]
                    ocitem={}
                    ocitem["id"]=2
                    ocitem["name"]=2
                    ocitem["lastTS"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    ocitem["lastTS_ts"]=round(time.time())
                    ocitem["odds"]=convertUS2DEC(l)
                    ocitem["odds_spread"]=0
                    item["oc"].append(ocitem)				
                    '''
                    try:
                        mcckey=id+"_"+str(ocitem["id"])
                        mcc.set("ptn_ctt_"+mcckey,json.dumps(ocitem),36000)
                    except:
                        pass
                    '''
                if "d" in mli:
                    l=mli["d"]
                    if l!=0:
                        ocitem={}
                        ocitem["id"]="X"
                        ocitem["name"]='X'
                        ocitem["lastTS"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        ocitem["lastTS_ts"]=round(time.time())
                        ocitem["odds"]=convertUS2DEC(l)
                        ocitem["odds_spread"]=0
                        item["oc"].append(ocitem)
                        '''
                        try:
                            mcckey=id+"_"+str(ocitem["id"])
                            mcc.set("ptn_ctt_"+mcckey,json.dumps(ocitem),36000)
                        except:
                            pass
                        '''
                #print("OC size:",len(item["oc"]))
                if len(item["oc"]) > 0:
                    boIns.append(item)
    except:
        pass
    try:
        if "t" in mkt:
            for mli in mkt["t"]:
                #print(Fore.CYAN,"total",mli)	
                #print(Fore.RED,"Total Detected")
                
                
                item={}		
                item["type"]="total"
                id=str(idGame)+"_"+str(abs(mli["hp"]))+"_"+item["type"]
                item["id"]=id
                item["oc"]=[]
                '''try:
                    mcckey_bo="ptn_bo_"+str(id)
                    mcrow=mcc.get(mcckey_bo)							
                    if mcrow is None:
                        #print(Fore.RED,mcckey_bo)
                        mcc.set(mcckey_bo,json.dumps(item),36000)
                        pass
                    else:
                        #print(Fore.GREEN,mcckey_bo)
                        pass
                except:
                    pass
                '''

                ocitem={}
                ocitem["id"]="over"
                ocitem["name"]="over"
                ocitem["lastTS"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ocitem["lastTS_ts"]=round(time.time())
                ocitem["odds"]=convertUS2DEC(mli["h"])
                ocitem["odds_spread"]=abs(mli["hp"])
                item["oc"].append(ocitem)
                '''try:
                    mcckey=id+"_"+str(ocitem["id"])
                    mcc.set("ptn_ctt_"+mcckey,json.dumps(ocitem),36000)
                except:
                    pass
                '''
                ocitem={}
                ocitem["id"]="under"
                ocitem["name"]="under"
                ocitem["lastTS"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ocitem["lastTS_ts"]=round(time.time())
                ocitem["odds"]=convertUS2DEC(mli["v"])
                ocitem["odds_spread"]=abs(mli["hp"])
                item["oc"].append(ocitem)			
                '''try:
                    mcckey=id+"_"+str(ocitem["id"])
                    mcc.set("ptn_ctt_"+mcckey,json.dumps(ocitem),36000)
                except:
                    pass
                '''
                #print("OC size:",len(item["oc"]))
                if len(item["oc"]) > 0:
                    boIns.append(item)
    except:
        pass
    try:
        if "s" in mkt:	
            for mli in mkt["s"]:
                #print(Fore.YELLOW,"spread",mli)	
                #print(Fore.RED,"Total Detected")
                
                
                item={}		
                item["type"]="spread"			
                id=str(idGame)+"_"+str(mli["hp"])+"_"+item["type"]
                item["id"]=id
                item["oc"]=[]
                '''
                mcckey_bo="ptn_bo_"+str(id)				
                try:
                    mcrow=mcc.get(mcckey_bo)		
                    if mcrow is None:
                        #print(Fore.RED,mcckey_bo)
                        mcc.set(mcckey_bo,json.dumps(item),36000)
                        pass
                    else:
                        #print(Fore.GREEN,mcckey_bo)
                        pass
                except:
                    pass
                '''

                ocitem={}
                ocitem["id"]=1
                ocitem["name"]=1
                ocitem["lastTS"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ocitem["lastTS_ts"]=round(time.time())
                ocitem["odds"]=convertUS2DEC(mli["h"])
                ocitem["odds_spread"]=mli["hp"]
                item["oc"].append(ocitem)
                '''try:
                    mcckey=id+"_"+str(ocitem["id"])
                    mcc.set("ptn_ctt_"+mcckey,json.dumps(ocitem),36000)
                except:
                    pass
                '''

                ocitem={}
                ocitem["id"]=2
                ocitem["name"]=2
                ocitem["lastTS"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ocitem["lastTS_ts"]=round(time.time())
                ocitem["odds"]=convertUS2DEC(mli["v"])
                ocitem["odds_spread"]=mli["vp"]
                item["oc"].append(ocitem)			
                '''
                try:
                    mcckey=id+"_"+str(ocitem["id"])
                    mcc.set("ptn_ctt_"+mcckey,json.dumps(ocitem),36000)
                except:
                    pass
                '''

                #print("OC size:",len(item["oc"]))
                if len(item["oc"]) > 0:
                    boIns.append(item)
    except:
        pass

        #print(Fore.RED,boIns)	
    
    return boIns




def pull_GetSchedule(start_date_str,end_date_str,scheduleType):
    conn = http.client.HTTPSConnection("apis.plannatech.com")
    payload = json.dumps({
        "ScheduleType": scheduleType,
        "DateFrom": start_date_str,
        "DateTo": end_date_str
    })
    headers = {
        'Content-Type': 'application/json'
    }
    conn.request("POST", "/gameinfo/GetSchedule", payload, headers)
    res = conn.getresponse()
    data = res.read()
    d=data.decode("utf-8")
    #print(data.decode("utf-8"))
    f=open("Schedule.json","w")
    f.write(d)
    j=json.loads(d)
    #j=json.loads(d)
    #print(json.dumps(j,indent=3))
    return j

def pull_GetGameListInfo(parentGameList):
    #print(Fore.YELLOW," pull_GetGameListInfo(parentGameList):",parentGameList)
    conn = http.client.HTTPSConnection("apis.plannatech.com")
    payload = json.dumps({
    "List": parentGameList
    })
    headers = {
    'Content-Type': 'application/json'
    }
    conn.request("POST", "/gameinfo/GetGameListInfo", payload, headers)
    res = conn.getresponse()
    data = res.read()
    d=data.decode("utf-8")
    f=open("GameListInfo.json","w")	
    f.write(d)
    #print(data.decode("utf-8"))
    j=json.loads(d)
    return j

def pull_Sports(sportIdList):
    conn = http.client.HTTPSConnection("apis.plannatech.com")
    payload = json.dumps({
        "RealSportsList": sportIdList
        
    })
    headers = {
        'Content-Type': 'application/json'
    }
    conn.request("POST", "/gameinfo/GetRealSport", payload, headers)
    res = conn.getresponse()
    data = res.read()
    d=data.decode("utf-8")
    #print(data.decode("utf-8"))
    f=open("Sports.json","w")
    f.write(d)
    j=json.loads(d)
    #j=json.loads(d)
    print(json.dumps(j,indent=3))
    return j


def publishData(data,test,sbid,isFF):
    try:
        pURL = app_config.publish_url(isFF)
        if(test):
            pURL+="&test=1"
        pURL+="&sb="+str(sbid)
        r = requests.post(pURL, data=json.dumps(data))
    except Exception as e2:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print('error PHP data push')

def connect_amq_local(clientName):
    print("reconnect ... ",clientName)
    try:
        c = pika.BlockingConnection(app_config.local_rabbitmq_params(clientName, heartbeat=5))
        return c
    except Exception as ex:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)


def gameloop(isFF,doPublish, gameListInfoJson,oddict,oddict_ff):
    mcc = pymemcache.Client(app_config.memcache_address())
    gameIdList=[]
    docCount=0
    bigdoc=[]
    notGame=0
    liveCount=0
    gameCount=0
    c = None
    thisId = "gameloop_"+str(isFF)
    try:
        c=connect_amq_local(thisId)
        ch = c.channel()        
    except Exception as ex:
        pass
    for game in gameListInfoJson["Games"]:
        #print("-")
        #print(game)
        if game["PeriodDesc"]!="Game" and not isFF:
            #print("not a game")
            notGame+=1
            continue
        else:
            gameCount+=1
        ins={}
        gid = game["IdGame"]
        if gid not in gameIdList:
            gameIdList.append(gid)

        ins["sbid"]=31
        if isFF:
            game["IdGame"]			
        else:
            ins["sb_contest_id"]=game["ParentGame"]			
        ins["home"]=game["HomeTeam"]
        ins["away"]=game["VisitorTeam"]
        sport = game["RealSportDesc"]
        if sport in sports:
            sport=sports[sport]

        ins["sport"]=sport
        '''if type(game["Lines"]) is list:
            if ins["home"]==ins["away"]:
                ins["name"]=ins["home"]
            else:
                ins["name"]=game["Description"]

        else:
            if ins["home"]==ins["away"]:
                ins["name"]=ins["home"]
            else:
                ins["name"]=ins["home"]+" vs "+ins["away"]
        '''
        if ins["home"]==ins["away"]:
                ins["name"]=ins["home"]
        else:
            ins["name"]=ins["home"]+" vs "+ins["away"]
        ins["groupname"]=game["LeagueDesc"]
        ins["start"]= re.sub(r'[a-z]', ' ', str(game["GameDateTime"]).lower()).rstrip()
        dateTMP=re.sub(r'\.[^.]+$','',re.sub(r'[a-z]', ' ', str(game["GameDateTime"]).lower()).rstrip())
        #################################
        # FIX THE TIME ISSUE - 7 hours  #
        #################################
        #print('dateTMP:',dateTMP)
        date_time_obj=datetime.strptime(dateTMP, '%Y-%m-%d %H:%M:%S')
        #print("airdate before ", airdate)
        offset = +7
        new_start=(date_time_obj+timedelta(hours=offset)).strftime("%Y-%m-%d %H:%M:%S")
        #print("new formatted:",new_start)
        #print("old date:",game["GameDateTime"],"new date:",new_start)
        ins["start"]=new_start
        if game["LiveAction"]:
            ins["live"]=1	
            liveCount+=1
        else:
            ins["live"]=0
        if game["LiveGame"]:
            ins["live_state"]=game["PeriodDesc"]
        
        ffDetected=False
        if ins["home"] == ins["away"] or "special" in game["LeagueDesc"].lower() or "props" in game["LeagueDesc"].lower():
            ffDetected=True
        if "futures" in game["LeagueDesc"].lower():
            ffDetected=True
        if ins['home'].lower()=="yes" or ins['home'].lower()=="no":
            ffDetected=True
        if ins['away'].lower()=="yes" or ins['away'].lower()=="no":
            ffDetected=True
        #if(game["PropCount"]>0):
        #	ffDetected=True

        #if (not isFF and not ffDetected) or isFF: -> disabling isFF
        if (not isFF and not ffDetected):
            ins["bo"]=insertBetoffers(game["Lines"],game["IdGame"],isFF,game,mcc,oddict,oddict_ff)
            if(len(ins["bo"])>0):
                bigdoc.append(ins)
                docCount+=1
                #game["ParentGame"]
                try:
                    mcc.set("pnt_i_"+str(game["IdGame"]),json.dumps(ins),3600)
                    mcc.set("pnt_p_"+str(game["ParentGame"]),json.dumps(ins),3600)
                except:
                    pass
                #print(Fore.YELLOW,"\n======================\n")
                #print(Fore.YELLOW,json.dumps(ins,indent=3))
                #print(Fore.YELLOW,"\n======================\n")
            else:
                #print(Fore.RED,"XXXX SKIPPING INSERT",ins)
                pass
        else:
            #print(Fore.RED,"ZZZZ SKIPPING INSERT isFF;",isFF," ffDetected:",ffDetected, ins)
            pass
        #bigdoc.append(ins)
        #print((len(bigdoc)))
        if len(bigdoc)%20==0 and len(bigdoc)>0:
            if doPublish:

                print("pushing 20...",str(len(bigdoc)))
                #publishData(bigdoc,False,31,isFF)
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
                        json.dumps(bigdoc),
                        pika.BasicProperties(content_type='text/plain',
                        delivery_mode=1)
                        )
                except Exception as exPika:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)

                
            else:
                print("doPublish is off")
            bigdoc=[]
    try:
        if(len(bigdoc)>0):
            if doPublish:
                print("pushing",str(len(bigdoc)))
                #publishData(bigdoc,False,31,isFF)
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
                        json.dumps(bigdoc),
                        pika.BasicProperties(content_type='text/plain',
                        delivery_mode=1)
                        )
                except Exception as exPika:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)

            else:
                print("doPublish is off")
            
        else:
            print("no push, empty",str(len(bigdoc)))
    except Exception as ex5:
        print("shit",ex5)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        pass
    try:
        print(len(gameIdList))
        if len(gameIdList)>0:
            #mcc.set("pnt_i_"+str(game["IdGame"]),json.dumps(ins),3600)
            mcrow=mcc.get("gameIdList")
            if(mcrow is None):
                mcc.set("gameIdList",json.dumps(gameIdList),36000)
            else:
                storedList=json.loads(mcrow)	
                print("--------1------")			
                newList=list(set(storedList+gameIdList))
                print("--------2------")			
                print("size",len(gameIdList),len(storedList),len(newList))
                mcc.set("gameIdList",json.dumps(newList),36000)
    except Exception as crap:
        print("Mcc error",crap)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

    print("notGame:",notGame)
    print("gameCount:",gameCount)
    print("liveCount:",liveCount)


def gameloop_(isFF,doPublish, gameListInfoJson,oddict,oddict_ff):
    mcc = pymemcache.Client(app_config.memcache_address())
    gameIdList=[]
    docCount=0
    bigdoc=[]
    notGame=0
    liveCount=0
    gameCount=0

    for game in gameListInfoJson["Games"]:
        #print("-")
        #print(game)
        if game["PeriodDesc"]!="Game" and not isFF:
            #print("not a game")
            notGame+=1
            continue
        else:
            gameCount+=1
        ins={}
        gid = game["IdGame"]
        if gid not in gameIdList:
            gameIdList.append(gid)

        ins["sbid"]=31
        if isFF:
            game["IdGame"]			
        else:
            ins["sb_contest_id"]=game["ParentGame"]			
        ins["home"]=game["HomeTeam"]
        ins["away"]=game["VisitorTeam"]
        sport = game["RealSportDesc"]
        if sport in sports:
            sport=sports[sport]

        ins["sport"]=sport
        '''if type(game["Lines"]) is list:
            if ins["home"]==ins["away"]:
                ins["name"]=ins["home"]
            else:
                ins["name"]=game["Description"]

        else:
            if ins["home"]==ins["away"]:
                ins["name"]=ins["home"]
            else:
                ins["name"]=ins["home"]+" vs "+ins["away"]
        '''
        if ins["home"]==ins["away"]:
                ins["name"]=ins["home"]
        else:
            ins["name"]=ins["home"]+" vs "+ins["away"]
        ins["groupname"]=game["LeagueDesc"]
        ins["start"]= re.sub(r'[a-z]', ' ', str(game["GameDateTime"]).lower()).rstrip()
        dateTMP=re.sub(r'\.[^.]+$','',re.sub(r'[a-z]', ' ', str(game["GameDateTime"]).lower()).rstrip())
        #################################
        # FIX THE TIME ISSUE - 7 hours  #
        #################################
        #print('dateTMP:',dateTMP)
        date_time_obj=datetime.strptime(dateTMP, '%Y-%m-%d %H:%M:%S')
        #print("airdate before ", airdate)
        offset = +7
        new_start=(date_time_obj+timedelta(hours=offset)).strftime("%Y-%m-%d %H:%M:%S")
        #print("new formatted:",new_start)
        #print("old date:",game["GameDateTime"],"new date:",new_start)
        ins["start"]=new_start
        if game["LiveAction"]:
            ins["live"]=1	
            liveCount+=1
        else:
            ins["live"]=0
        if game["LiveGame"]:
            ins["live_state"]=game["PeriodDesc"]
        
        ffDetected=False
        if ins["home"] == ins["away"] or "special" in game["LeagueDesc"].lower() or "props" in game["LeagueDesc"].lower():
            ffDetected=True
        if "futures" in game["LeagueDesc"].lower():
            ffDetected=True
        if ins['home'].lower()=="yes" or ins['home'].lower()=="no":
            ffDetected=True
        if ins['away'].lower()=="yes" or ins['away'].lower()=="no":
            ffDetected=True
        #if(game["PropCount"]>0):
        #	ffDetected=True


        if (not isFF and not ffDetected) or isFF:
            ins["bo"]=insertBetoffers(game["Lines"],game["IdGame"],isFF,game,mcc,oddict,oddict_ff)
            if(len(ins["bo"])>0):
                bigdoc.append(ins)
                docCount+=1
                #game["ParentGame"]
                try:
                    mcc.set("pnt_i_"+str(game["IdGame"]),json.dumps(ins),3600)
                    mcc.set("pnt_p_"+str(game["ParentGame"]),json.dumps(ins),3600)
                except:
                    pass
                #print(Fore.YELLOW,"\n======================\n")
                #print(Fore.YELLOW,json.dumps(ins,indent=3))
                #print(Fore.YELLOW,"\n======================\n")
            else:
                #print(Fore.RED,"XXXX SKIPPING INSERT",ins)
                pass
        else:
            #print(Fore.RED,"ZZZZ SKIPPING INSERT isFF;",isFF," ffDetected:",ffDetected, ins)
            pass
        #bigdoc.append(ins)
        #print((len(bigdoc)))
        if len(bigdoc)%100==0 and len(bigdoc)>0:
            if doPublish:
                print("pushing 100...",str(len(bigdoc)))
                publishData(bigdoc,False,31,isFF) #notrunning
                
            else:
                print("doPublish is off")
            bigdoc=[]
    try:
        if(len(bigdoc)>0):
            if doPublish:
                print("pushing",str(len(bigdoc)))
                publishData(bigdoc,False,31,isFF) #notrunning
                
            else:
                print("doPublish is off")
            
        else:
            print("no push, empty",str(len(bigdoc)))
    except Exception as ex5:
        print("shit",ex5)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        pass
    try:
        print(len(gameIdList))
        if len(gameIdList)>0:
            #mcc.set("pnt_i_"+str(game["IdGame"]),json.dumps(ins),3600)
            mcrow=mcc.get("gameIdList")
            if(mcrow is None):
                mcc.set("gameIdList",json.dumps(gameIdList),36000)
            else:
                storedList=json.loads(mcrow)	
                print("--------1------")			
                newList=list(set(storedList+gameIdList))
                print("--------2------")			
                print("size",len(gameIdList),len(storedList),len(newList))
                mcc.set("gameIdList",json.dumps(newList),36000)
    except Exception as crap:
        print("Mcc error",crap)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

    print("notGame:",notGame)
    print("gameCount:",gameCount)
    print("liveCount:",liveCount)
