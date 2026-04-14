#!/usr/bin/python3

from datetime import timedelta, date, datetime
import plannatech_func as pf
import json
import os,sys, re
from colorama import Fore, Back, Style
now=datetime.now()
import pymemcache
import argparse
import config as app_config
#print(now)
import multiprocessing as mp



ap = argparse.ArgumentParser()
ap.add_argument("-days","--days",required=False, help="Must be integer 1-90")
ap.add_argument("-daysfrom","--daysfrom",required=False, help="Must be integer 1-90")
ap.add_argument("-mods","--mods",required=False, help="Must be one of : both/ff/mst")
ap.add_argument("-pub","--pub",required=False, help="Must be 1 or is ignored")
ap.add_argument("-cached","--cached",required=False, help="Must be 1 or is ignored")

args = vars(ap.parse_args())

daysP=1
daysfromP=-1

if args['days'] is not None:	
	try:
		daysP=int(args['days'])
		if daysP>90:
			daysP=90
			print(Fore.YELLOW,"days parameter (",args['days'],") too large. 90 days selected.")
		elif daysP<1:
			daysP=1
			print(Fore.YELLOW,"days parameter (",args['days'],") too small. 1 day selected.")
		else:			
			print(Fore.GREEN, daysP, "days selected")
	except:
		print(Fore.RED,"days parameter must be integer (0-90)",args['days'],"is not an integer")
		sys.exit(2)

if args['daysfrom'] is not None:	
	try:
		daysfromP=int(args['daysfrom'])
		if daysfromP>90:
			daysfromP=90
			print(Fore.YELLOW,"days parameter (",args['daysfrom'],") too large. 90 days selected.")
		elif daysfromP<-5:
			daysfromP=-5
			print(Fore.YELLOW,"days parameter (",args['daysfrom'],") too small. -5 day selected.")
		else:			
			print(Fore.GREEN, daysfromP, "days selected")
	except:
		print(Fore.RED,"days parameter must be integer (0-90)",args['daysfrom'],"is not an integer")
		sys.exit(2)


mods="both"
if args['mods'] is not None:
	if args["mods"] in ('both','ff','mst'):
		mods=args["mods"]
	else:
		print(Fore.RED,"mods parameter must be one of : both/ff/mst")
		sys.exit(2)
doPublish=True
if args['pub'] is not None:
	if args['pub']=="1":
		doPublish=True
		print(Fore.GREEN,"Publising is enabled")
	elif args['pub']=="0":
		doPublish=False
		print(Fore.GREEN,"Publising is disabled")
	else:
		print(Fore.YELLOW,"Publising is ENABLED, pub must be 0/1 if specified")

isCached=False
if args['cached'] is not None:
	if args['cached']=="1":
		isCached=True
		print(Fore.GREEN,"Caching is enabled")
	elif args['cached']=="0":
		isCached=False
		print(Fore.GREEN,"Caching is disabled")
	else:
		print(Fore.YELLOW,"Caching is disabled, cached parameter must be 0/1 if specified")




now_date = datetime.today();
#print(start_date)

start_date=timedelta(days=+daysfromP)+now_date
end_date=timedelta(days=+daysP)+now_date

start_date_str=datetime.combine(start_date,datetime.min.time()).isoformat()+"Z"
end_date_str=datetime.combine(end_date,datetime.min.time()).isoformat()+"Z"
print(start_date, start_date_str)
print(end_date, end_date_str)
# 4 game, 5 game + events 7 all
manager = mp.Manager()
oddict = manager.dict()
oddict_ff = manager.dict()

'''def loadGameIdList_memcached():
	oddict_j = mcc.get("oddict")
	#print("Scheduled start, gameIdList size:",len(gameIdList))
	if oddict_j is not None:
		oddlist_stored = json.loads(oddlist_j)
		diffCnt=0	
		for odd_id in  oddlist_stored:					
			if odd_id not in oddList:
				diffCnt+=1
				#print(idGame)
				oddList.append(odd_id)
		newList=list(gameIdList)
	mcc.set("gameIdList",json.dumps(newList),36000)
'''


app_config.load_env()
mcc = pymemcache.Client(app_config.memcache_address())

def get_recursive():
	gameListJson = pf.pull_GetSchedule(start_date_str,end_date_str,7)
	#f= open('Dumped.json')
	#gameListJson=json.load(f)
	idGameList=[]
	idGameDict={}
	parentGameDict={}
	dupeCount=0
	notDupeCount=0
	disaCount=0
	enaCount=0

	cnt = 0
	for i in gameListJson["List"]:
		parentGame=i["ParentGame"]
		idGame=i["IdGame"]
		cnt+=1
		#print(cnt,parentGame)
		#if cnt<500:
		if(i["GameStat"]=="D"):
			#print("DDDDDDD")
			disaCount+=1
			continue
		else:
			enaCount+=1
		if parentGame in parentGameDict:
			#print(Fore.RED,"Dupe:---------------------------------\n")
			#print(Fore.RED,json.dumps(i,indent=3))
			#print(Fore.YELLOW,json.dumps(parentGameDict[parentGame],indent=3))
			#dupeCount+=1
			pass
		else:
			pass
			#parentGameList.append(parentGame)
			parentGameDict[parentGame]=i
			#notDupeCount+=1


		if idGame in idGameDict:
			#print(Fore.RED,"Dupe:",i,idGameDict[idGame])
			dupeCount+=1
		else:
			idGameList.append(idGame)			
			idGameDict[idGame]=i
			notDupeCount+=1
		#print(len(parentGameList))

	print("notDupeCount:",notDupeCount,"dupeCount:",dupeCount)
	print("disaCount:",disaCount,"enaCount:",enaCount)
	gameListInfo = pf.pull_GetGameListInfo(idGameList)
	return gameListInfo



if not isCached:
	gameListInfo=get_recursive()
	gameListInfoJson=gameListInfo
else:
	f= open('GameListInfo.json')
	gameListInfoJson=json.load(f)

#sys.exit(0)
#print(gameListInfoJson)

#sys.exit(0)
sportIdList=[]

maxDescLen=0

for game in gameListInfoJson["Games"]:
	sportIdList.append(game["IdRealSport"])
	
	thisLen=len(game["Description"])
	if(thisLen>maxDescLen):
		maxDescLen=thisLen
print("max len:",maxDescLen)


#pf.pull_Sports(sportIdList)
	
#sys.exit(0)

#def gameloop(isFF,doPublish, gameListInfoJson):

if mods in ('both','mst'):
	isFF=False
	pf.gameloop(isFF,doPublish,gameListInfoJson,oddict,oddict_ff)

if mods in ('both','ff'):
	isFF=True
	#pf.gameloop(isFF,doPublish,gameListInfoJson,oddict,oddict_ff) #isff disabled

f=open("oddict.json","w")	
f.write(json.dumps(dict(oddict)))
f=open("oddict_ff.json","w")	
f.write(json.dumps(dict(oddict_ff)))


mcc.set("oddict_","WTF",36000)
#mcc.set("oddict",json.dumps(dict(oddict)),36000)


#print(datetime.combine(start_date,datetime.min.time()).isoformat()+"Z") #,datetime.from_date(end_date))

