import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import datetime
import timedate
import numpy as np
client = pymongo.MongoClient()
pishkarDb = client['pishkar']
import time

def getvalue(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pishkarDb['ValueBranche'].find_one({'username':username,'name':data['Branches'], 'dateShow':data['date']['Show']},{'_id':0})
        if df == None:
            return json.dumps({'repaly':True,'df':''})
        else:
            df = df['valueBranche']
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()

def addvalue(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        valueDic = {'username':username,'name':data['Branches'],'dateShow':data['date']['Show'],'dateTimeStump':data['date']['date'],'valueBranche':data['valueBranche']}
        if pishkarDb['ValueBranche'].find_one({'username':username,'name':data['Branches'],'dateShow':data['date']['Show']})!=None:
            pishkarDb['ValueBranche'].update_one({'username':username,'name':data['Branches'],'dateShow':data['date']['Show']},{'$set':valueDic})
        else:
            pishkarDb['ValueBranche'].insert_one(valueDic)
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def copyPreviousMonth(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        copy = pishkarDb['ValueBranche'].find_one(filter={'username':username,'name':data['Branches']},sort=[('dateTimeStump',-1)])
        if copy == None:
            return json.dumps({'replay':False, 'msg':'اطلاعاتی از ماه قبل یافت نشد'})
        del copy['_id']
        copy['dateShow'] = data['date']['Show']
        copy['dateTimeStump'] = data['date']['date']
        pishkarDb['ValueBranche'].insert_one(copy)
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()