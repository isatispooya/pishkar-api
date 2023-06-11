import json
import timedate
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
from assing import NCtName
import timedate

from lincens import lincensList

client = pymongo.MongoClient()
pishkarDb = client['pishkar']


def lincenslist(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        lincenslist = []
        for i in lincensList.keys():
            key = lincensList[i]
            for k in key:
                lincens = i + ' - ' + k
                lincenslist.append(lincens)
        return json.dumps({'replay':True,'lincenslist':lincenslist})
    else:
        return ErrorCookie()


def getsub(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['sub'].find({'username':username},{'_id':0}))
        try:df['name'] = df['name'].fillna('')
        except:df['name'] = ''
        df['level'] = df['level'].fillna('شخص مشاور')
        df = df.fillna(False)
        if len(df)==0:
            return json.dumps({'replay':False})
        getlincenslist = json.loads(lincenslist(data))['lincenslist']
        for i in getlincenslist:
            if i not in df.columns:
                df[i] = False
        for i in df.columns:
            if i not in getlincenslist and i!='username' and i!='subPhone' and i!='name' and i!='level':
                df = df.drop(columns=[i])
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df,'lincenslist':getlincenslist})
    else:
        return ErrorCookie()



