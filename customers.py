import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import timedate
import numpy as np
import random
import string
client = pymongo.MongoClient()
pishkarDb = client['pishkar']


def NCToStandards(x):
    nc = str(x).replace('nan','').replace('.0','')
    l = 10-len(nc)
    if l<10:
        nc = ('0'*l) + nc
        return nc
    else:
        return x

def splitCode(x):
    code = ''
    for i in x:
        if i in string.digits:code = code + i
    code = code.replace(' ','')
    return code

def splitName(x):
    if len(splitCode(x))>0:
        name = x[0:-len(splitCode(x))]
        if name[-5:-1]== ' کد ': name = name[-0:-5]
        if name[-4:-1]== ' کد': name = name[-0:-4]
        if name[-4:-1]== 'کد ': name = name[-0:-4]
        if name[-4:-1]== 'کد': name = name[-0:-3]
        return name
    return x

def uploadfile(cookier,file,comp):
    user = cookie(cookier)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.read_excel(file,dtype=str)
        df['کد ملي بيمه گذار'] = [NCToStandards(x) for x in df['کد ملي بيمه گذار']]
        RequiredColumns = ['بيمه گذار','کد ملي بيمه گذار','آدرس','تلفن همراه']
        for R in RequiredColumns:
            if R not in df.columns:
                return json.dumps({'replay':False, 'msg':f'ستون {R} در فایل یافت نشد'})
        df = df[['بيمه گذار','کد ملي بيمه گذار','آدرس','تلفن همراه']]

        df['code'] = [splitCode(x) for x in df['بيمه گذار']]
        df['name'] = [splitName(x) for x in df['بيمه گذار']]
        df['comp'] = comp
        df['username'] = username
        dff = pd.DataFrame(pishkarDb['customers'].find({'username':username,'comp':comp},{'_id':0}))
        if len(dff)==0:
            df = df.to_dict('records')
            pishkarDb['customers'].insert_many(df)
            return json.dumps({'replay':True})
        df = pd.concat([df,dff],keys=[0,1]).reset_index()
        df = df.sort_values(by=['level_0']).drop(columns=['level_0','level_1'])
        df = df.drop_duplicates(subset=['کد ملي بيمه گذار'],keep='first')
        df = df.to_dict('records')
        pishkarDb['customers'].insert_many(df)
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def customergroup(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['customers'].find({'username':username},{'_id':0}))
        df['count'] = 1
        df = df.groupby(by=['comp']).sum(numeric_only=True).reset_index()[['comp','count']]
        df = df.to_dict('records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()

def customermanual(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['customers'].find({'username':username},{'_id':0}))
        df = df[['name','code','کد ملي بيمه گذار','آدرس','تلفن همراه','comp']]
        df = df.fillna('')
        df = df.to_dict('records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()
    

def delCustomerGroup(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['customers'].delete_many({'username':username,'comp':data['comp']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def delCustomerManual(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['customers'].delete_one({'username':username,'comp':data['dict']['comp'], 'code':data['dict']['code']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def Edit(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        if(pishkarDb['customers'].find_one({'username':username, 'comp':data['Comp'], 'code':data['doc']['کد']})) != None:
            pishkarDb['customers'].delete_one({'username':username, 'comp':data['Comp'], 'code':data['doc']['کد']})
        pishkarDb['customers'].insert_one({'username':username, 'comp':data['Comp'], 'کد ملي بيمه گذار':data['doc']['کد ملي بيمه گذار'],'بيمه گذار':data['doc']['بيمه گذار']+' کد'+data['doc']['کد'],'تلفن همراه':data['doc']['تلفن همراه'],'آدرس':data['doc']['آدرس'],'code':data['doc']['کد'],'name':data['doc']['بيمه گذار']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()