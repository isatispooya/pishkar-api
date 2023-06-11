import pymongo
import datetime
from persiantools.jdatetime import JalaliDate
import pandas as pd
from SystemMassage import splitCode
from customers import splitName
import time
client = pymongo.MongoClient()
pishkarDb = client['pishkar']



def getInsByUsernameAndEnding(username,prevDay):
    today = datetime.date.today()
    endDate = today + datetime.timedelta(days=int(prevDay))
    endDate = JalaliDate.to_jalali(endDate.year,endDate.month,endDate.day)
    endDate = str(endDate).replace('-','/')
    df = pd.DataFrame(pishkarDb['issuing'].find({'username':username,'تاريخ پایان':endDate},{'_id':0,'username':0}))
    df = df.drop_duplicates(subset=['comp','کد رایانه صدور بیمه نامه'])
    return df

def setPhoneToIns(df,username):
    df['phone'] = ''
    df['ntionalCode'] = ''
    for i in df.index:
        customer = pishkarDb['customers'].find_one({'code':splitCode(df['پرداخت کننده حق بیمه'][i]),'comp':df['comp'][i],'username':username})
        if customer != None:
            df['phone'][i] = customer['تلفن همراه']
            df['nationalCode'][i] = customer['کد ملي بيمه گذار']

    return df

def dfToListNumberText(df,prevDay,username):
    NumberText = []
    nationalCode = list(set(df['nationalCode']))
    company = pishkarDb['username'].find_one({'phone':username})
    for i in nationalCode:
        dff = df[df['nationalCode'] == i]
        if len(dff)>0:
            dic = {'phone':dff['phone'][dff.index.min()]}
            name = splitName(dff['پرداخت کننده حق بیمه'][dff.index.min()])
            endDay = dff['تاريخ پایان'][dff.index.min()]
            if len(dff)==1:
                

            text = ''

def smsNoLifeRevival():
    setting = pishkarDb['settingSms'].find({},{'_id':0})
    for i in setting:
        if int(i['time']) == int(datetime.datetime.now().hour):
            ins = getInsByUsernameAndEnding(i['username'],i['prevDay'])
            ins = setPhoneToIns(ins,i['username'])
            print(ins)





sms()