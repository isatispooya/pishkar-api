import pymongo
import datetime
from persiantools.jdatetime import JalaliDate
import pandas as pd
from SystemMassage import splitCode
from customers import splitName
import time
from sms import SendSms
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
    df['nationalCode'] = ''
    for i in df.index:
        customer = pishkarDb['customers'].find_one({'code':splitCode(df['پرداخت کننده حق بیمه'][i]),'comp':df['comp'][i],'username':username})
        if customer != None:
            df['phone'][i] = customer['تلفن همراه']
            df['nationalCode'][i] = customer['کد ملي بيمه گذار']
    return df

def dfToListNumberPerText(df,prevDay,username):
    NumberText = []
    nationalCode = list(set(df['nationalCode']))
    company = pishkarDb['username'].find_one({'phone':username})
    for i in nationalCode:
        dff = df[df['nationalCode'] == i]
        if len(dff)>0:
            phone = dff['phone'][dff.index.min()]
            name = splitName(dff['پرداخت کننده حق بیمه'][dff.index.min()])
            endDay = dff['تاريخ پایان'][dff.index.min()]
            feilds = list(dff['رشته'])
            codeIns = list(dff['کد رایانه صدور بیمه نامه'])
            dic = {'phone':phone,'name':name,'endDay':endDay,'prevDay':prevDay,'companyName':company['company'],'web':company['web'],'phonework':company['phonework'],'feilds':feilds,'address':company['address'],'codeIns':codeIns}
            NumberText.append(dic)
    return NumberText

def dicSend(dic,username,hours):
    if len(dic)>0:
        for i in dic:
            phone = i['phone']
            text = 'بیمه گذار محترم\n'+i['name']+'\n'+'با سلام و احترام اعلام میداریم که'
            if len(i['feilds'])==1:
                text = text+' بیمه نامه ی '+i['feilds'][0]
            else:
                text = text+' بیمه نامه های '
                for j in i['feilds']:
                    text = text + j + ' ,'
                text = text[:-1]
            text = text + ' شما طی '+i['prevDay']+' روز دیگر '+'منقضی خواهد شد '+' لطفا برای تمدید آن به '+i['companyName'] +' به آدرس '+i['address'] +' مراجعه و یا با شماره '+i['phonework']+'تماس بگیرید'+'\n'+i['web']
            lastSend = pishkarDb['SendedSms'].find({'username':username,'prevDay':i['prevDay'],'hours':hours,'text':text})
            if lastSend==None:
                code = SendSms('09011010959',text) # شماره مشتری موقتا با شماره خودم تغییر دادم برای تست
                for p in i['codeIns']:
                    pishkarDb['SendedSms'].insert_one({'username':username,'codeIns':p,'customer':i['name'],'codeDeliver':code,'text':text,'prevDay':i['prevDay'],'date':datetime.datetime.now(),'hours':hours})



def smsNoLifeRevival():
    setting = pishkarDb['settingSms'].find({},{'_id':0})
    for i in setting:
        if int(i['time']) == int(datetime.datetime.now().hour):
            ins = getInsByUsernameAndEnding(i['username'],i['prevDay'])
            if len(ins)>0:
                ins = setPhoneToIns(ins,i['username'])
                ins = dfToListNumberPerText(ins,i['prevDay'],i['username'])
                dicSend(ins,i['username'],i['time'])








while True:
    smsNoLifeRevival()
    time.sleep(900000)

