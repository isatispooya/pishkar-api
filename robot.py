import pymongo
import datetime
from persiantools.jdatetime import JalaliDate
import pandas as pd
from SystemMassage import splitCode
from customers import splitName
import time
import setting
from sms import SendSms , CheckDeliver
from reports import decrypt, group110, removePassed, CulcBalanceAdjust,negetiveToZiro
import pyodbc


client = pymongo.MongoClient()
pishkarDb = client['pishkar']

def sumGroup(df):
    Bede = df['Bede'].sum()
    Best = df['Best'].sum()
    LtnComm = df['LtnComm'].sum()
    dff = pd.DataFrame([{'Bede':Bede,'Best':Best,'LtnComm':LtnComm}])
    return dff


def getInsByUsernameAndEnding(username,prevDay):
    today = datetime.date.today()
    endDate = today + datetime.timedelta(days=int(prevDay))
    endDate = JalaliDate.to_jalali(endDate.year,endDate.month,endDate.day)
    endDate = str(endDate).replace('-','/')
    df = pd.DataFrame(pishkarDb['issuing'].find({'username':username,'تاريخ پایان':endDate,'additional':'اصلی'},{'_id':0,'username':0}))
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
                text = text+' بیمه نامه ی "'+i['feilds'][0]+'"'
            else:
                text = text+' بیمه نامه های '
                for j in i['feilds']:
                    text = text + '"' + j + '"' + ' ,'
                text = text[:-1]
            text = text + ' شما طی '+i['prevDay']+' روز دیگر '+'منقضی خواهد شد '+' لطفا برای تمدید آن به '+i['companyName'] +' به آدرس '+i['address'] +' مراجعه و یا با شماره '+i['phonework']+'تماس بگیرید'+'\n'+i['web']
            lastSend = pishkarDb['SendedSms'].find({'username':username,'prevDay':i['prevDay'],'hours':hours,'text':text})
            lastSend = [x for x in lastSend]
            if len(lastSend)==0:
                code = SendSms(phone,text)
                for p in i['codeIns']:
                    pishkarDb['SendedSms'].insert_one({'username':username,'codeIns':p,'customer':i['name'],'codeDeliver':code,'text':text,'prevDay':i['prevDay'],'date':datetime.datetime.now(),'hours':hours,'deliver':False})


def smsNoLifeRevival():
    setting = pishkarDb['settingSms'].find({},{'_id':0})
    for i in setting:
        if int(i['time']) == int(datetime.datetime.now().hour):
            ins = getInsByUsernameAndEnding(i['username'],i['prevDay'])
            if len(ins)>0:
                ins = setPhoneToIns(ins,i['username'])
                ins = dfToListNumberPerText(ins,i['prevDay'],i['username'])
                dicSend(ins,i['username'],i['time'])


def checkDelivers():
    df = pd.DataFrame(pishkarDb['SendedSms'].find({'deliver':False}))
    for i in df.index:
        if (df['date'][i] + datetime.timedelta(days=3))>datetime.datetime.now():
            cd = CheckDeliver(df['codeDeliver'][i])
            if cd['statusCode'] ==200:
                if int(cd['content'].decode('utf-8')) == 1:
                    pishkarDb['SendedSms'].update_one({'_id':df['_id'][i]},{'$set':{'deliver':True}})




def SmsBalance():
    config = pishkarDb['balanceSms'].find_one()
    if str(datetime.datetime.now().weekday()) == '0' and str(datetime.datetime.now().hour)==str(9):
        conn_str_db = f'DRIVER={{SQL Server}};SERVER={setting.ip_sql_server},{setting.port_sql_server};DATABASE={setting.database};UID={setting.username_sql_server};PWD={setting.password_sql_server}'
        conn = pyodbc.connect(conn_str_db)
        query = f"SELECT * FROM DOCB"
        df = pd.read_sql(query, conn)
        df['110'] = [group110(x,'03') for x in df['Acc_Code']]
        df = df[df['110']==True]        
        df = df[['Acc_Code','Bede','Best','LtnComm']]
        df = df.fillna('')
        df['LtnComm'] = df['LtnComm'].apply(decrypt)
        df['LtnComm'] = df['LtnComm'].apply(removePassed)
        df['LtnComm'] = df['LtnComm'].apply(CulcBalanceAdjust)
        df = df.groupby('Acc_Code').apply(sumGroup)
        df['balance_bede'] = df['Bede'] - df['Best']
        df = df[df['balance_bede']>=int(config['from'])]
        df['balance_best'] = df['Best'] - df['Bede']
        df['balance_bede'] = df['balance_bede'].apply(negetiveToZiro)
        df['balance_best'] = df['balance_best'].apply(negetiveToZiro)
        df['balanceAdjust'] = df['Bede'] - df['Best'] - df['LtnComm']
        conn = pyodbc.connect(conn_str_db)
        query = f"SELECT * FROM ACC"
        df_acc = pd.read_sql(query, conn)
        df_acc = df_acc[['Code','Name','Mobile']]
        df_acc = df_acc.rename(columns={'Code':'Acc_Code'})
        df = df.join(df_acc.set_index('Acc_Code')).reset_index()
        df = df.drop(columns=['level_1'])
        df = df.fillna('')
        df['balance_bede'] = df['balance_bede'].apply(int)
        df['balanceAdjust'] = df['balanceAdjust'].apply(int)

        dfAdj = df[df['balanceAdjust']>0]
        dfNoAdj = df[df['balanceAdjust']<=0]
        dfAdj['balance_bede'] = dfAdj['balance_bede'].apply(str)
        dfAdj['balanceAdjust'] = dfAdj['balanceAdjust'].apply(str)
        dfNoAdj['balance_bede'] = dfNoAdj['balance_bede'].apply(str)
        dfNoAdj['balanceAdjust'] = dfNoAdj['balanceAdjust'].apply(str)
        dfNoAdj['text'] =  'بیمه گذار محترم '+ dfNoAdj['Name'] + '\n' + 'لطفا نسبت به پرداخت بدهی خود به مبلغ ' + dfNoAdj['balance_bede'] + 'ریال به شماره کارت 6104337811317393 به نام کارگزاری بیمه ایساتیس پویا واریز نمایید.باتشکر\n035220088'
        dfAdj['text'] =  'بیمه گذار محترم '+ dfAdj['Name'] + '\n' + 'لطفا نسبت به پرداخت بدهی معوق خود به مبلغ ' + dfAdj['balanceAdjust'] + 'ریال به شماره کارت 6104337811317393 به نام کارگزاری بیمه ایساتیس پویا واریز نمایید.\nبدهی کامل شما:'+dfAdj['balance_bede']+' ریال میباشد\n035220088'
        df = pd.concat([dfNoAdj,dfAdj])
        for i in df.index:
            mobile = df['Mobile'][i]
            text = df['text'][i]
            SendSms(mobile,text)
    time.sleep(60*60)
        



while True:
    print('start loop')
    SmsBalance()
    smsNoLifeRevival()
    time.sleep(300)
    checkDelivers()
    time.sleep(1500)
    SmsBalance()

