
import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import timedate
import numpy as np
import random
import datetime
import time
import string
import re

client = pymongo.MongoClient()
pishkarDb = client['pishkar']


def splitCode(x):
    code = ''
    for i in x:
        if i in string.digits:code = code + i
    code = code.replace(' ','')
    return code


def check_number(string):
    pattern = r'\d+'  # عبارت منظم برای یافتن یک یا چند رقم عددی
    match = re.search(pattern, str(string))
    if match:
        return True
    else:
        return False


def ExpierName(x):
    num = int(x)
    if num>=0:
        return 'منقضی'
    elif num<0 and num>=-3:
        return 'اخطار'
    elif num<-3 and num>=-7:
        return 'هشدار'
    elif num<-7 and num>=-15:
        return 'اعلان'




def NCtName(cl,nc):
    if str(nc)!='nan':
        df = cl[cl['nationalCode']==nc]
        df['nationalCode'] = [str(x) for x in df['nationalCode'] ]
        if len(df)>0:
            df['full'] = df['gender'] +' '+ df['fristName'] +' '+ df['lastName']
            df = df['full'][df.index.max()]
            return df
        else:
            return 'بدون مشاور'
    else:
        return 'بدون مشاور'

def NCtNameReplace(cl,df):
    cl['nationalCode'] = [str(x) for x in cl['nationalCode'] ]
    cl['full'] = cl['gender'] +' '+ cl['fristName'] +' '+ cl['lastName']
    cl = cl[['nationalCode','full']].to_dict('records')
    for i in cl:
        df = df.replace(i['nationalCode'],i['full'])
    return df


def customerCheck(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['customers'].find({'username':username},{'_id':0}))
        df = df[['name','کد ملي بيمه گذار','تلفن همراه','comp','code']]
        df = df.set_index(['comp','code'])
        dfissuing = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0,'comp':1,'پرداخت کننده حق بیمه':1}))
        dfissuing['code'] = [splitCode(x) for x in dfissuing['پرداخت کننده حق بیمه']]
        dfissuing = dfissuing[['code','comp']]
        dfissuing['in'] = True
        dfissuing = dfissuing.set_index(['comp','code'])
        df = df.join(dfissuing).reset_index()
        df = df[df['in']==True]
        df = df.drop_duplicates(subset=['comp','code'])
        df = df[['تلفن همراه','کد ملي بيمه گذار','name','comp','code']]
        df = df.fillna('')
        dict = {}
        for i in ['تلفن همراه','کد ملي بيمه گذار']:
            dff = df[(df[i]=='') | (df[i]=='0') | (df[i]==0) | (df[i].str.len() < 10)].reset_index()
            lenn = len(dff)
            dff = dff[dff.index<3]
            sample = ''
            for j in dff.index:
                sample = sample + ' "' +dff['name'][j] + ' ' +'کد' + ' ' + dff['code'][j] + ' ' +'در' + ' ' + dff['comp'][j] +'" '
            sample = sample.replace('" "','","')
            dict[i] = {'len':lenn,'sample':sample}
        return json.dumps({'replay':True, 'df':dict})
    else:
        return ErrorCookie()


def standardFeeCheck(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['Fees'].find({'username':username},{'_id':0,'مورد بیمه':1,'رشته':1}))
        dfissuing = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0,'مورد بیمه':1,'رشته':1}))
        df = pd.concat([df,dfissuing])
        dff = pd.DataFrame(pishkarDb['standardfee'].find({'username':username},{'_id':0,'field':1}))
        dff['standardFee'] = True
        dff = dff.drop_duplicates()
        dff = dff.set_index('field')
        df = df.fillna('')
        df['Field'] = df['رشته'] + ' '+ '('+ df['مورد بیمه'] + ')'
        df['Field'] = [str(x).replace(' ()','') for x in df['Field']]
        df['Title'] = True
        df = df[df['Field']!='']
        
        
        df = df.drop_duplicates()
        df = df.drop(columns=['مورد بیمه','رشته'])
        df = df.set_index('Field')
        df = df.join(dff,how='left')
        df = df[df['standardFee']!=True]
        df = list(df.index)
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()

def counsultantFee(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['Fees'].find({'username':username},{'_id':0,'مورد بیمه':1,'رشته':1}))
        dfissuing = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0,'مورد بیمه':1,'رشته':1}))
        df = pd.concat([df,dfissuing])
        df = df.fillna('')
        df['Field'] = df['رشته'] + ' '+ '('+ df['مورد بیمه'] + ')'
        df['Field'] = [str(x).replace(' ()','') for x in df['Field']]
        df['Title'] = True
        dff = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username}))
        return json.dumps({'replay':True,'df':0})
    else:
        return ErrorCookie()

def revival(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:

        df = pd.DataFrame(pishkarDb['issuing'].find({'username':username,'additional':'اصلی'},{'_id':0,'کد رایانه صدور بیمه نامه':1,'پرداخت کننده حق بیمه':1,'تاريخ بيمه نامه يا الحاقيه':1,'comp':1,'مبلغ کل حق بیمه':1,'additional':1,'تاريخ پایان':1,'رشته':1}))
        df = df.drop_duplicates(subset=['کد رایانه صدور بیمه نامه','پرداخت کننده حق بیمه','تاريخ بيمه نامه يا الحاقيه','comp','مبلغ کل حق بیمه','additional'])
        df = df.set_index(['comp','کد رایانه صدور بیمه نامه'])
        assing = pd.DataFrame(pishkarDb['AssingIssuing'].find({'username':username},{'_id':0}))
        assing = assing.set_index(['comp','کد رایانه صدور بیمه نامه'])
        df = df.join(assing)
        Revivaldf = pd.DataFrame(pishkarDb['Revival'].find({'username':username},{'_id':0,'username':0}))
        Revivaldf = Revivaldf.set_index(['comp','کد رایانه صدور بیمه نامه'])
        df = df.join(Revivaldf).reset_index()
        df = df.fillna('No')
        df = df.reset_index()
        consultant = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username},{'_id':0}))
        if 'type' in user['Authorization'].keys():
            consultant = consultant[consultant['phone']==user['Authorization']['type']['subPhone']]
            nc = consultant.to_dict('recods')[0]['nationalCode']
            df = df[df['cunsoltant']==nc]
        else:
            if data['type'] == 'df' and data['consultant']!='all':
                df = df[df['cunsoltant']==data['consultant']]
        
        if 'consultant' in data:
            if data['consultant'] != 'all':
                df = df[df['cunsoltant']==data['consultant']]
        df['cunsoltant' ] = [NCtName(consultant,x) for x in df['cunsoltant']]
        df= df[df['تاريخ پایان']!='0']
        df['Expiration'] = [timedate.diffTime2(x) for x in df['تاريخ پایان']]
        if ('Date' in data):
            if(data['Date']['from']!=None or data['Date']['to']!=None):
                df = df[df['RevivalResponse']=='No']
                DateFrom = 0
                DateTo = 100000000000**100
                if data['Date']['from']!=None:DateFrom = timedate.dateToInt(timedate.timStumpTojalali(data['Date']['from']))
                if data['Date']['to']!=None:DateTo = timedate.dateToInt(timedate.timStumpTojalali(data['Date']['to']))
                df['dateInt'] = [timedate.dateToInt(x)>=DateFrom and timedate.dateToInt(x)<=DateTo for x in df['تاريخ پایان']]
                df = df[df['dateInt']==True]
                df = df.drop(columns=['additional','username','dateInt'])
                df['ExpirationName'] = [ExpierName(x) for x in df['Expiration']]
                df['تلفن همراه'] = ''
                df = df.to_dict('records')
                for i in range(len(df)):
                    comp = df[i]['comp']
                    code = splitCode(df[i]['پرداخت کننده حق بیمه'])
                    df[i]['تلفن همراه'] = pishkarDb['customers'].find_one({'username':username,'comp':comp,'code':code})['تلفن همراه']
                return json.dumps({'replay':True,"df":df})


        if data['type'] == 'len':
            df = df[df['RevivalResponse']=='No']
            Black = df[df['Expiration']>=0].to_dict('records')
            Red = df[df['Expiration']>=-3]
            Red = Red[Red['Expiration']<0].to_dict('records')
            Oreng = df[df['Expiration']>=-7]
            Oreng = Oreng[Oreng['Expiration']<-3].to_dict('records')
            Yellow = df[df['Expiration']>=-15]
            Yellow = Yellow[Yellow['Expiration']<-7].to_dict('records')
            return json.dumps({'replay':True,"threshold":{"Black":len(Black), "Red":len(Red), "Oreng":len(Oreng), "Yellow":len(Yellow)}})
        
        elif data['type'] == 'منقضی شده':
            df = df[df['RevivalResponse']=='No']
            df = df[df['Expiration']>=0]
            df['ExpirationName'] = [ExpierName(x) for x in df['Expiration']]
            df = df.drop(columns=['additional','username'])

        elif data['type'] == 'اخطار انقضا':
            df = df[df['RevivalResponse']=='No']
            df = df[df['Expiration']>=-3]
            df = df[df['Expiration']<=0]
            df['ExpirationName'] = [ExpierName(x) for x in df['Expiration']]
            df = df.drop(columns=['additional','username'])

        elif data['type'] == 'هشدار انقضا':
            df = df[df['RevivalResponse']=='No']
            df = df[df['Expiration']>=-7]
            df = df[df['Expiration']<-3]
            df['ExpirationName'] = [ExpierName(x) for x in df['Expiration']]
            df = df.drop(columns=['additional','username'])

        elif data['type'] == 'اعلان انقضا':
            df = df[df['RevivalResponse']=='No']
            df = df[df['Expiration']>=-15]
            df = df[df['Expiration']<-7]
            df['ExpirationName'] = [ExpierName(x) for x in df['Expiration']]
            df = df.drop(columns=['additional','username'])

        elif data['type'] == 'عدم نیاز':
            df = df[df['RevivalResponse']=='no need']
            df = df.drop(columns=['additional','username'])

        elif data['type'] == 'تمدید نشد':
            df = df[df['RevivalResponse']=='not extended']
            df = df.drop(columns=['additional','username'])

        elif data['type'] == 'تمدید شد':
            df = df[df['RevivalResponse']=='extended']
            df = df.drop(columns=['additional','username'])

        else:
            df = df[df['Expiration']>=-15]
            df['ExpirationName'] = [ExpierName(x) for x in df['Expiration']]
            df = df.drop(columns=['additional','username'])

        df['تلفن همراه'] = ''
        df = df.to_dict('records')
        for i in range(len(df)):
            comp = df[i]['comp']
            code = splitCode(df[i]['پرداخت کننده حق بیمه'])
            df[i]['تلفن همراه'] = pishkarDb['customers'].find_one({'username':username,'comp':comp,'code':code})['تلفن همراه']
            if df[i]['RevivalResponse'] != 'No':
                df[i]['ExpirationName'] = str(df[i]['RevivalResponse']).replace('no need','عدم نیاز').replace('not extended','تمدید نشد').replace('extended','تمدید شد')


        return json.dumps({'replay':True,"df":df})
    else:
        return ErrorCookie()

def lifeRevival(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        methodPay = {'12':'اقساط ماهانه','6':'اقساط دو ماهه','4':'اقساط سه ماهه','3':'اقساط چهار ماهه','2':'اقساط شش ماهه','1':'سالانه'}
        if data['Comp'] !='همه':
            df = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username,'status':'جاری','comp':data['Comp']},{'_id':0,'مدت':1,'comp':1,'تاريخ شروع':1,'تعداد اقساط در سال':1,'روش پرداخت':1,'طرح':1,'شماره بيمه نامه':1,'نام بیمه گذار':1,'ضریب رشد سالانه حق بیمه':1,"حق بیمه هر قسط \n(جمع عمر و پوششها)":1,'تاريخ  انقضاء':1}))
        else:
            df = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username,'status':'جاری'},{'_id':0,'مدت':1,'comp':1,'تاريخ شروع':1,'تعداد اقساط در سال':1,'روش پرداخت':1,'طرح':1,'شماره بيمه نامه':1,'نام بیمه گذار':1,'ضریب رشد سالانه حق بیمه':1,"حق بیمه هر قسط \n(جمع عمر و پوششها)":1,'تاريخ  انقضاء':1}))
        if len(df)==0:
            return json.dumps({"replay":False,'msg':'بیمه نامه ای یافت نشد'})
        df = df.drop_duplicates(subset=['شماره بيمه نامه'])
        consoltant = pd.DataFrame(pishkarDb['AssingIssuingLife'].find({'username':username},{'_id':0,'username':0})).set_index('شماره بيمه نامه')
        adden = pd.DataFrame(pishkarDb['AddenLife'].find({'username':username},{'_id':0,'username':0}))
        adden = adden.drop_duplicates(subset=['شماره بيمه نامه','تاريخ شروع'])
        if len(adden)>0:
            numlist = adden['شماره بيمه نامه'].to_list()
            df['adden'] = [x in numlist for x in df['شماره بيمه نامه']]
            dfadden = df[df['adden']==True]
            df = df[df['adden']==False]

            if len(dfadden)>0:
                for i in dfadden.index:
                    basic = dfadden.loc[i].to_dict()
                    basicAdden = adden[adden['شماره بيمه نامه']==basic['شماره بيمه نامه']]
                    basicAdden['startDateInt'] = [timedate.dateToInt(x) for x in basicAdden['تاريخ شروع']]
                    firstAdden = basicAdden[basicAdden['startDateInt']==basicAdden['startDateInt'].min()].to_dict('records')[0]
                    basic['تاريخ  انقضاء'] = firstAdden['تاريخ شروع']
                    basic = pd.DataFrame([basic])
                    df = pd.concat([df,basic])
                    liststart = basicAdden['startDateInt'].to_list()
                    basicAdden = basicAdden.set_index(['startDateInt']).sort_index()
                    for j in basicAdden.index:
                        row = basicAdden.loc[j].to_dict()
                        endList = [ x for x in liststart if x>j]
                        if len(endList)==0: basic['تاريخ  انقضاء'] = row['تاريخ  انقضاء']
                        else: basic['تاريخ  انقضاء'] = timedate.intToDate(min(endList))
                        basic['تاريخ شروع'] = row['تاريخ شروع']
                        basic['ضریب رشد سالانه حق بیمه'] = int(row['ضریب رشد سالانه حق بیمه'])
                        basic['تعداد اقساط در سال'] = int(row['تعداد اقساط در سال'])
                        basic['روش پرداخت'] = methodPay[str(row['تعداد اقساط در سال'])]
                        basic['حق بیمه هر قسط \n(جمع عمر و پوششها)'] = int(str(row['حق بیمه هر قسط \n(جمع عمر و پوششها)']).replace(',','')) * (100/(100+int(row['ضریب رشد سالانه حق بیمه'])))
                        df = pd.concat([df,basic])
            df = df.drop(columns='adden')

        df = df.set_index('شماره بيمه نامه').join(consoltant,how='left').reset_index()
        if data['consultant'] != 'all':
            df = df[df['consultant']==data['consultant']]
        if len(df)==0:
            return json.dumps({"replay":False,'msg':'بیمه نامه ای یافت نشد'})
        cl_consultant = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username}))
        df['consultant'] = NCtNameReplace(cl_consultant,df['consultant'])
        dff = []


        if data['Date']['from']!=None or data['Date']['to']!=None:
            DateFrom = False
            DateTo = False
            if data['Date']['from']!=None:DateFrom = timedate.dateToInt(timedate.timStumpTojalali(data['Date']['from']))
            if data['Date']['to']!=None:DateTo = timedate.dateToInt(timedate.timStumpTojalali(data['Date']['to']))
            for i in df.index:
                dic = df[df.index==i].to_dict('records')[0]
                qestlist = timedate.qestListNoLimetByTF(dic['مدت'],dic['تاريخ شروع'],dic['تعداد اقساط در سال'],dic['تاريخ  انقضاء'],DateTo,DateFrom)

                for j in qestlist:
                    ddic = dic.copy()
                    ddic['qest'] = j
                    dff.append(ddic)            
        else:
            for i in df.index:
                dic = df[df.index==i].to_dict('records')[0]
                qestlist = timedate.qestListNoLimet(dic['مدت'],dic['تاريخ شروع'],dic['تعداد اقساط در سال'],dic['تاريخ  انقضاء'])
                
                for j in qestlist:
                    ddic = dic.copy()
                    ddic['qest'] = j
                    dff.append(ddic)


        if len(dff)==0:
            return json.dumps({"replay":False,'msg':'بیمه نامه ای یافت نشد'})
        dff = pd.DataFrame(dff)
        dff['شماره بيمه نامه'] = dff['شماره بيمه نامه'].astype(str)
        dff['qest'] = dff['qest'].astype(str)
        dff = dff.set_index(['شماره بيمه نامه','comp','qest'])
        revival = pd.DataFrame(pishkarDb['RevivalLife'].find({'username':username},{'_id':0,'username':0}))

        if len(revival)>0:
            revival['شماره بيمه نامه'] = revival['شماره بيمه نامه'].astype(str)
            revival['qest'] = revival['qest'].astype(str)

            revival = revival.set_index(['شماره بيمه نامه','comp','qest'])
            dff = dff.join(revival)

            df = dff.reset_index()
            dff['RevivalResponse'] = dff['RevivalResponse'].fillna('در انتظار')
            dff = dff[dff['RevivalResponse']==data['RevivalFilter']]
        else:
            dff['RevivalResponse'] = 'در انتظار'
        dff['yearStart'] = [int(x.split('/')[0]) for x in dff['تاريخ شروع']]
        dff = dff.reset_index()
        dff['yearQest'] = [int(str(x).split('-')[0]) for x in dff['qest']]
        dff['ضریب رشد سالانه حق بیمه'] = [int(x) for x in dff['ضریب رشد سالانه حق بیمه']]
        dff['rete'] = (1+(dff['ضریب رشد سالانه حق بیمه']/100)) ** (dff['yearQest'] - dff['yearStart'])
        dff["حق بیمه هر قسط \n(جمع عمر و پوششها)"] = dff["حق بیمه هر قسط \n(جمع عمر و پوششها)"] * dff['rete']
        dff["حق بیمه هر قسط \n(جمع عمر و پوششها)"] = [int(x) for x in dff["حق بیمه هر قسط \n(جمع عمر و پوششها)"]]
        if 'index' in dff.columns:dff = dff.drop(columns='index')
        dff['count'] = int(1)
        dfo = dff[['شماره بيمه نامه','qest','comp']]
        dfo['شماره بيمه نامه'] = [str(x) for x in dfo['شماره بيمه نامه']]
        df = dff.groupby(by=['شماره بيمه نامه','comp','نام بیمه گذار','consultant']).sum(numeric_only=True).drop(columns='ضریب رشد سالانه حق بیمه').reset_index()
        df['code'] = [splitCode(x) for x in df['نام بیمه گذار']]
        if data['Comp'] !='همه':
            customer = pd.DataFrame(pishkarDb['customers'].find({'username':username,'comp':data['Comp']},{'_id':0,'بيمه گذار':1,'code':1,'تلفن همراه':1}))
        else:
            customer = pd.DataFrame(pishkarDb['customers'].find({'username':username},{'_id':0,'بيمه گذار':1,'code':1,'تلفن همراه':1}))
        customer = customer.drop_duplicates(subset=['code','بيمه گذار'])
        customer.columns = ['نام بیمه گذار', 'تلفن همراه', 'code']
        customer = customer.set_index('نام بیمه گذار').drop(columns='code')
        df = df.set_index('نام بیمه گذار').join(customer,how='left').reset_index()
        df['تلفن همراه'] = df['تلفن همراه'].fillna(0)
        df['تلفن همراه'] = ['0'+str(int(str(x).split('.')[0].split('-')[0].split('/')[0])) for x in df['تلفن همراه']]
        df['تلفن همراه'] = df['تلفن همراه'].replace('00','')
        minQest = dff.sort_values(by=['qest'])[['qest','شماره بيمه نامه']].copy()
        minQest = minQest.drop_duplicates(subset=['شماره بيمه نامه'],keep='first').set_index('شماره بيمه نامه')
        if 'qest' in df.columns: df = df.drop(columns=['qest'])
        df = df.set_index('شماره بيمه نامه').join(minQest)
        dff['num'] = dff['شماره بيمه نامه']
        dff = dff.set_index('num')
        dff['_children'] = dff.to_dict('records')
        dff = dff[['_children']].reset_index()
        dff = dff.groupby('num').agg({'_children':list})
        dff = dff.reset_index()
        dff = dff.rename(columns={'num':'شماره بيمه نامه'})
        df = df.join(dff.set_index('شماره بيمه نامه')).reset_index()
        df = df.to_dict('records')
        return json.dumps({'replay':True,"df":df})
    else:
        return ErrorCookie()
    
def liferevivalyear(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
    
        
        if data['Date'] == '': return json.dumps({"replay":False,'msg':'لطفا دوره کار را مشخص کنید'})
        methodPay = {'12':'اقساط ماهانه','6':'اقساط دو ماهه','4':'اقساط سه ماهه','3':'اقساط چهار ماهه','2':'اقساط شش ماهه','1':'سالانه'}
        if data['Comp'] !='همه':
            df = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username,'status':'جاری','comp':data['Comp']},{'_id':0,'مدت':1,'comp':1,'تاريخ شروع':1,'تعداد اقساط در سال':1,'روش پرداخت':1,'طرح':1,'شماره بيمه نامه':1,'نام بیمه گذار':1,'ضریب رشد سالانه حق بیمه':1,"حق بیمه هر قسط \n(جمع عمر و پوششها)":1,'تاريخ  انقضاء':1}))
        else:
            df = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username,'status':'جاری'},{'_id':0,'مدت':1,'comp':1,'تاريخ شروع':1,'تعداد اقساط در سال':1,'روش پرداخت':1,'طرح':1,'شماره بيمه نامه':1,'نام بیمه گذار':1,'ضریب رشد سالانه حق بیمه':1,"حق بیمه هر قسط \n(جمع عمر و پوششها)":1,'تاريخ  انقضاء':1}))

        if len(df)==0:
            return json.dumps({"replay":False,'msg':'بیمه نامه ای یافت نشد'})
        
        df['period'] = [timedate.dateToPriodMonth(x) for x in df['تاريخ شروع']]
        date = data['Date']['Show'].split('-')[1].replace(' ','')
        df = df[df['period']==date]

        if len(df)==0:
            return json.dumps({"replay":False,'msg':'بیمه نامه ای یافت نشد'})
        
        df = df.drop_duplicates(subset=['شماره بيمه نامه'])
        consoltant = pd.DataFrame(pishkarDb['AssingIssuingLife'].find({'username':username},{'_id':0,'username':0})).set_index('شماره بيمه نامه')
        adden = pd.DataFrame(pishkarDb['AddenLife'].find({'username':username},{'_id':0,'username':0}))
        adden = adden.drop_duplicates(subset=['شماره بيمه نامه','تاريخ شروع'])
        if len(adden)>0:
            numlist = adden['شماره بيمه نامه'].to_list()
            df['adden'] = [x in numlist for x in df['شماره بيمه نامه']]
            dfadden = df[df['adden']==True]
            df = df[df['adden']==False]

            if len(dfadden)>0:
                for i in dfadden.index:
                    basic = dfadden.loc[i].to_dict()
                    basicAdden = adden[adden['شماره بيمه نامه']==basic['شماره بيمه نامه']]
                    basicAdden['startDateInt'] = [timedate.dateToInt(x) for x in basicAdden['تاريخ شروع']]
                    firstAdden = basicAdden[basicAdden['startDateInt']==basicAdden['startDateInt'].min()].to_dict('records')[0]
                    basic['تاريخ  انقضاء'] = firstAdden['تاريخ شروع']
                    df = df.append(basic,ignore_index=True)
                    liststart = basicAdden['startDateInt'].to_list()
                    basicAdden = basicAdden.set_index(['startDateInt']).sort_index()
                    for j in basicAdden.index:
                        row = basicAdden.loc[j].to_dict()
                        endList = [ x for x in liststart if x>j]
                        if len(endList)==0: basic['تاريخ  انقضاء'] = row['تاريخ  انقضاء']
                        else: basic['تاريخ  انقضاء'] = timedate.intToDate(min(endList))
                        basic['تاريخ شروع'] = row['تاريخ شروع']
                        basic['ضریب رشد سالانه حق بیمه'] = int(row['ضریب رشد سالانه حق بیمه'])
                        basic['تعداد اقساط در سال'] = int(row['تعداد اقساط در سال'])
                        basic['روش پرداخت'] = methodPay[str(row['تعداد اقساط در سال'])]
                        basic['حق بیمه هر قسط \n(جمع عمر و پوششها)'] = int(str(row['حق بیمه هر قسط \n(جمع عمر و پوششها)']).replace(',','')) * (100/(100+int(row['ضریب رشد سالانه حق بیمه'])))
                        df = df.append(basic,ignore_index=True)
            df = df.drop(columns='adden')

        df = df.set_index('شماره بيمه نامه').join(consoltant,how='left').reset_index()
        if data['consultant'] != 'all':
            df = df[df['consultant']==data['consultant']]
        if len(df)==0:
            return json.dumps({"replay":False,'msg':'بیمه نامه ای یافت نشد'})
        cl_consultant = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username}))
        df['consultant'] = NCtNameReplace(cl_consultant,df['consultant'])
        if data['Comp'] !='همه':
            customer = pd.DataFrame(pishkarDb['customers'].find({'username':username,'comp':data['Comp']},{'_id':0,'بيمه گذار':1,'code':1,'تلفن همراه':1}))
        else:
            customer = pd.DataFrame(pishkarDb['customers'].find({'username':username},{'_id':0,'بيمه گذار':1,'code':1,'تلفن همراه':1}))
        customer = customer.drop_duplicates(subset=['code','بيمه گذار'])
        customer.columns = ['نام بیمه گذار', 'تلفن همراه', 'code']
        customer = customer.set_index('نام بیمه گذار').drop(columns='code')
        df = df.set_index('نام بیمه گذار').join(customer,how='left').reset_index()
        df['تلفن همراه'] = df['تلفن همراه'].fillna(0)
        df['تلفن همراه'] = ['0'+str(int(str(x).split('.')[0].split('-')[0].split('/')[0])) for x in df['تلفن همراه']]
        df['تلفن همراه'] = df['تلفن همراه'].replace('00','')


        df = df.to_dict('records')
        return json.dumps({'replay':True,"df":df})
    else:
        return ErrorCookie()


def customerincompnocode(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        dfIssuing = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0,'پرداخت کننده حق بیمه':1,'comp':1,'شماره بيمه نامه':1}))
        dfIssuinglife = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username},{'_id':0,'نام بیمه گذار':1,'comp':1,'شماره بيمه نامه':1}))
        dfIssuinglife = dfIssuinglife.rename(columns={'نام بیمه گذار':'customer','شماره بيمه نامه':'num'})
        dfIssuing = dfIssuing.rename(columns={'پرداخت کننده حق بیمه':'customer','شماره بيمه نامه':'num'})
        df = pd.concat([dfIssuinglife,dfIssuing])
        df['check_number'] = [check_number(x) for x in df['customer']]
        df = df[df['check_number']==False]
        df = df.to_dict('records')
        return json.dumps({'replay':True,"df":df})
    else:
        return ErrorCookie()  
    

def customerincomp(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        insurec = pd.DataFrame(pishkarDb['insurer'].find({'username':username},{'نام':1,'بیمه گر':1,'_id':0}))
        insurec = insurec.set_index('نام').to_dict(orient='dict')['بیمه گر']
        dfIssuing = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0,'پرداخت کننده حق بیمه':1,'comp':1}))
        dfIssuinglife = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username},{'_id':0,'نام بیمه گذار':1,'comp':1}))
        dfIssuinglife.columns = ['code','comp']
        dfIssuing.columns = ['code','comp']
        df = pd.concat([dfIssuing,dfIssuinglife])
        df['customer'] = df['code'].copy()
        df['code'] = [splitCode(x).replace(' ','') for x in df['code']]


        df = df.set_index(['code','comp'])
        df = df.drop_duplicates()


        dfCostomer = pd.DataFrame(pishkarDb['customers'].find({'username':username},{'_id':0,'code':1,'comp':1,'بيمه گذار':1}))
        dfCostomer.columns = ['customerc','comp','code']
        dfCostomer['Available'] = True
        dfCostomer = dfCostomer.drop_duplicates()
        dfCostomer = dfCostomer.set_index(['code','comp'])
        df = df.join(dfCostomer,how='left')

        df = df[df['Available']!=True]
        df = df.reset_index()[['customer','comp']]
        df['cuont'] = 1
        res = []
        dff = df.groupby(['comp']).sum(numeric_only=True).reset_index()
        for i in dff.index:
            if dff['cuont'][i] >10:
                dic = {'comp':dff['comp'][i],'cuont':int(dff['cuont'][i])}
                dft = df[df['comp']==dff['comp'][i]].reset_index()
                dft = list(dft[dft.index<3]['customer'])
                dic['name'] = dft
                res.append(dic)
            else:
                dft = df[df['comp']==dff['comp'][i]].reset_index()
                for j in dft.index:
                    dic = {'comp':dft['comp'][j],'cuont':int(dft['cuont'][j]),'name':dft['customer'][j]}
                    res.append(dic)
        return json.dumps({'replay':True,"df":res})
    else:
        return ErrorCookie()


def Alarm(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = json.loads(customerCheck(data))
        df = df['df']['تلفن همراه']['len'] + df['df']['کد ملي بيمه گذار']['len']
        if df>0:return json.dumps({"replay":True,"Alarm":True})
        df = json.loads(standardFeeCheck(data))['df']
        if len(df)>0:return json.dumps({"replay":True,"Alarm":True})
        df = json.loads(customerincomp(data))['df']
        if len(df)>0:return json.dumps({"replay":True,"Alarm":True})
        df = json.loads(customerincompnocode(data))['df']
        if len(df)>0:return json.dumps({"replay":True,"Alarm":True})
        return json.dumps({"replay":True,"Alarm":False})

    else:
        return ErrorCookie()
    