import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import timedate
import numpy as np
from reports import comparisomForFrocast
import random
client = pymongo.MongoClient()
pishkarDb = client['pishkar']

def NCtName(cl,nc):
    if str(nc)!='nan':
        df = cl[cl['nationalCode']==nc]
        if len(df)>0:
            df['full'] = df['fristName'] +' '+ df['lastName']
            df = df['full'][df.index.max()]
            return df
        else:
            return 'بدون مشاور'
    else:
        return 'بدون مشاور'
    
def getfees(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        feild = pd.DataFrame(pishkarDb['Fees'].find({'username':username}))
        feild = feild[['رشته','مورد بیمه']]
        feild = feild.drop_duplicates().reset_index().drop(columns=['index']).fillna('')
        feild = feild[feild['رشته']!='']
        feild['feild'] = feild['رشته']+' ('+feild['مورد بیمه']+')'
        feild = list(feild['feild'])
        feild = [str(x).replace(' ()','') for x in feild]
        consultant = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username,'nationalCode':data['nationalCode']},{'_id':0,'username':0}))
        
        for f in feild:
            if f not in consultant.columns:
                consultant[f] = 0
        for i in ['active','fristName','lastName','ConsultantSelected','nationalCode','gender','phone','code','childern','freetaxe','salaryGroup','insureWorker','insureEmployer']:
            try: consultant = consultant.drop(columns=[i])
            except: pass
        consultant = consultant.to_dict(orient='records')
        return json.dumps({'replay':True, 'df':consultant})
    else:
        return ErrorCookie()
            

def setfees(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        dic = data['fees']

        del dic['salary']
        pishkarDb['cunsoltant'].update_one({'username':username,'nationalCode':data['nc']},{'$set':data['fees']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def getatc(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username,'salary':True},{'_id':0,'fristName':1,'lastName':1,'nationalCode':1,'gender':1,'active':1}))
        df['active'] = df['active'].fillna(True).replace('false',False).replace('true',True)
        df = df[df['active']==True]
        if len(df)==0:
            return json.dumps({'replay':False, 'msg':'هیچ مشاوری تعریف نشده'})
        actDf = pd.DataFrame(pishkarDb['act'].find({'username':username,'period':data['today']['Show']},{'_id':0}))
        if len(actDf)==0:
            df['act'] = 0
            df['vaction'] = 0
            df['hours'] = 0
            df['period'] = data['today']['Show']
        else:
            df = df.set_index('nationalCode').join(actDf.set_index('nationalCode')).reset_index()
            df['period'] = data['today']['Show']
            if 'vaction' not in df.columns: df['vaction'] = 0
            if 'hours' not in df.columns: df['hours'] = 0
        df = df[['nationalCode','fristName','lastName','gender','act','period','vaction','hours']].fillna(0)
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})

    else:
        return ErrorCookie()


def setatc(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        act = (data['listatc'])
        for i in act:
            pishkarDb['act'].delete_many({'username':username,'nationalCode':i['nationalCode'],'period':i['period']})
            pishkarDb['act'].insert_one({'username':username,'nationalCode':i['nationalCode'],'period':i['period'],'act':i['act'],'hours':i['hours'],'vaction':i['vaction']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()



def actcopylastmonth(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        copy = pd.DataFrame(pishkarDb['act'].find({'username':username},{'_id':0}))
        copy['period'] = [timedate.PriodStrToInt(x) for x in copy['period']]
        copy = copy[copy['period']==copy['period'].max()]
        copy['period'] = data['date']['Show']
        copy = copy.to_dict(orient='records')
        pishkarDb['act'].delete_many({'period':data['date']['Show']})
        pishkarDb['act'].insert_many(copy)
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def forcastfee(data):
    data['OnBase'] = ''
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        assing = pd.DataFrame(pishkarDb['AssingIssuing'].find({'username':username,'cunsoltant':data['consultant']},{'_id':0,'username':0}))
        if len(assing)==0:
            return json.dumps({'replay':False,'msg':'هیچ صدوری برای این مشاور تخصیص داده نشده'})
        assing = assing.set_index(['کد رایانه صدور بیمه نامه','comp'])
        inssuing = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0,'username':0})).set_index(['کد رایانه صدور بیمه نامه','comp'])
        inssuing = inssuing.join(assing, how='right').reset_index()

        inssuing = inssuing[['کد رایانه صدور بیمه نامه','comp','رشته','مبلغ تسویه شده','مورد بیمه','شماره بيمه نامه','تاريخ بيمه نامه يا الحاقيه','تاریخ سررسید','مبلغ کل حق بیمه','بدهی باقی مانده','additional','پرداخت کننده حق بیمه']]
        inssuing['additional'] = inssuing['additional'].replace('برگشتی',-1).replace('اصلی',1).replace('اضافی',1)
        inssuing['بدهی باقی مانده'] = inssuing['بدهی باقی مانده'].fillna(0) * inssuing['additional']
        inssuing['مبلغ تسویه شده'] = inssuing['مبلغ تسویه شده'].fillna(inssuing['مبلغ کل حق بیمه']) * inssuing['additional']
        inssuing['مبلغ کل حق بیمه'] = inssuing['مبلغ کل حق بیمه'] * inssuing['additional']
        
        childern = inssuing.copy()
        childern['رشته'] = childern['رشته'].fillna('')
        childern['مورد بیمه'] = childern['مورد بیمه'].fillna('')
        childern['Title'] =  childern['رشته'] + ' ('+childern['مورد بیمه']+')'
        childern['Title'] = [str(x).replace(' ()','') for x in childern['Title']]
        childern['fees'] = 0
        childern['feesStandard'] = 0

        inssuing['تاریخ سررسید'] = [timedate.DateToPeriodInt(x) for x in inssuing['تاریخ سررسید']]

        inssuing = inssuing[inssuing['تاریخ سررسید']>0]
        inssuing = inssuing.groupby(by=['comp','رشته','تاریخ سررسید','مورد بیمه']).sum(numeric_only=True).reset_index().drop(columns=['additional'])
        comparisomValue = json.loads(comparisomForFrocast(data))
        comparisomValue = pd.DataFrame(comparisomValue['df']).drop(columns=['OutLine','groupMain','groupSub','index'])
        comparisomValue = comparisomValue.set_index(['comp','Title'])
        comparisomValue = comparisomValue.to_dict(orient='dict')['RealFeeRate']
        comparisomStandard = pd.DataFrame(pishkarDb['standardfee'].find({'username':username},{'_id':0,'field':1,'rate':1}))
        comparisomStandard = comparisomStandard.set_index('field').to_dict(orient='dict')['rate']

        fee = pishkarDb['cunsoltant'].find_one({'username':username,'nationalCode':data['consultant']})

        inssuing['رشته'] = inssuing['رشته'].fillna('')
        inssuing['مورد بیمه'] = inssuing['مورد بیمه'].fillna('')
        inssuing['Title'] =  inssuing['رشته'] + ' ('+inssuing['مورد بیمه']+')'
        inssuing['Title'] = [str(x).replace(' ()','') for x in inssuing['Title']]
        inssuing['fees'] = 0
        inssuing['feesStandard'] = 0


        for i in inssuing.index:
            field = (inssuing['comp'][i],inssuing['Title'][i])
            try:inssuing['fees'][i] = (comparisomValue[field]/100) * (int(fee[inssuing['Title'][i]])/100)
            except:pass
            try:inssuing['feesStandard'][i] = (int(comparisomStandard[inssuing['Title'][i]])/100) * (int(fee[inssuing['Title'][i]])/100)
            except:pass

                
        inssuing['Get'] = (inssuing['fees'] * inssuing['مبلغ تسویه شده']) + (inssuing['fees'] * inssuing['بدهی باقی مانده'])
        inssuing['GetStandard'] = (inssuing['feesStandard'] * inssuing['مبلغ تسویه شده']) + (inssuing['feesStandard'] * inssuing['بدهی باقی مانده'])
        #inssuing = inssuing[inssuing['Get']>0]
        inssuing['Count'] = 1 


        for i in childern.index:
            field = (childern['comp'][i],childern['Title'][i])
            try:childern['fees'][i] = (comparisomValue[field]/100) * (int(fee[childern['Title'][i]])/100)
            except:pass
            try:childern['feesStandard'][i] = int(comparisomStandard[childern['Title'][i]]/100) * (int(fee[childern['Title'][i]])/100)
            except:pass
        childern = childern.dropna()
        childern['Get'] = (childern['fees'] * childern['مبلغ تسویه شده']) + (childern['fees'] * childern['بدهی باقی مانده'])
        childern['Get'] = [int(x) for x in childern['Get']]
        childern['GetStandard'] = (childern['feesStandard'] * childern['مبلغ تسویه شده']) + (childern['feesStandard'] * childern['بدهی باقی مانده'])
        childern['GetStandard'] = [int(x) for x in childern['GetStandard']]

        #childern = childern[childern['Get']>0]
        childern['Count'] = 1
        childern = childern[['comp','تاریخ سررسید','Get','GetStandard','Title','Count','پرداخت کننده حق بیمه']]
        childern['period'] = [timedate.DateToPeriodInt(x) for x in childern['تاریخ سررسید']]
        childern['period'] = [timedate.PeriodIntToPeriodStr(x) for x in childern['period']]
        inssuing = inssuing.groupby(by=['تاریخ سررسید']).sum(numeric_only=True)[['Get','Count','GetStandard']]
        inssuing['پرداخت کننده حق بیمه'] = ''
        inssuing['comp'] = ''
        inssuing['Title'] = ''
        inssuing['_children'] = ''
        inssuing = inssuing.reset_index()
        inssuing['Get'] = [int(x) for x in inssuing['Get']]
        inssuing['GetStandard'] = [int(x) for x in inssuing['GetStandard']]
        inssuing['تاریخ سررسید'] = [timedate.PeriodIntToPeriodStr(x) for x in inssuing['تاریخ سررسید']]
        inssuing = inssuing.to_dict('records')
        df = []
        for i in inssuing:
            dic = i
            childDic = childern[childern['period']==i['تاریخ سررسید']]
            dic['_children'] = childDic.to_dict('records')
            dic['Count'] = len(childDic)
            df.append(dic)
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()
    
def forcastfeelife(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username},{'_id':0,'username':0}))
        consultant = pd.DataFrame(pishkarDb['AssingIssuingLife'].find({'username':username},{'_id':0,'username':0}))
        df = df.drop_duplicates(subset=['comp','شماره بيمه نامه'])
        df = df.set_index('شماره بيمه نامه').join(consultant.set_index('شماره بيمه نامه')).reset_index()
        df = df[df['consultant']==data['consultant']]
        if len(df) == 0:
            return json.dumps({'replay':False,'msg':'بیمه نامه ای یافت نشد'})
        try:datePeriod = timedate.timStumpTojalali(data['datePeriod']['date'])
        except:return json.dumps({'replay':False,'msg':'لطفا دور کار را به صورت صحیح انتخاب کنید'})
        df['expier'] = [abs(timedate.Diff2DatePersian(datePeriod,x))<1825 for x in df['تاريخ شروع']]
        df = df[df['expier']==True]
        df['مدت'] = [int(x) for x in df['مدت']]
        df['تعداد اقساط در سال'] = [int(x) for x in df['تعداد اقساط در سال']]
        if len(df) == 0:
            return json.dumps({'replay':False,'msg':'بیمه نامه ای یافت نشد'})
        dff = pd.DataFrame()
        for i in df.index:
            qestlist = timedate.qestList(df['مدت'][i],df['تاريخ شروع'][i],df['تعداد اقساط در سال'][i])
            for q in qestlist:
                if timedate.Diff2DatePersian(q,df['تاريخ شروع'][i])<1825 and timedate.Diff2DatePersian(datePeriod,q)<=0:
                    dic = {"شماره بيمه نامه":df['شماره بيمه نامه'][i],
                        'تاريخ شروع':df['تاريخ شروع'][i],
                        'تاريخ اقساط':q,
                        'حق بیمه سال نخست':df['حق بیمه هر قسط \n(جمع عمر و پوششها)'][i],
                        'تعداد اقساط':df['تعداد اقساط در سال'][i],
                        'comp':df['comp'][i],
                        'نام بیمه گذار' :df['نام بیمه گذار'][i],
                        }
                    dff = dff.append(dic,ignore_index=True)
        dff['fee'] = ((dff['حق بیمه سال نخست'] / dff['تعداد اقساط'])*0.75)/5
        rateFeeConsultant = pishkarDb['cunsoltant'].find_one({'username':username,'nationalCode':data['consultant']})['عمر و سرمایه گذاری']
        dff['fee'] = dff['fee'] * (int(rateFeeConsultant)/100)
        dff['fee'] = [int(x) for x in dff['fee']]
        dff['period'] = [timedate.dateToPriod(x) for x in dff['تاريخ اقساط']]
        dff['last'] = [timedate.dateToInt(x)>timedate.dateToInt(datePeriod) for x in dff['تاريخ اقساط']]
        dff = dff[dff['last']==True]
        df_ = dff.groupby('period').sum(numeric_only=True)['fee'].reset_index()
        df_ = df_.to_dict('records')
        df = []
        for i in df_:
            dic = i
            childDic = dff[dff['period']==i['period']].drop(columns=['حق بیمه سال نخست','period','last','تعداد اقساط'])
            dic['_children'] = childDic.to_dict('records')
            dic['Count'] = len(childDic)
            df.append(dic)
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()


def addintegration(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        lst = [x['code'] for x in data['ConsultantSelected']]
        if len(lst)!=len(set(lst)):
            return json.dumps({'replay':False, 'msg':'مشاوران میبایست غیر تکراری باشند'})
        inDb = pishkarDb['cunsoltant'].find_one({'username':username,'lastName':data['name']})
        if inDb!=None:
            return json.dumps({'replay':False, 'msg':'نام گروه تلفیق تکراری است'})
        dic = {'fristName':'تلفیق','lastName':data['name'],'nationalCode':str(random.randint(1000000000,9999999999)),'gender':'گروه','salary':False,'childern':0,'freetaxe':0,'insureWorker':0,'insureEmployer':0,'username':username}
        dic['ConsultantSelected'] = data['ConsultantSelected']
        pishkarDb['cunsoltant'].insert_one(dic)
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def getintegration(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username,'fristName':'تلفیق','gender':'گروه'},{'_id':0,'fristName':1,'lastName':1,'nationalCode':1,'gender':1,'ConsultantSelected':1}))
        df['count'] = 0
        df['ditaile'] = ''
        cl_consultant = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username}))
        for i in df.index:
            listconsultant = df['ConsultantSelected'][i]
            newlist = ''
            df['count'][i] = len(listconsultant)
            for j in listconsultant:
                consultant = NCtName(cl_consultant,j['code'])
                fee = j['fee']
                newlist = newlist + '(' + str(consultant)+' %' + str(fee) + ') '
            df['ditaile'][i] = newlist
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True, 'df':df})
    else:
        return ErrorCookie()

def delintegration(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        nationalCode = data['code']['nationalCode']
        pishkarDb['cunsoltant'].delete_many({'username':username,'nationalCode':nationalCode})
        pishkarDb['AssingIssuing'].delete_many({'username':username,'AssingIssuing':nationalCode})
        pishkarDb['benefit'].delete_many({'username':username,'nationalCode':nationalCode})
        pishkarDb['assing'].delete_many({'username':username,'assing':nationalCode})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()