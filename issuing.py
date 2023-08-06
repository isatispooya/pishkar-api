import json
import timedate
import pymongo
from bson.objectid import ObjectId
import pandas as pd
from Sing import cookie, ErrorCookie
from assing import NCtName
import timedate
client = pymongo.MongoClient()
pishkarDb = client['pishkar']





def addfileNoneAdditional(cookier,file,comp):
    user = cookie(cookier)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.read_excel(file)
        msg = ''
        requiedCulomns = ['رشته','مورد بیمه','کد رایانه صدور بیمه نامه','پرداخت کننده حق بیمه',
            'شماره بيمه نامه','تاريخ بيمه نامه يا الحاقيه','تاریخ عملیات','تاریخ سررسید',
            'تاريخ سند دريافتي','وضعيت وصول','مبلغ کل حق بیمه','مبلغ تسویه شده','بدهی باقی مانده']
        for rc in requiedCulomns:
            inculomns = rc in df.columns
            if inculomns==False:
                return json.dumps({'replay':False,'msg':f'فایل فاقد ستون ضروری "{rc}" است'})
        df['comp'] = comp
        df['username'] = username
        beforDuplicatesLen = len(df)
        df = df.drop_duplicates(['کد رایانه صدور بیمه نامه','تاریخ سررسید'])
        AfterDuplicatesLen = len(df)
        if (beforDuplicatesLen!=AfterDuplicatesLen):
            msg = msg + f'{beforDuplicatesLen - AfterDuplicatesLen} رکورد تکراری بوده و حذف شده' +  '\n'
        dff = pd.DataFrame(pishkarDb['issuing'].find({'username':username,'comp':comp},{'_id':0,'کد رایانه صدور بیمه نامه':1,'تاریخ سررسید':1}))
        if len(dff)>0:
            dff['act'] = 1
            df =df.set_index(['کد رایانه صدور بیمه نامه','تاریخ سررسید']).join(dff.set_index(['کد رایانه صدور بیمه نامه','تاریخ سررسید']))
            df = df.reset_index()
            duplicateLen = int(df['act'].sum())
            if duplicateLen>0:
                msg = msg + f'{duplicateLen} رکورد قبلا ثبت شده بود که بروز رسانی شد'
                dropList = df[df['act']==1][['کد رایانه صدور بیمه نامه','تاریخ سررسید']]
                for d in dropList.index:
                    pishkarDb['issuing'].delete_one({'username':username,'comp':comp,'کد رایانه صدور بیمه نامه':int(dropList['کد رایانه صدور بیمه نامه'][d]),'تاریخ سررسید':dropList['تاریخ سررسید'][d]})
            df = df.drop(columns=['act'])
        df['additional'] = 'اصلی'
        df['تاريخ پایان'] = [timedate.PersianToGregorian(x) for x in df['تاريخ بيمه نامه يا الحاقيه']]
        df['تاريخ پایان'] = [timedate.GregorianPlus(x,364) for x in df['تاريخ پایان']]
        df['تاريخ پایان'] = [timedate.GregorianToPersian(x) for x in df['تاريخ پایان']]
        df['تاريخ پایان'] = [str(x).replace('-','/') for x in df['تاريخ پایان']]
        df = df.to_dict(orient='records')
        pishkarDb['issuing'].insert_many(df)
        msg = 'ثبت شد \n ' + msg
        return json.dumps({'replay':True,'additional':False,'msg':msg})
    else:
        return ErrorCookie()

def addfilewitheAdditional(cookier,file,comp,additional):
    user = cookie(cookier)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.read_excel(file)
        msg = ''
        requiedCulomns = ['رشته','مورد بیمه','کد رایانه صدور بیمه نامه','پرداخت کننده حق بیمه',
            'شماره بيمه نامه','تاريخ بيمه نامه يا الحاقيه','تاریخ عملیات','تاریخ سررسید',
            'تاريخ سند دريافتي','وضعيت وصول','مبلغ کل حق بیمه','مبلغ تسویه شده','بدهی باقی مانده']
        for rc in requiedCulomns:
            inculomns = rc in df.columns
            if inculomns==False:
                return json.dumps({'replay':False,'msg':f'فایل فاقد ستون ضروری "{rc}" است'})
        df['comp'] = comp
        df['username'] = username
        beforDuplicatesLen = len(df)
        df = df.drop_duplicates(['کد رایانه صدور بیمه نامه','تاریخ سررسید','شماره الحاقیه'])
        AfterDuplicatesLen = len(df)
        if (beforDuplicatesLen!=AfterDuplicatesLen):
            msg = msg + f'{beforDuplicatesLen - AfterDuplicatesLen} رکورد تکراری بوده و حذف شده' +  '\n'
        dff = pd.DataFrame(pishkarDb['issuing'].find({'username':username,'comp':comp},{'_id':0,'کد رایانه صدور بیمه نامه':1,'تاریخ سررسید':1}))
        if len(dff)>0:
            dff['act'] = 1
            df =df.set_index(['کد رایانه صدور بیمه نامه','تاریخ سررسید']).join(dff.set_index(['کد رایانه صدور بیمه نامه','تاریخ سررسید']))
            df = df.reset_index()
            duplicateLen = int(df['act'].sum())
            if duplicateLen>0:
                msg = msg + f'{duplicateLen} رکورد قبلا ثبت شده بود که بروز رسانی شد'
                dropList = df[df['act']==1][['کد رایانه صدور بیمه نامه','تاریخ سررسید']]
                for d in dropList.index:
                    pishkarDb['issuing'].delete_one({'username':username,'comp':comp,'کد رایانه صدور بیمه نامه':int(dropList['کد رایانه صدور بیمه نامه'][d]),'تاریخ سررسید':dropList['تاریخ سررسید'][d]})
            df = df.drop(columns=['act'])
        df['additional'] = 'اصلی'
        additionals = str(additional).split('},{')
        additionals = [json.loads('{'+(x.replace('{','').replace('}',''))+'}') for x in additionals]
        for i in df.index:
            for j in additionals:
                if df['comp'][i] == j['comp'] and df['کد رایانه صدور بیمه نامه'][i] == j['کد رایانه صدور بیمه نامه'] and df['شماره الحاقیه'][i] == j['شماره الحاقیه']:
                    df['additional'][i] = j['additional']
        df['تاريخ پایان'] = [timedate.PersianToGregorian(x) for x in df['تاريخ بيمه نامه يا الحاقيه']]
        df['تاريخ پایان'] = [timedate.GregorianPlus(x,364) for x in df['تاريخ پایان']]
        df['تاريخ پایان'] = [timedate.GregorianToPersian(x) for x in df['تاريخ پایان']]
        df['تاريخ پایان'] = [str(x).replace('-','/') for x in df['تاريخ پایان']]
        df = df.to_dict(orient='records')
        pishkarDb['issuing'].insert_many(df)
        msg = 'ثبت شد \n ' + msg
        return json.dumps({'replay':True,'additional':False,'msg':msg})
    else:
        return ErrorCookie()

def CheackAdditional(cookier,file,comp,additional):
    user = cookie(cookier)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        try:df = pd.read_excel(file)
        except: return json.dumps({'replay':False,'msg':'خطا، فایل دارای ایراد است'})
        msg = ''
        requiedCulomns = ['رشته','مورد بیمه','کد رایانه صدور بیمه نامه','پرداخت کننده حق بیمه',
            'شماره بيمه نامه','شماره الحاقیه','تاريخ بيمه نامه يا الحاقيه','تاریخ عملیات','تاریخ سررسید',
            'تاريخ سند دريافتي','وضعيت وصول','مبلغ کل حق بیمه','مبلغ تسویه شده','بدهی باقی مانده']
        for rc in requiedCulomns:
            inculomns = rc in df.columns
            if inculomns==False:
                return json.dumps({'replay':False,'msg':f'فایل فاقد ستون ضروری "{rc}" است'})
        df['comp'] = comp
        df['username'] = username
        df = df[df['شماره الحاقیه']!=0]
        if len(df)==0:
            return addfileNoneAdditional(cookier,file,comp)
        if additional =='null':
            df = df.drop_duplicates(['کد رایانه صدور بیمه نامه','تاریخ سررسید','شماره الحاقیه'])
            df = df[['کد رایانه صدور بیمه نامه','شماره الحاقیه','comp']]
            df['additional'] = 'اضافی'
            df = df.fillna('')
            df = df.to_dict(orient='records')
            return json.dumps({'replay':True,'additional':df})
        else:
            return addfilewitheAdditional(cookier,file,comp,additional)
    else:
        return ErrorCookie()

def getdf(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0}))
        if len(df)==0:
            return json.dumps({'replay':False})
        df = df[['تاریخ عملیات','مبلغ کل حق بیمه','comp','کد رایانه صدور بیمه نامه']]
        df = df.fillna('')
        df['دوره عملیات'] = [timedate.dateToPriod(x) for x in df['تاریخ عملیات']]
        df['تعداد'] = 1
        df['تاریخ عملیات عددی'] = [timedate.dateToInt(x) for x in df['تاریخ عملیات']]
        dfcount = df.drop_duplicates(subset=['کد رایانه صدور بیمه نامه','comp'])
        dfcount['count'] = 1
        dfcount = dfcount.groupby(by=['دوره عملیات','comp']).sum(numeric_only=True)[['count','مبلغ کل حق بیمه']]
        dff = df.groupby(by=['دوره عملیات','comp']).sum(numeric_only=True).drop(columns=['تاریخ عملیات عددی','مبلغ کل حق بیمه'])
        dff['از تاریخ'] = df.groupby(by=['دوره عملیات','comp'])[['تاریخ عملیات عددی']].min()['تاریخ عملیات عددی']
        dff['تا تاریخ'] = df.groupby(by=['دوره عملیات','comp'])[['تاریخ عملیات عددی']].max()['تاریخ عملیات عددی']
        dff = dff.join(dfcount)
        dff = dff.dropna()
        dff = dff.reset_index()
        dff['از تاریخ'] = [timedate.intToDate(x) for x in dff['از تاریخ']]
        dff['تا تاریخ'] = [timedate.intToDate(x) for x in dff['تا تاریخ']]
        dff = dff.to_dict(orient='records')
        return json.dumps({'replay':True,'df':dff})
    else:
        return ErrorCookie()

def getdfLife(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username},{'_id':0}))

        if len(df)==0:
            return json.dumps({'replay':False})
        df = df[['تاريخ شروع','حق بیمه هر قسط \n(جمع عمر و پوششها)','comp','شماره بيمه نامه','تعداد اقساط در سال']]
        df['تعداد اقساط در سال'] = [int(x) for x in df['تعداد اقساط در سال']]
        df['حق بیمه هر قسط \n(جمع عمر و پوششها)'] = [int(x) for x in df['حق بیمه هر قسط \n(جمع عمر و پوششها)']]
        df['مبلغ کل حق بیمه'] = df['حق بیمه هر قسط \n(جمع عمر و پوششها)'] * df['تعداد اقساط در سال']
        df = df.drop(columns=['حق بیمه هر قسط \n(جمع عمر و پوششها)','تعداد اقساط در سال'])
        df = df.fillna('')
        df['تاريخ شروع'] = [timedate.dateToInt(x) for x in df['تاريخ شروع']]
        dfcount = df.drop_duplicates(subset=['شماره بيمه نامه','comp'])
        dfcount['count'] = 1
        dfcount = dfcount.groupby(by=['comp']).sum(numeric_only=True)[['count','مبلغ کل حق بیمه']]
        dff = df.groupby(by=['comp']).sum(numeric_only=True).drop(columns=['تاريخ شروع','مبلغ کل حق بیمه'])
        dff['از تاریخ'] = df.groupby(by=['comp']).min()['تاريخ شروع']
        dff['تا تاریخ'] = df.groupby(by=['comp']).max()['تاريخ شروع']
        dff = dff.join(dfcount)
        dff = dff.dropna()
        dff = dff.reset_index()
        dff['از تاریخ'] = [timedate.intToDate(x) for x in dff['از تاریخ']]
        dff['تا تاریخ'] = [timedate.intToDate(x) for x in dff['تا تاریخ']]
        dff = dff.to_dict(orient='records')
        return json.dumps({'replay':True,'df':dff})
    else:
        return ErrorCookie()
    
def getcunsoltant(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        dfIssuing = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0}))
        dfAssing = pd.DataFrame(pishkarDb['AssingIssuing'].find({'username':username},{'_id':0,'username':0}))
        if len(dfIssuing)==0:
            return json.dumps({'replay':False, 'msg':'هیچ صدوری ثبت نشده است'})

        df = dfIssuing.drop_duplicates(subset=['کد رایانه صدور بیمه نامه','comp'])

        if len(dfAssing) == 0:
            df['cunsoltant'] = ''
        else:
            df = df.set_index(['comp','کد رایانه صدور بیمه نامه'])
            dfAssing = dfAssing.set_index(['comp','کد رایانه صدور بیمه نامه'])
            df = df.join(dfAssing,how='left').reset_index()

            df = df.fillna('')
            if data['showAll']==False:
                df = df[df['cunsoltant']=='']
                df['cunsoltantName'] = ''
            else:
                cl_consultant = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username}))
                df['cunsoltantName'] = [NCtName(cl_consultant,x) for x in df['cunsoltant']]

        df = df.fillna('')
        df = df.drop_duplicates(subset=['کد رایانه صدور بیمه نامه','comp'])
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()

def getcunsoltantLife(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        dfIssuing = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username},{'_id':0}))
        dfAssing = pd.DataFrame(pishkarDb['AssingIssuingLife'].find({'username':username},{'_id':0,'username':0}))

        if len(dfIssuing)==0:
            return json.dumps({'replay':False, 'msg':'هیچ صدوری ثبت نشده است'})
        df = dfIssuing.drop_duplicates(subset=['شماره بيمه نامه'],keep='last')
        if len(dfAssing) == 0:
            df['cunsoltant'] = ''
        else:
            df = df.set_index(['شماره بيمه نامه'])
            dfAssing = dfAssing.set_index(['شماره بيمه نامه'])[['consultant']]

            df = df.join(dfAssing,how='left').reset_index()
            df = df.fillna('')
            if data['showAll']==False:
                df = df[df['consultant']=='']
                df['cunsoltantName'] = ''
            else:
                cl_consultant = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username}))
                df['cunsoltantName'] = [NCtName(cl_consultant,x) for x in df['consultant']]
        df = df.fillna('')
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()
    
def assingcunsoltant(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['AssingIssuing'].delete_many({'username':username,'comp':data['InsurenceData']['comp'],'کد رایانه صدور بیمه نامه':data['InsurenceData']['کد رایانه صدور بیمه نامه']})
        pishkarDb['AssingIssuing'].insert_one({'username':username,'comp':data['InsurenceData']['comp'],'کد رایانه صدور بیمه نامه':data['InsurenceData']['کد رایانه صدور بیمه نامه'],'cunsoltant':data['consultant']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def assingcunsoltantLife(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['AssingIssuingLife'].delete_many({'username':username,'شماره بيمه نامه':data['InsurenceData']['شماره بيمه نامه']})
        pishkarDb['AssingIssuingLife'].insert_one({'username':username,'شماره بيمه نامه':data['InsurenceData']['شماره بيمه نامه'],'consultant':data['consultant']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def getissuingmanual(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['issuing'].find({'username':username}))
        if len(df)>0:
            df = df[['_id','رشته','کد رایانه صدور بیمه نامه','مورد بیمه','پرداخت کننده حق بیمه','شماره بيمه نامه','تاريخ بيمه نامه يا الحاقيه','تاریخ عملیات','تاریخ سررسید','مبلغ کل حق بیمه','مبلغ تسویه شده','بدهی باقی مانده','comp','additional','تاريخ پایان']]
        df = df.fillna('')
        df['مدت زمان'] = ''
        for i in df[['تاريخ بيمه نامه يا الحاقيه','تاريخ پایان']].index:
            try:
                df['مدت زمان'][i] = int(str(abs(timedate.Diff2DatePersian(df['تاريخ بيمه نامه يا الحاقيه'][i],df['تاريخ پایان'][i]))+1))
            except:
                df['مدت زمان'][i] = 0
        df['_id'] = [str(x) for x in df['_id']]
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()

def getIssuingLifeManual(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username}))
        if len(df)>0:
            df = df[['مدت','تاريخ شروع','تاريخ  انقضاء','روش پرداخت','طرح','شماره بيمه نامه','نام بیمه گذار','حق بیمه هر قسط \n(جمع عمر و پوششها)','تعداد اقساط در سال','comp','ضریب رشد سالانه حق بیمه']]
            df.columns = ['مدت','تاريخ شروع','تاريخ  پایان','روش پرداخت','طرح','شماره بيمه نامه','نام بیمه گذار','حق بیمه هر قسط \n(جمع عمر و پوششها)','تعداد اقساط در سال','comp','ضریب رشد سالانه حق بیمه']
        df['تعداد اقساط در سال'] = [int(x) for x in df['تعداد اقساط در سال']]
        df['حق بیمه هر قسط \n(جمع عمر و پوششها)'] = [int(x) for x in df['حق بیمه هر قسط \n(جمع عمر و پوششها)']]
        df['حق بیمه'] = df['حق بیمه هر قسط \n(جمع عمر و پوششها)'] * df['تعداد اقساط در سال']
        df = df.fillna('')
        df.to_excel('dff.xlsx')
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()
    
def addissuingmanual(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        IssuingDict = data['IssuingDict']
        try:
            IssuingDict['مبلغ کل حق بیمه'] = int(IssuingDict['مبلغ کل حق بیمه'])
            IssuingDict['مبلغ تسویه شده'] = int(IssuingDict['مبلغ تسویه شده'])
            IssuingDict['بدهی باقی مانده'] = int(IssuingDict['بدهی باقی مانده'])
        except:
            return json.dumps({'replay':False,'msg':'مبلغ کل حق بیمه، مبلغ تسویه شده، بدهی باقی مانده میبایست از نوع عددی باشد'})
        try:
            IssuingDict['تاریخ عملیات'] = timedate.dateToStandard(IssuingDict['تاریخ عملیات'])
            IssuingDict['تاریخ سررسید'] = timedate.dateToStandard(IssuingDict['تاریخ سررسید'])
            IssuingDict['تاريخ بيمه نامه يا الحاقيه'] = timedate.dateToStandard(IssuingDict['تاريخ بيمه نامه يا الحاقيه'])
        except:
            return json.dumps({'replay':False,'msg':'تاریخ عملیات، تاریخ سررسید، تاريخ بيمه نامه يا الحاقيه میبایست از نوع تاریخ باشد'})
        IssuingDict['username'] = username

        if len(IssuingDict['_id'])>1:
            dupl = pishkarDb['issuing'].find_one({'username':username,'_id':ObjectId(IssuingDict['_id'])})
            if dupl!=None:
                p = pishkarDb['issuing'].delete_one({'username':username,'_id':ObjectId(IssuingDict['_id'])})
        del IssuingDict['_id']
        pishkarDb['issuing'].insert_one(IssuingDict)
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()
    
def delfile(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['issuing'].find({'username':username,'comp':data['data']['comp']}))
        df['دوره عملیات'] = [timedate.dateToPriod(x) for x in df['تاریخ عملیات']]
        df = df[df['دوره عملیات']==data['data']['دوره عملیات']]
        if len(df)==0:
            return json.dumps({'replay':False,'msg':'موردی یافت نشد'})
        for i in df.index:
            pishkarDb['issuing'].delete_one({'_id':df['_id'][i]})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def delissuingmanual(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['issuing'].delete_many({"_id":ObjectId(data['dict']['_id'])})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def delissuingmanuallife(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['issuingLife'].delete_many({'username':username,'comp':data['dict']['comp'],'شماره بيمه نامه':data['dict']['شماره بيمه نامه']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()
    

def getadditional(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0,'شماره بيمه نامه':1,'رشته':1,'کد رایانه صدور بیمه نامه':1,'مورد بیمه':1,'پرداخت کننده حق بیمه':1,'additional':1,'تاريخ بيمه نامه يا الحاقيه':1,'مبلغ کل حق بیمه':1,'comp':1}))
        if len(df)==0:
            return json.dumps({'replay':False, 'msg':'هیچ بیمه نامه ای یافت نشد'})
        add = df[df['additional']!='اصلی']
        add['count'] = 1
        add = add.groupby(by=['کد رایانه صدور بیمه نامه','comp']).sum(numeric_only=True)
        add = add[['count']]
        df = df.drop_duplicates(subset=['comp','کد رایانه صدور بیمه نامه'])
        df = df[df['additional']=='اصلی']
        df = df.set_index(['کد رایانه صدور بیمه نامه','comp'])
        df = df.join(add,how='left')
        df['count'] = df['count'].fillna(0)
        df = df.reset_index()
        df = df.fillna('')
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()

def addaditional(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        additionalDict = data['additionalDict']
        df = pishkarDb['issuing'].find_one({'username':username,'کد رایانه صدور بیمه نامه': additionalDict['کد رایانه صدور بیمه نامه'], 'comp':additionalDict['comp']},{'_id':0})
        df['additional'] = additionalDict['additional']
        if(additionalDict['additional']=='برگشتی'):
            if(additionalDict['مبلغ کل حق بیمه']>df['مبلغ کل حق بیمه']):
                return json.dumps({'replay':True,'msg':'حق بیمه الحاقیه برگشتی نمیتواند بیشتر از حق بیمه اصلی باشد'})
        df['مبلغ کل حق بیمه'] = additionalDict['مبلغ کل حق بیمه']
        df['مبلغ تسویه شده'] = additionalDict['مبلغ تسویه شده']
        df['بدهی باقی مانده'] = additionalDict['بدهی باقی مانده']
        df['تاریخ سررسید'] = additionalDict['تاریخ سررسید']
        df['تاریخ عملیات'] = additionalDict['تاریخ عملیات']
        df['تاريخ بيمه نامه يا الحاقيه'] = additionalDict['تاريخ بيمه نامه يا الحاقيه']
        df['شماره الحاقیه'] = additionalDict['شماره الحاقیه']
        df['شماره بيمه نامه'] = additionalDict['شماره بيمه نامه']
        pishkarDb['issuing'].insert_one(df)
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()


def sales(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        issuing = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0}))
        issuingLife = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username},{'_id':0}))
        issuingLife = issuingLife[['نام بیمه گذار','تاريخ شروع','شماره بيمه نامه', 'تعداد اقساط در سال','حق بیمه هر قسط \n(جمع عمر و پوششها)','comp']]
        issuingLife['حق بیمه هر قسط \n(جمع عمر و پوششها)'] = [int(x) for x in issuingLife['حق بیمه هر قسط \n(جمع عمر و پوششها)']]
        issuingLife['تعداد اقساط در سال'] = [int(x) for x in issuingLife['تعداد اقساط در سال']]
        issuingLife['Date'] = [timedate.PersianToTimetump(x) for x in issuingLife['تاريخ شروع']]
        issuingLife = issuingLife[issuingLife['Date']>=int(data['Date']['from'])]
        issuingLife = issuingLife[issuingLife['Date']<=int(data['Date']['to'])]
        issuingLife['count'] = 1
        issuingLife['countAdd'] = 0
        issuingLife['countdiff'] = 0
        issuingLife['مبلغ کل حق بیمه'] = issuingLife['حق بیمه هر قسط \n(جمع عمر و پوششها)'] * issuingLife['تعداد اقساط در سال']
        issuingLife['Field'] = 'زندگی'

        issuingLife = issuingLife[['شماره بيمه نامه','نام بیمه گذار','Field','comp','مبلغ کل حق بیمه','count', 'countAdd', 'countdiff']]

        issuing['Date'] = [timedate.PersianToTimetump(x) for x in issuing['تاریخ عملیات']]
        dd = issuing[issuing['comp']=='ما']
        dd = dd[dd['شماره بيمه نامه']==30].to_dict('records')[-1]['Date']

        issuing = issuing[issuing['Date']>=int(data['Date']['from'])]
        issuing = issuing[issuing['Date']<=int(data['Date']['to'])]
        #issuing = issuing[issuing['Date']>=int(data['Date']['from'])-3600001]
        #issuing = issuing[issuing['Date']<=int(data['Date']['to']+3600001)]
        issuing = issuing.drop_duplicates(subset=['کد رایانه صدور بیمه نامه','شماره بيمه نامه','comp','additional','مبلغ کل حق بیمه','تاریخ عملیات'])
        issuing['count'] = 1
        issuing['countAdd'] = issuing['additional']=='اضافی'
        issuing['countdiff'] = issuing['additional']=='برگشتی'
        issuing['additional'] = issuing['additional'].replace('برگشتی','-1').replace('اصلی','1').replace('اضافی','1')
        issuing['additional'] = [int(x) for x in issuing['additional']]
        issuing['مبلغ کل حق بیمه'] = issuing['مبلغ کل حق بیمه'] * issuing['additional']
        stndrd = pd.DataFrame(pishkarDb['standardfee'].find({'username':username},{'_id':0, 'field':1, 'groupMain':1}))
        issuing = issuing.fillna('')
        issuing['Field'] =  issuing['رشته'] + ' ('+issuing['مورد بیمه']+')'
        issuing['Field'] = [str(x).replace(' ()','') for x in issuing['Field']]
        issuing = issuing[['شماره بيمه نامه','پرداخت کننده حق بیمه','Field','comp','مبلغ کل حق بیمه','count', 'countAdd', 'countdiff']]
        issuing.columns =['شماره بيمه نامه', 'نام بیمه گذار', 'Field', 'comp','مبلغ کل حق بیمه', 'count', 'countAdd', 'countdiff']

        issuing = pd.concat([issuing,issuingLife])
        issuing['tax'] = [('درمان' in x or 'زندگی' in x) for x in issuing['Field']]
        issuing['tax'] = issuing['tax'].replace(False,'1.09').replace(True,'1')
        issuing['tax'] = [float(x) for x in issuing['tax']]
        issuing['مبلغ کل حق بیمه'] =  issuing['مبلغ کل حق بیمه'] / issuing['tax']
        issuing['مبلغ کل حق بیمه'] = [int(x) for x in issuing['مبلغ کل حق بیمه']]
        children = issuing.copy()
        df = issuing.groupby(by=['Field','comp']).sum(numeric_only=True)
        df = df.reset_index()
        df = df.set_index(['Field']).join(stndrd.set_index(['field']),how='left').reset_index()
        df['groupMain'] = df['groupMain'].fillna('دیگر')
        df['groupMain'] = df['groupMain'].replace('دیگر','عمر و سرمایه گذاری').replace('زندگی','عمر و سرمایه گذاری')
        df.columns = ['Field', 'comp', 'مبلغ کل حق بیمه', 'count', 'countAdd', 'countdiff','tax','groupMain']
        df['_children'] = ''
        df = df.to_dict('records')
        for i in range(len(df)):
            fld = df[i]['Field']
            cmp = df[i]['comp']
            chld = children[children['Field']==fld]
            chld = chld[chld['comp']==cmp]
            chld['groupMain'] = df[i]['groupMain']
            chld = chld.to_dict('records')
            df[i]['_children'] = chld
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()
    

def revival(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        res = pishkarDb['Revival'].find({'username':username,'comp':data['dataRevival']['comp'],'کد رایانه صدور بیمه نامه':data['dataRevival']['کد رایانه صدور بیمه نامه']})
        if res!=None:
            pishkarDb['Revival'].delete_many({'username':username,'comp':data['dataRevival']['comp'],'کد رایانه صدور بیمه نامه':data['dataRevival']['کد رایانه صدور بیمه نامه']})
        pishkarDb['Revival'].insert_one({'username': username, 'comp': data['dataRevival']['comp'], 'کد رایانه صدور بیمه نامه': data['dataRevival']['کد رایانه صدور بیمه نامه'], 'RevivalResponse': data['RevivalResponse']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def revivalLife(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        if data['Status'] !='جاری':
            pishkarDb['issuingLife'].update_one({'username':username,'comp':data['dataRevival']['comp'],'شماره بيمه نامه':data['dataRevival']['شماره بيمه نامه']},{'$set':{'status':data['Status']}})
            pp = pishkarDb['issuingLife'].find_one({'username':username,'comp':data['dataRevival']['comp'],'شماره بيمه نامه':data['dataRevival']['شماره بيمه نامه']})
            return json.dumps({'replay':False,'msg':'وضعیت بیمه نامه تغییر کرد و از لیست خارج شد'})
        dic = {'username':username,'شماره بيمه نامه':data['dataRevival']['شماره بيمه نامه'],'comp':data['dataRevival']['comp'],'RevivalResponse':data['dataRevival']['RevivalResponse'],'qest':data['dataRevival']['qest']}
        if pishkarDb['RevivalLife'].find_one({'username':username,'شماره بيمه نامه':data['dataRevival']['شماره بيمه نامه'],'comp':data['dataRevival']['comp'],'qest':data['dataRevival']['qest']}) == None:
            pishkarDb['RevivalLife'].insert_one(dic)
        else:
            pishkarDb['RevivalLife'].update_one({'username':username,'شماره بيمه نامه':data['dataRevival']['شماره بيمه نامه'],'comp':data['dataRevival']['comp'],'qest':data['dataRevival']['qest']},{'$set':{'RevivalResponse':data['dataRevival']['RevivalResponse']}})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()
    
def lifefile(cookier,file,comp):
    user = cookie(cookier)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        reqColumns =['مدت','تاريخ شروع','تاريخ  انقضاء','شماره رایانه بیمه نامه','روش پرداخت','طرح','شماره بيمه نامه','نام بیمه گذار','کد ملی بیمه گذار','ضریب رشد سالانه سرمایه بیمه','ضریب رشد سالانه حق بیمه','تعداد اقساط در سال','حق بیمه هر قسط \n(جمع عمر و پوششها)']
        df = pd.read_excel(file,dtype={'شماره بيمه نامه':str})
        df.columns = [x.replace('مدت ', 'مدت').replace('حق بیمه هر قسط _x000D_\n(جمع عمر و پوششها)','حق بیمه هر قسط \n(جمع عمر و پوششها)') for x in df.columns]
        try: df['تعداد اقساط در سال'] = [int(x) for x in df['تعداد اقساط در سال']]
        except: return json.dumps({'replay':False,'msg':"ستون 'تعداد اقساط در سال' میبایست فقط اعداد باشد"})
        for x in reqColumns:
            if x not in df.columns:
                return json.dumps({'replay':False,'msg':f'ستون "{x}" در فایل موجود نیست'})
        dff = pd.DataFrame(pishkarDb['issuingLife'].find({'comp':comp},{'_id':0}))
        df['comp'] = comp
        df['username'] = username
        df = pd.concat([df,dff])
        df = df.drop_duplicates(subset=['شماره بيمه نامه'],keep='last')
        try:df['status'] = df['status'].fillna('جاری')
        except:df['status'] = 'جاری'
        df = df.to_dict('records')
        pishkarDb['issuingLife'].insert_many(df)
    return json.dumps({'replay':True})


def deldfLife(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['issuingLife'].delete_many({'comp':data['data']['comp']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def addissuinglifemanual(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    methodPay = {'12':'اقساط ماهانه','6':'اقساط دو ماهه','4':'اقساط سه ماهه','3':'اقساط چهار ماهه','2':'اقساط شش ماهه','1':'سالانه'}
    if user['replay']:
        IssuingDict = data['IssuingDict']
        IssuingDict['username'] = username
        if pishkarDb['issuingLife'].find_one({'username':username,'شماره بيمه نامه':IssuingDict['شماره بيمه نامه']})!=None:pishkarDb['issuingLife'].delete_one({'شماره بيمه نامه':IssuingDict['شماره بيمه نامه']})
        IssuingDict['روش پرداخت'] = methodPay[str(IssuingDict['تعداد اقساط در سال'])]
        IssuingDict['حق بیمه هر قسط \n(جمع عمر و پوششها)'] = int(IssuingDict['حق بیمه']) / int(IssuingDict['تعداد اقساط در سال'])
        IssuingDict['تاريخ  انقضاء'] = IssuingDict['تاريخ  پایان']
        IssuingDict['status'] = 'جاری'
        pishkarDb['issuingLife'].insert_one(IssuingDict)
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()


def revivalall(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['Revival'].find({'username':username},{'_id':0,'کد رایانه صدور بیمه نامه':1,'RevivalResponse':1})).set_index('کد رایانه صدور بیمه نامه')
        if len(df)==0:
            return json.dumps({'replay':False,'msg':'موردی یافت نشد'})
        issuing = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0})).set_index('کد رایانه صدور بیمه نامه')
        df = df.join(issuing).reset_index()
        df = df.fillna('')
        df['RevivalResponse'] = df['RevivalResponse'].replace('extended','تمدید شد').replace('not extended','تمدید نشد').replace('no need','عدم نیاز')
        df = df.to_dict('records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()

def addenLife(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['AddenLife'].find({'username':username},{'_id':0,'username':0}))

        df = df.to_dict('records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()

def getinsurerbynum(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        ins = pishkarDb['issuingLife'].find_one({'username':username,'شماره بيمه نامه':data['IssuingDict']['شماره بيمه نامه'],'status':'جاری'},{'_id':'0','تاريخ  انقضاء':1,'ضریب رشد سالانه حق بیمه':1,'تعداد اقساط در سال':1,'حق بیمه هر قسط \n(جمع عمر و پوششها)':1,'تاريخ شروع':1,'شماره بيمه نامه':1,'comp':1})
        if ins==None:
            return json.dumps({'replay':False,'msg':'بیمه نامه ای یافت نشد'})
        return json.dumps({'replay':True,'ins':ins})
    else:
        return ErrorCookie()

def AddLifeAdden(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        cheackNumber = pishkarDb['issuingLife'].find_one({'username':username,'شماره بيمه نامه':data['IssuingDict']['شماره بيمه نامه'],'status':'جاری'})
        if cheackNumber==None:
            return json.dumps({'replay':False,'msg':'شماره بیمه نامه اشتباه است'})
        addenDict = data['IssuingDict']
        if len(addenDict['تاريخ شروع'].split('/'))!=3 or len(addenDict['تاريخ شروع'])!=10:
            return json.dumps({'replay':False,'msg':'فرمت تاریخ شروع اشتباه است'})
        if len(addenDict['تاريخ  انقضاء'].split('/'))!=3 or len(addenDict['تاريخ  انقضاء'])!=10:
            return json.dumps({'replay':False,'msg':'فرمت تاريخ انقضاء اشتباه است'})
        if str(addenDict['تعداد اقساط در سال']) not in ['1','2','3','4','6','12']:
            return json.dumps({'replay':False,'msg':'تعداد اقساط در سال اشتباه است'})
        pishkarDb['AddenLife'].delete_many({'username':username,'تاريخ شروع': addenDict['تاريخ شروع'],'شماره بيمه نامه': addenDict['شماره بيمه نامه']})
        pishkarDb['AddenLife'].insert_one({'username':username,'تاريخ شروع': addenDict['تاريخ شروع'], 'تاريخ  انقضاء':addenDict['تاريخ  انقضاء'], 'شماره بيمه نامه': addenDict['شماره بيمه نامه'], 'ضریب رشد سالانه حق بیمه': addenDict['ضریب رشد سالانه حق بیمه'], 'تعداد اقساط در سال': addenDict['تعداد اقساط در سال'], 'حق بیمه هر قسط \n(جمع عمر و پوششها)': addenDict['حق بیمه هر قسط \n(جمع عمر و پوششها)'], 'comp':addenDict['comp']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()


def deladdenlife(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        (data)
        pishkarDb['AddenLife'].delete_many({'username':username,'تاريخ شروع':data['dict']['تاريخ شروع'],'شماره بيمه نامه':data['dict']['شماره بيمه نامه']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()


def getnuminslist(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username},{'_id':0,'شماره بيمه نامه':1}))['شماره بيمه نامه'].to_list()
        return json.dumps({'replay':True,'lst':df})
    else:
        return ErrorCookie()


def liferevivalgroup(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        for i in data['group']:
            dic = {'username':username,'شماره بيمه نامه':i['شماره بيمه نامه'],'comp':i['comp'],'RevivalResponse':data['revival'],'qest':i['qest']}
            if pishkarDb['RevivalLife'].find_one({'username':username,'شماره بيمه نامه':i['شماره بيمه نامه'],'comp':i['comp'],'qest':i['qest']}) == None:
                pishkarDb['RevivalLife'].insert_one(dic)
            else:
                pishkarDb['RevivalLife'].update_one({'username':username,'شماره بيمه نامه':i['شماره بيمه نامه'],'comp':i['comp'],'qest':i['qest']},{'$set':{'RevivalResponse':data['revival']}})
        return json.dumps({'replay':True,'lst':'df'})
    else:
        return ErrorCookie()

def noFee(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        insurer = pishkarDb['insurer'].find({'username':username},{'_id':0,'نام':1,'بیمه گر':1})
        fee = pd.DataFrame(pishkarDb['Fees'].find({'username':username},{'_id':0,'شماره بيمه نامه':1,'comp':1,'UploadDate':1,'کد رایانه صدور':1,'تاریخ صدور بیمه نامه':1}))
        fee['کد رایانه صدور'] = fee['کد رایانه صدور'].fillna(0)
        fee['کد رایانه صدور'] = [str(int(x)) for x in fee['کد رایانه صدور']]
        for i in insurer:
            fee['comp'] = fee['comp'].replace(i['نام'],i['بیمه گر'])
        fee['شماره بيمه نامه'] = fee['شماره بيمه نامه'].astype(str)

        life = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username},{'_id':0,'شماره بيمه نامه':1,'نام بیمه گذار':1,'comp':1,'تاريخ صدور':1,'تاريخ شروع':1}))
        life['رشته'] = 'زندگی'
        life['مورد بیمه'] = ''
        life['تاريخ صدور'] = life['تاريخ صدور'].fillna(life['تاريخ شروع'])
        life = life.drop(columns=['تاريخ شروع'])
        life['شماره بيمه نامه'] = [str(x).replace('\t','') for x in life['شماره بيمه نامه']]

        life = life.set_index('شماره بيمه نامه').join(fee.set_index('شماره بيمه نامه')[['UploadDate']],how='left')
        life = life.reset_index().drop_duplicates(subset=['شماره بيمه نامه'])
        life['UploadDate'] = life['UploadDate'].isnull()
        life = life[life['UploadDate']==True]
        if username == '09131533223':
            life['dateInt'] = life['تاريخ صدور'].fillna('0')
            life['dateInt'] = [int(x.replace('/','')) for x in life['dateInt']]
            life = life[life['dateInt']>14010631]
            life = life.drop(columns=['dateInt'])

        noLife = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0,'شماره بيمه نامه':1,'رشته':1,'پرداخت کننده حق بیمه':1,'مورد بیمه':1,'کد رایانه صدور بیمه نامه':1,'تاریخ عملیات':1,'comp':1}))
        noLife = noLife.drop_duplicates(subset=['کد رایانه صدور بیمه نامه','comp'])
        noLife['کد رایانه صدور بیمه نامه'] = [str(int(x)) for x in noLife['کد رایانه صدور بیمه نامه']]
        noLife = noLife.rename(columns={'کد رایانه صدور بیمه نامه':'کد رایانه صدور','پرداخت کننده حق بیمه':'نام بیمه گذار','تاریخ عملیات':'تاريخ صدور'})
        noLife = noLife.set_index(['کد رایانه صدور','comp']).join(fee.set_index(['کد رایانه صدور','comp'])[['UploadDate']],how='left')
        noLife = noLife.reset_index().drop_duplicates(subset=['کد رایانه صدور','comp'])
        noLife['UploadDate'] = noLife['UploadDate'].isnull()
        noLife = noLife[noLife['UploadDate']==True]
        df = pd.concat([life,noLife]).drop(columns=['UploadDate'])
        df = df.fillna('')

 

        # یافت شرکت هایی که کد رایانه صدور ندارند
        noIdIssuing = ['ایران','کوثر']
        feeSmale = fee[fee['comp'].isin(noIdIssuing)].drop_duplicates(subset=['شماره بيمه نامه'])
        feeSmale['شماره بيمه نامه'] = [x.split('/')[-1] for x in feeSmale['شماره بيمه نامه']]
        feeSmale = feeSmale.rename(columns={'تاریخ صدور بیمه نامه':'تاريخ صدور'})
        feeSmale = feeSmale.set_index(['شماره بيمه نامه','تاريخ صدور','comp'])[['UploadDate']]

        df = df.set_index(['شماره بيمه نامه','تاريخ صدور','comp']).join(feeSmale,how='left')
        df = df.reset_index()

        df['UploadDate'] = df['UploadDate'].isnull()
        df = df[df['UploadDate']==True].drop(columns=['UploadDate'])
        df = df.reset_index().drop(columns='index').reset_index()
        df['index'] = df['index']+1



        df = df.to_dict('records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()