import json
import numpy as np
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import timedate
import datetime
import sms
client = pymongo.MongoClient()
pishkarDb = client['pishkar']

def NcToName(nc,username):
    name = pishkarDb['cunsoltant'].find_one({'username':username,'nationalCode':nc})
    if name != None:
        name = name['fristName'] + ' ' +name['lastName']
        return name
    else:
        return ''

def profile(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        user = user['user']
        useNew = data['userNew']
        pishkarDb['username'].update_many({'phone':user['phone']},{'$set':useNew},upsert=False)
        return json.dumps({'replay':True,'msg':'تغییرات با موفقیت ثبت شد'})
    else:
        return ErrorCookie()


def cunsoltant(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:

        # برخی مواقع این مقدار به صورت استرینگ میرسد با این حرکت اصلاح کردیم
        if data['cunsoltant']['active'] == 'false':data['cunsoltant']['active'] = False
        else: data['cunsoltant']['active'] = True
        #------------------------------------------------------------------------------

        cheakNC = pishkarDb['cunsoltant'].find_one({'nationalCode':data['cunsoltant']['nationalCode']})
        cheakP = pishkarDb['cunsoltant'].find_one({'phone':data['cunsoltant']['phone']})
        if cheakNC == None:
            if cheakP == None:
                data['cunsoltant']['username'] = user['user']['phone']
                pishkarDb['cunsoltant'].insert_one(data['cunsoltant'])
                return json.dumps({'replay':True})
            else:
                data['cunsoltant']['username'] = user['user']['phone']
                pishkarDb['cunsoltant'].update_many({'username':username,'phone':data['cunsoltant']['phone']},{'$set':data['cunsoltant']})
                return json.dumps({'replay':True})
        else:
            
            data['cunsoltant']['username'] = user['user']['phone']
            pishkarDb['cunsoltant'].update_many({'username':username,'nationalCode':data['cunsoltant']['nationalCode']},{'$set':data['cunsoltant']})
            return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def getcunsoltant(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username},{'_id':0,'fristName':1,'lastName':1,'nationalCode':1,'gender':1,'phone':1,'salary':1,'childern':1,'freetaxe':1,'salaryGroup':1,'insureWorker':1,'insureEmployer':1,'active':1}))
        if user['Authorization']['all'] == False:
            if user['Authorization']['level'] == 'شخص مشاور':
                df = df[df['phone']==user['Authorization']['phone']]
                df = df.to_dict('records')
                return json.dumps({'replay':True, 'df':df})
            
        if len(df)==0:
            return json.dumps({'replay':False, 'msg':'هیچ مشاوری تعریف نشده'})
        if 'active' not in df.columns: df['active'] = True
        df['active'] = df['active'].fillna(True)
        try:
            if data['FilterActive']!='all':
                data['FilterActive'] = data['FilterActive']=='true'
                df = df[df['active']==data['FilterActive']]
        except:
            pass
        df = df.fillna('')
        try:
            if data['integration']==False:
                df = df[df['gender']!='گروه']
                df = df[df['fristName']!='تلفیق']
        except:
            df = df[df['gender']!='گروه']
            df = df[df['fristName']!='تلفیق']
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True, 'df':df})
    else:
        return ErrorCookie()

def getcunsoltantAndAll(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username},{'_id':0,'fristName':1,'lastName':1,'nationalCode':1,'gender':1,'phone':1,'salary':1,'childern':1,'freetaxe':1,'salaryGroup':1,'insureWorker':1,'insureEmployer':1}))
        if user['Authorization']['all'] == False:
            if user['Authorization']['level'] == 'شخص مشاور':
                df = df[df['phone']==user['Authorization']['phone']]
                df = df.to_dict('records')
                return json.dumps({'replay':True, 'df':df, 'operation':False})
        if len(df)==0:
            return json.dumps({'replay':False, 'msg':'هیچ مشاوری تعریف نشده'})
        if 'active' not in df.columns: df['active'] = True
        df['active'] = df['active'].fillna(True)
        try:
            if data['FilterActive']!='all':
                data['FilterActive'] = data['FilterActive']=='true'
                df = df[df['active']==data['FilterActive']]
        except:
            pass
        df = df.fillna('')
        try:
            if data['integration']==False:
                df = df[df['gender']!='گروه']
                df = df[df['fristName']!='تلفیق']
        except:
            df = df[df['gender']!='گروه']
            df = df[df['fristName']!='تلفیق']
        df = df.to_dict(orient='records')
        if len(df)>1:
            df.append({'fristName': 'همه', 'lastName': '', 'nationalCode': 'all', 'gender': '', 'phone': '', 'salary': '', 'childern': '', 'freetaxe': '', 'insureWorker': '', 'insureEmployer': '', 'salaryGroup': '', 'active': True})
        return json.dumps({'replay':True, 'df':df,'operation':True})
    else:
        return ErrorCookie()
    

def delcunsoltant(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        allowDel = pishkarDb['assing'].find_one({'username':username,'nationalCode':data['nationalCode']}) ==None
        if allowDel:
            pishkarDb['cunsoltant'].delete_many({'nationalCode':data['nationalCode']})
            return json.dumps({'replay':True})
        else:
            return json.dumps({'replay':False,'msg':'حذف انجام نشد، این مشاور  به بیمه نامه ای تخصیص یافته'})

    else:
        return ErrorCookie()


def addinsurer(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        columns = data['insurer']
        columns['username'] = username
        find = pishkarDb['insurer'].find_one({'نام':columns['نام'],'username':username})==None
        if find:
            pishkarDb['insurer'].insert_one(columns)
            return json.dumps({'replay':True,'msg':'ثبت شده'})
        else:
            return json.dumps({'replay':False,'msg':'بیمه گر با این نام قبلا ثبت شده'})
    else:
        return ErrorCookie()

    
def settax(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        lavel = data['level']
        lavel['username'] = username
        if pishkarDb['taxe'].find_one({'username':username,'year':lavel['year']}) == None:
            pishkarDb['taxe'].insert_one(lavel)
        else:
            pishkarDb['taxe'].update_one({'username':username,'year':lavel['year']},{'$set':lavel})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()


def gettax(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['taxe'].find({'username':username},{'_id':0,'username':0}))
        df = df.replace('',0)
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie() 
    
def deltax(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['taxe'].find_one_and_delete({'username':username,'year':data['year']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie() 
    
def getinsurer(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        insurerList = pd.DataFrame((pishkarDb['insurer'].find({'username':username},{'_id':0,'نام':1,'بیمه گر':1})))
        insurerList = insurerList.to_dict(orient='records')
        return json.dumps({'replay':True, 'list':insurerList})
    else:
        return ErrorCookie()
    
def delinsurer(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['insurer'].delete_many({'username':username,'نام':data['name']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()


def salary(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        salary = data['salary']
        salary['username'] = username
        if pishkarDb['salary'].find_one({'username':username, 'year':salary['year'],'gruop':salary['gruop']})==None:
            pishkarDb['salary'].insert_one(salary)
            return json.dumps({'replay':True , 'msg':'ثبت شده'})
        else:
            pishkarDb['salary'].update_one({'username':username, 'year':salary['year'],'gruop':salary['gruop']},{'$set':salary})
            return json.dumps({'replay':True, 'msg':'بروز رسانی شد'})
    else:
        return ErrorCookie()

def getsalary(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = [x for x in pishkarDb['salary'].find({'username':username},{'_id':0})]
        return json.dumps({'df':df})
    else:
        return ErrorCookie()

def delsalary(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['salary'].delete_one({'username':username, 'year':data['year']})
        return json.dumps({'replat':True})
    else:
        return ErrorCookie()

def setsub(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']

    if user['replay']:
        if data['sub']['level'] == 'شخص مشاور':
            inConsultant = pishkarDb['cunsoltant'].find_one({'username':username,'phone':data['sub']['phone']})
            if inConsultant == None:
                return json.dumps({'replay':False,'msg':'کاربر با این شماره همراه در مشاوران یافت نشد'})
        if pishkarDb['sub'].find_one({'username':username,'subPhone':data['sub']['phone']}) != None:
            pishkarDb['sub'].delete_many({'username':username,'subPhone':data['sub']['phone']})
        sub = data['sub']
        sub['username'] = username
        sub['subPhone'] = data['sub']['phone']
        pishkarDb['sub'].insert_one(sub)
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()


def getgroupsalary(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = list(pishkarDb['salary'].find({'username':username},{'_id':0,'gruop':1}))
        if len(df)==0:
            return json.dumps({'replay':False,'msg':'هیچ گروه حقوق و دسمتزدی یافت نشد'})
        df = [x['gruop'] for x in df]
        return json.dumps({'replay':True, 'df':df})
    else:
        return ErrorCookie()

def addbranche(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        subList = [x['subconsultant'] for x in data['SubConsultantList']]
        subList = list(set(subList))
        if data['managementBranche'] in subList:
            return json.dumps({'replay':False, 'msg':'مدیر شعبه در مشاوران شعبه تکرار شده است'})
        branche = pd.DataFrame(pishkarDb['branches'].find({}))
        if len(branche)>0:
            brancheSubList = []
            for i in list(branche[branche['BranchesName']!=data['BranchesName']]['SubConsultantList']):
                for j in i:
                    brancheSubList.append(j)
            for i in subList:
                if i in brancheSubList:
                    name = NcToName(i,username)
                    return json.dumps({'replay':False,'msg':f'{name} در شعبه دیگری ثبت شده'})
        if pishkarDb['branches'].find_one({'username':username,'BranchesName':data['BranchesName']})!=None:
            pishkarDb['branches'].update_one({'username':username,'BranchesName':data['BranchesName']},{'$set':{'managementBranche':data['managementBranche'],'SubConsultantList':subList}})
            return json.dumps({'replay':True})
        if pishkarDb['branches'].find_one({'username':username,'management':data['managementBranche']})!=None:
            return json.dumps({'replay':False, 'msg':'مدیر شعبه تکراری است'})
        pishkarDb['branches'].insert_one({'username':username,'BranchesName':data['BranchesName'],'managementBranche':data['managementBranche'],'SubConsultantList':subList})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()


def getbranche(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['branches'].find({'username':username},{'_id':0}))
        if len(df)>0:
            df['managementBranche'] = [NcToName(x,username) for x in df['managementBranche']]
            for i in df.index:
                if len(df['SubConsultantList'][i])==0:
                    df['SubConsultantList'][i] = ''
                else:
                    listsubi = ''
                    for j in df['SubConsultantList'][i]:
                        listsubi = listsubi + ' , ' +(NcToName(j,username))
                    df['SubConsultantList'][i] = listsubi[3:]

            df = df.to_dict(orient='records')
            return json.dumps({'replay':True,'df':df})
        else:
            return json.dumps({'replay':True,'msg':'هیچ شعبه ای ثبت نشده'})
    else:
        return ErrorCookie()


def delbranche(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        dl = pishkarDb['branches'].find_one_and_delete({'username':username,'BranchesName':data['name']})
        if dl != None:
            return json.dumps({'replay':True})
        else:
            return json.dumps({'replay':False,'msg':'شعبه یافت نشد'})
    else:
        return ErrorCookie()

def addminsalary(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pishkarDb['minimumSalary'].find_one({'username':username,'year':data['minimum']['year']})
        if df == None:
            pishkarDb['minimumSalary'].insert_one({'username':username,'year':data['minimum']['year'],'value':data['minimum']['value']})
        else:
            pishkarDb['minimumSalary'].update_one({'username':username,'year':data['minimum']['year']},{'$set':{'value':data['minimum']['value']}})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def getminsalary(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = [x for x in pishkarDb['minimumSalary'].find({'username':username},{'_id':0,'username':0})]
        if len(df)==0:
            return json.dumps({'replay':False})
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()

def delminsalary(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['minimumSalary'].delete_one({'username':username,'year':data['year']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def workinghour(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        find = pishkarDb['workingHours'].find_one({'username':username,'Period':data['Period']['Show']})
        if find ==None:
            pishkarDb['workingHours'].delete_many({'username':username,'Period':data['Period']['Show']})
        pishkarDb['workingHours'].insert_one({'username':username,'Period':data['Period']['Show'],'date':data['Period']['date'],'hours':data['Hours']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()


def getworkinghour(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['workingHours'].find({'username':username},{'_id':0,'username':0})).to_dict('records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()

def getaworkinghour(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pishkarDb['workingHours'].find_one({'username':username,'Period':data['date']['Show']},{'_id':0,'hours':1})
        if df == None: df = 0
        else: df = df['hours']
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()
    
def delworkinghour(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['workingHours'].delete_many({'username':username,'Period':data['date']['Show']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def addVacation(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['vacation'].insert_one({'username':username,'consultant':data['consultant'],'Hours':data['Hours'],'Show':data['Period']['Show'],'date':data['Period']['date']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def getvacation(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pishkarDb['vacation'].find({'username':username})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()
    

def delsub(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['sub'].delete_many({'username':username,'subPhone':data['phone']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()


def setsettingsms(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['settingSms'].delete_many({'username':username})
        for i in data['listSend']:
            dic = i
            dic['username'] = username
            pishkarDb['settingSms'].insert_one(dic)
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()



def getsettingsms(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['settingSms'].find({'username':username},{'_id':0,'username':0}))
        df = df.sort_values('name')
        df = df.to_dict('records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()



def getallrevivalbydate(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['SendedSms'].find({'username':username},{'_id':0,'username':0,'codeDeliver':0}))
        if len(df) == 0:
            return json.dumps({'replay':False,'msg':'هیچ پیامکی یافت نشد'})
        if data['Date']['from'] != None:
            frm = datetime.datetime.fromtimestamp(data['Date']['from']/1000)
            df = df[df['date']>=frm]
        if data['Date']['to'] != None:
            to = datetime.datetime.fromtimestamp(data['Date']['to']/1000)
            df = df[df['date']<=to]
        if len(df) == 0:
            return json.dumps({'replay':False,'msg':'هیچ پیامکی یافت نشد'})
        df['dateJalali'] = [str(timedate.GregorianToPersian(x)).replace('-','/') for x in df['date']]
        df['time'] = [str(x.hour)+':'+str(x.minute) for x in df['date']]
        df = df.drop(columns=['date'])
        df = df.to_dict('records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()



def personalized(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        for i in data['listCustomer']:
            chphone = timedate.CheckPhone(i['تلفن همراه'])
            name = i['name']
            if chphone['replay']==False:
                return json.dumps({'replay':False,'msg':f'شماره همراه {name} صحیح نیست'})
            phone = chphone['phone']
            codrDeliver = sms.SendSms(phone,data['textMsg'])
            pishkarDb['SendedSms'].insert_one({'username':username,'codeIns':0,'customer':name,'codeDeliver':codrDeliver,'text':data['textMsg'],'prevDay':0,'date':datetime.datetime.now(),'hours':0,'deliver':False})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()