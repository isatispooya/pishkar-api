import json
import timedate
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
from timedate import ColorRandom
client = pymongo.MongoClient()
pishkarDb = client['pishkar']


def issunigSum(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0, 'ردیف':0,
            'شعبه واحد صدور بیمه نامه یا الحاقیه':0, 'واحدصدور بیمه نامه':0, 'نمایندگی بیمه نامه':0,
            'كد رايانه بيمه نامه':0, 'کد رایانه عملیات': 0, 'تاريخ سند دريافتي':0 ,'تاريخ واگذاري':0 ,
            'تاريخ وضعیت وصول':0, 'وضعيت وصول':0 , 'شماره سند دريافتي':0 ,'شرح دريافتي':0 , 'شماره حساب':0,
            'حساب واگذاري':0, 'بانك':0, 'شرح عملیات':0,'username':0, 'additional':0}))
        df = df.drop_duplicates(subset=['شماره بيمه نامه','شماره الحاقیه','تاريخ بيمه نامه يا الحاقيه','مبلغ کل حق بیمه','comp','کد رایانه صدور بیمه نامه'])
        dfLifie = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username},{'تاريخ صدور':1,'حق بیمه هر قسط \n(جمع عمر و پوششها)':1,'تعداد اقساط در سال':1}))
        dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)'] = [int(x) for x in dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)']]
        dfLifie['تعداد اقساط در سال'] = [int(x) for x in dfLifie['تعداد اقساط در سال']]
        dfLifie['مبلغ کل حق بیمه'] = dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)'] * dfLifie['تعداد اقساط در سال']
        dfLifie = dfLifie[['تاريخ صدور', 'مبلغ کل حق بیمه']]
        dfLifie.columns = ['تاریخ عملیات', 'مبلغ کل حق بیمه']
        dfLifie = dfLifie.dropna(subset=['تاریخ عملیات'])
        df['تاریخ عملیات عددی'] = [timedate.dateToInt(x) for x in df['تاریخ عملیات']]
        df = df.dropna(subset=['تاریخ عملیات'])
        df['group'] = df['تاریخ عملیات']
        df = pd.concat([df,dfLifie])
        if data['period'] == '7': df['group'] = [timedate.PersianToGregorianWeek(x) for x in df['تاریخ عملیات']]
        if data['period'] == '30': df['group'] = [timedate.PersianToGregorianMonth(x) for x in df['تاریخ عملیات']]
        if data['period'] == '90': df['group'] = [timedate.PersianToGregorianTrimester(x) for x in df['تاریخ عملیات']]
        if data['period'] == '180': df['group'] = [timedate.PersianToGregorianSixter(x) for x in df['تاریخ عملیات']]
        if data['period'] == '365': df['group'] = [timedate.PersianToGregorianSixter(x) for x in df['تاریخ عملیات']]
        df = df.groupby(['group']).sum(numeric_only=True)[['مبلغ کل حق بیمه']]
        df = df.sort_index(ascending=False).reset_index()
        df = df[df.index>df.index.max()-15]
        labels = df['group'].to_list()
        dataa = df['مبلغ کل حق بیمه'].to_list()
        return json.dumps({'replay':True,'df':{'labels':labels,'datasets':[{'id':0,'data':dataa}]}})
    else:
        return ErrorCookie()

def issunigFeild(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        dfLifie = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username},{'تاريخ شروع':1,'حق بیمه هر قسط \n(جمع عمر و پوششها)':1,'تعداد اقساط در سال':1}))
        dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)'] = [int(x) for x in dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)']]
        dfLifie['تعداد اقساط در سال'] = [int(x) for x in dfLifie['تعداد اقساط در سال']]
        dfLifie['مبلغ کل حق بیمه'] = dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)'] * dfLifie['تعداد اقساط در سال']
        dfLifie = dfLifie[['تاريخ شروع', 'مبلغ کل حق بیمه']]
        dfLifie.columns = ['تاریخ عملیات', 'مبلغ کل حق بیمه']
        dfLifie = dfLifie.dropna(subset=['تاریخ عملیات'])
        dfLifie['تاریخ عملیات'] = [timedate.dateToInt(x) for x in dfLifie['تاریخ عملیات']]
        stndrd = pd.DataFrame(pishkarDb['standardfee'].find({'username':username},{'_id':0, 'field':1, 'groupMain':1,'groupSub':1}))
        df = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0, 'رشته':1,
            'کد رایانه صدور بیمه نامه':1,'شماره الحاقیه':1, 'مورد بیمه':1,'تاريخ بيمه نامه يا الحاقيه':1, 'تاریخ عملیات':1,'مبلغ کل حق بیمه':1, 'comp': 1, 'additional':1 }))
        df['تاریخ عملیات'] = [timedate.dateToInt(x) for x in df['تاریخ عملیات']]
        df = df.fillna('')
        df = df.drop_duplicates(subset=['کد رایانه صدور بیمه نامه','شماره الحاقیه','تاريخ بيمه نامه يا الحاقيه','مبلغ کل حق بیمه','comp','کد رایانه صدور بیمه نامه'])
        df['Field'] =  df['رشته'] + ' ('+df['مورد بیمه']+')'
        df['Field'] = [str(x).replace(' ()','') for x in df['Field']]
        df = df.set_index(['Field']).join(stndrd.set_index(['field']),how='left').reset_index()
        OnToday = timedate.toDayInt()
        OnPeroid = timedate.deltaToDayInt(data['period'],OnToday)
        df = df[df['تاریخ عملیات']>OnPeroid]
        dfLifie = dfLifie[dfLifie['تاریخ عملیات']>OnPeroid]
        dfLifie['groupMain'] = 'زندگی'
        df = pd.concat([df,dfLifie])
        df = df.groupby(by=['groupMain']).sum(numeric_only=True)[['مبلغ کل حق بیمه']]
        df = df.sort_values(by=['مبلغ کل حق بیمه'],ascending=False).reset_index()
        df['rate'] = (df['مبلغ کل حق بیمه'] / df['مبلغ کل حق بیمه'].sum())
        dff = df[df.index<6]
        if dff['rate'].sum()<1:
            dff = pd.concat([dff , pd.DataFrame([{'groupMain':'دیگر','مبلغ کل حق بیمه':df['مبلغ کل حق بیمه'].sum() - dff['مبلغ کل حق بیمه'].sum(),'rate':1-dff['rate'].sum()}])])
        dff['rate'] = [int(x*1000)/10 for x in dff['rate']]
        dff['مبلغ کل حق بیمه'] = [int(x/1000000) for x in dff['مبلغ کل حق بیمه']]
        dff['groupMain'] = [str(x).replace('بیمه','') for x in dff['groupMain']]
        dff['color'] = 0
        color = [ColorRandom() for x in dff['color']]
        dff['color'] = color
        labels = dff['groupMain'].to_list()
        dataa = dff['مبلغ کل حق بیمه'].to_list()
        dff = dff.to_dict('records')
        return json.dumps({'replay':True,'df':{'labels':labels,'datasets':[{'id':0,'data':dataa}]},'color':color,'dff':dff})
    else:
        return ErrorCookie()

def issuniginsurer(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        dfLifie = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username},{'تاريخ صدور':1,'حق بیمه هر قسط \n(جمع عمر و پوششها)':1,'تعداد اقساط در سال':1,'comp':1}))
        dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)'] = [int(x) for x in dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)']]
        dfLifie['تعداد اقساط در سال'] = [int(x) for x in dfLifie['تعداد اقساط در سال']]
        dfLifie['مبلغ کل حق بیمه'] = dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)'] * dfLifie['تعداد اقساط در سال']
        dfLifie = dfLifie[['تاريخ صدور', 'مبلغ کل حق بیمه','comp']]
        dfLifie.columns = ['تاریخ عملیات', 'مبلغ کل حق بیمه','comp']
        dfLifie = dfLifie.dropna(subset=['تاریخ عملیات'])
        dfLifie['تاریخ عملیات'] = [timedate.dateToInt(x) for x in dfLifie['تاریخ عملیات']]

        df = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0, 'ردیف':0,
            'شعبه واحد صدور بیمه نامه یا الحاقیه':0, 'واحدصدور بیمه نامه':0, 'نمایندگی بیمه نامه':0,
            'كد رايانه بيمه نامه':0, 'کد رایانه عملیات': 0, 'تاريخ سند دريافتي':0 ,'تاريخ واگذاري':0 ,
            'تاريخ وضعیت وصول':0, 'وضعيت وصول':0 , 'شماره سند دريافتي':0 ,'شرح دريافتي':0 , 'شماره حساب':0,
            'حساب واگذاري':0, 'بانك':0, 'شرح عملیات':0,'username':0, 'additional':0}))
        df['تاریخ عملیات'] = [timedate.dateToInt(x) for x in df['تاریخ عملیات']]
        df = df.fillna('')
        df = df.drop_duplicates(subset=['شماره بيمه نامه','شماره الحاقیه','تاريخ بيمه نامه يا الحاقيه','مبلغ کل حق بیمه','comp','کد رایانه صدور بیمه نامه'])
        OnToday = timedate.toDayInt()
        OnPeroid = timedate.deltaToDayInt(data['period'],OnToday)
        dfLifie = dfLifie[dfLifie['تاریخ عملیات']>OnPeroid]
        df = df[df['تاریخ عملیات']>OnPeroid]
        df = pd.concat([df,dfLifie])
        df = df.groupby(by=['comp']).sum(numeric_only=True)[['مبلغ کل حق بیمه']]
        df = df.sort_values(by=['مبلغ کل حق بیمه'],ascending=False).reset_index()
        df['rate'] = (df['مبلغ کل حق بیمه'] / df['مبلغ کل حق بیمه'].sum())
        dff = df[df.index<6]
        if dff['rate'].sum()<1:
            dff = pd.concat([dff,pd.DataFrame([{'comp':'دیگر','مبلغ کل حق بیمه':df['مبلغ کل حق بیمه'].sum() - dff['مبلغ کل حق بیمه'].sum(),'rate':1-dff['rate'].sum()}])])
        dff['color'] = 0
        color = [ColorRandom() for x in dff['color']]
        dff['color'] = color
        dff['مبلغ کل حق بیمه'] = [int(x/1000000) for x in dff['مبلغ کل حق بیمه']]
        dff['rate'] = [int(x*1000)/10 for x in dff['rate']]
        labels = dff['comp'].to_list()
        dataa = dff['مبلغ کل حق بیمه'].to_list()
        dff = dff.to_dict('records')
        return json.dumps({'replay':True,'df':{'labels':labels,'datasets':[{'id':0,'data':dataa}]},'color':color,'dff':dff})
    else:
        return ErrorCookie()


def FeeSum(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['Fees'].find({'username':username},{'_id':0,'رشته':1,
            'مورد بیمه':1, 'کد رایانه صدور':1, 'كارمزد قابل پرداخت':1, 'کل مبلغ وصول شده': 1,
            'UploadDate':1 ,'comp':1 , 'تاریخ صدور بیمه نامه':1,'شماره بيمه نامه':1}))
        df = df.drop_duplicates(subset=['comp','UploadDate','کد رایانه صدور','شماره بيمه نامه','كارمزد قابل پرداخت'])
        df['UploadDate'] = [timedate.PriodStrToInt(x) for x in df['UploadDate']]
        df = df[['كارمزد قابل پرداخت','UploadDate']].groupby(by=['UploadDate']).sum(numeric_only=True)
        df = df.sort_values(by=['UploadDate'],ascending=False).reset_index()
        df = df[df.index<int(data['period'])]
        labels = df['UploadDate'].to_list()
        labels = [timedate.PeriodIntToPeriodStr(x) for x in labels]
        dataa = df['كارمزد قابل پرداخت'].to_list()
        return json.dumps({'replay':True,'df':{'labels':labels,'datasets':[{'id':0,'data':dataa}]}})
    else:
        return ErrorCookie()
    


def FeeFeild(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        stndrd = pd.DataFrame(pishkarDb['standardfee'].find({'username':username},{'_id':0, 'field':1, 'groupMain':1,'groupSub':1}))
        df = pd.DataFrame(pishkarDb['Fees'].find({'username':username},{'_id':0,'رشته':1, 'مورد بیمه':1,
            'كارمزد قابل پرداخت':1,'UploadDate':1, 'comp': 1,'UploadDate':1,'کد رایانه صدور':1,'شماره بيمه نامه':1}))
        df = df.fillna('')
        df = df.drop_duplicates(subset=['comp','UploadDate','کد رایانه صدور','شماره بيمه نامه','كارمزد قابل پرداخت'])
        df['Field'] =  df['رشته'] + ' ('+df['مورد بیمه']+')'
        df['Field'] = [str(x).replace(' ()','') for x in df['Field']]
        df = df.set_index(['Field']).join(stndrd.set_index(['field']),how='left').reset_index()
        df['UploadDate'] = [timedate.PriodStrToInt(x) for x in df['UploadDate']]
        start = int(timedate.FeeFeildStart(df['UploadDate'].max(),data['period']))
        df = df[df['UploadDate']>start]
        df = df[df['كارمزد قابل پرداخت']!='']
        df['كارمزد قابل پرداخت'] = [int(x) for x in df['كارمزد قابل پرداخت']]
        df = df[['كارمزد قابل پرداخت','groupMain']].groupby(by=['groupMain']).sum(numeric_only=True)
        df = df.sort_values(by=['كارمزد قابل پرداخت'], ascending=False).reset_index()
        df['rate'] = (df['كارمزد قابل پرداخت'] / df['كارمزد قابل پرداخت'].sum())
        dff = df[df.index<6]
        if dff['rate'].sum() < 1:
            dff = pd.concat([dff,pd.DataFrame([{'groupMain':'دیگر','كارمزد قابل پرداخت':df['كارمزد قابل پرداخت'].sum() - dff['كارمزد قابل پرداخت'].sum(),'rate':1-dff['rate'].sum()}])])
        dff['color'] = 0
        color = [ColorRandom() for x in dff['color']]
        dff['color'] = color
        dff['كارمزد قابل پرداخت'] = [int(x/1000000) for x in dff['كارمزد قابل پرداخت']]
        dff['rate'] = [int(x*1000)/10 for x in dff['rate']]
        dff['groupMain'] = [x.replace('بیمه','') for x in dff['groupMain']]
        labels = df['groupMain'].to_list()
        dataa = df['كارمزد قابل پرداخت'].to_list()
        dff = dff.to_dict('records')
        return json.dumps({'replay':True,'df':{'labels':labels,'datasets':[{'id':0,'data':dataa}]},'color':color,'dff':dff})
    else:
        return ErrorCookie()

def FeeInsurence(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['Fees'].find({'username':username},{'_id':0,'كارمزد قابل پرداخت':1,
            'UploadDate':1, 'comp': 1,'کد رایانه صدور':1,'شماره بيمه نامه':1}))
        df = df.fillna('')
        df = df.drop_duplicates(subset=['comp','UploadDate','کد رایانه صدور','شماره بيمه نامه','كارمزد قابل پرداخت'])
        df['UploadDate'] = [timedate.PriodStrToInt(x) for x in df['UploadDate']]
        start = int(timedate.FeeFeildStart(df['UploadDate'].max(),data['period']))
        df = df[df['UploadDate']>start]
        insurec = pd.DataFrame(pishkarDb['insurer'].find({'username':username},{'نام':1,'بیمه گر':1,'_id':0}))
        insurec = insurec.set_index('نام').to_dict(orient='dict')['بیمه گر']
        df['comp'] = [insurec[x] for x in df['comp']]
        df = df[df['كارمزد قابل پرداخت']!='']
        df['كارمزد قابل پرداخت'] = [int(x) for x in df['كارمزد قابل پرداخت']]
        df = df.groupby(by=['comp']).sum(numeric_only=True)[['كارمزد قابل پرداخت']]
        df = df.sort_values(by=['كارمزد قابل پرداخت'],ascending=False).reset_index()
        df['rate'] = (df['كارمزد قابل پرداخت'] / df['كارمزد قابل پرداخت'].sum())
        dff = df[df.index<6]
        if dff['rate'].sum() < 1:
            feePay = df['كارمزد قابل پرداخت'].sum() - dff['كارمزد قابل پرداخت'].sum()
            rate = 1-dff['rate'].sum()
            dfff = pd.DataFrame([{'comp':'دیگر','كارمزد قابل پرداخت':feePay,'rate':rate}])
            dff = pd.concat([dff,dfff])
        dff['color'] = 0
        color = [ColorRandom() for x in dff['color']]
        dff['color'] = color
        dff['كارمزد قابل پرداخت'] = [int(x/1000000) for x in dff['كارمزد قابل پرداخت']]
        dff['rate'] = [int(x*1000)/10 for x in dff['rate']]
        labels = df['comp'].to_list()
        dataa = df['كارمزد قابل پرداخت'].to_list()
        dff = dff.to_dict('records')
        return json.dumps({'replay':True,'df':{'labels':labels,'datasets':[{'id':0,'data':dataa}]},'color':color,'dff':dff})
    else:
        return ErrorCookie()


def cunsoltantIssunigSum(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        assing = pd.DataFrame(pishkarDb['AssingIssuing'].find({'username':username,'cunsoltant':data['cunsoltant']},{'_id':0,'username':0}))
        if len(assing)>0:
            df = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0, 'ردیف':0,
                'شعبه واحد صدور بیمه نامه یا الحاقیه':0, 'واحدصدور بیمه نامه':0, 'نمایندگی بیمه نامه':0,
                'كد رايانه بيمه نامه':0, 'کد رایانه عملیات': 0, 'تاريخ سند دريافتي':0 ,'تاريخ واگذاري':0 ,
                'تاريخ وضعیت وصول':0, 'وضعيت وصول':0 , 'شماره سند دريافتي':0 ,'شرح دريافتي':0 , 'شماره حساب':0,
                'حساب واگذاري':0, 'بانك':0, 'شرح عملیات':0,'username':0, 'additional':0}))
            df = df.drop_duplicates(subset=['شماره بيمه نامه','شماره الحاقیه','تاريخ بيمه نامه يا الحاقيه','مبلغ کل حق بیمه','comp','کد رایانه صدور بیمه نامه'])
            df = df.set_index(['comp','کد رایانه صدور بیمه نامه']).join(assing.set_index(['comp','کد رایانه صدور بیمه نامه']),how='left')
            df['cunsoltant'] = df['cunsoltant'].fillna(False)
            df = df[df['cunsoltant']!=False].reset_index().drop(columns='cunsoltant')
            df['تاریخ عملیات عددی'] = [timedate.dateToInt(x) for x in df['تاریخ عملیات']]
            df = df.dropna(subset=['تاریخ عملیات'])
            df['group'] = df['تاریخ عملیات']
        else:
            df = pd.DataFrame()
        assing = pd.DataFrame(pishkarDb['AssingIssuingLife'].find({'username':username,'consultant':data['cunsoltant']},{'_id':0,'username':0}))
        if len(assing)>0:
            dfLifie = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username},{'تاريخ صدور':1,'حق بیمه هر قسط \n(جمع عمر و پوششها)':1,'تعداد اقساط در سال':1,'شماره بيمه نامه':1}))
            dfLifie = dfLifie.set_index('شماره بيمه نامه').join(assing.set_index('شماره بيمه نامه'),how='left')
            dfLifie['consultant'] = dfLifie['consultant'].fillna(False)
            dfLifie = dfLifie[dfLifie['consultant']!=False].reset_index().drop(columns='consultant')
            dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)'] = [int(x) for x in dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)']]
            dfLifie['تعداد اقساط در سال'] = [int(x) for x in dfLifie['تعداد اقساط در سال']]
            dfLifie['مبلغ کل حق بیمه'] = dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)'] * dfLifie['تعداد اقساط در سال']
            dfLifie = dfLifie[['تاريخ صدور', 'مبلغ کل حق بیمه']]
            dfLifie.columns = ['تاریخ عملیات', 'مبلغ کل حق بیمه']
            dfLifie = dfLifie.dropna(subset=['تاریخ عملیات'])
        else:
            dfLifie = pd.DataFrame()
        df = pd.concat([df,dfLifie])
        if len(df)==0:
            return json.dumps({'replay':False,'msg':'بیمه نامه ای یافت نشد'})
        if data['period'] == '7': df['group'] = [timedate.PersianToGregorianWeek(x) for x in df['تاریخ عملیات']]
        if data['period'] == '30': df['group'] = [timedate.PersianToGregorianMonth(x) for x in df['تاریخ عملیات']]
        if data['period'] == '90': df['group'] = [timedate.PersianToGregorianTrimester(x) for x in df['تاریخ عملیات']]
        if data['period'] == '180': df['group'] = [timedate.PersianToGregorianSixter(x) for x in df['تاریخ عملیات']]
        if data['period'] == '365': df['group'] = [timedate.PersianToGregorianSixter(x) for x in df['تاریخ عملیات']]
        df = df.groupby(['group']).sum(numeric_only=True)[['مبلغ کل حق بیمه']]
        df = df.sort_index(ascending=False).reset_index()
        df = df[df.index>df.index.max()-15]
        labels = df['group'].to_list()
        dataa = df['مبلغ کل حق بیمه'].to_list()
        return json.dumps({'replay':True,'df':{'labels':labels,'datasets':[{'id':0,'data':dataa}]}})
    else:
        return ErrorCookie()
    
def cunsoltantIssunigFeild(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        assing = pd.DataFrame(pishkarDb['AssingIssuingLife'].find({'username':username,'consultant':data['cunsoltant']},{'_id':0,'username':0}))
        OnToday = timedate.toDayInt()
        OnPeroid = timedate.deltaToDayInt(data['period'],OnToday)
        if len(assing)>0:
            dfLifie = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username},{'تاريخ شروع':1,'حق بیمه هر قسط \n(جمع عمر و پوششها)':1,'تعداد اقساط در سال':1,'شماره بيمه نامه':1}))
            dfLifie = dfLifie.set_index('شماره بيمه نامه').join(assing.set_index('شماره بيمه نامه'),how='left')
            dfLifie['consultant'] = dfLifie['consultant'].fillna(False)
            dfLifie = dfLifie[dfLifie['consultant']!=False].reset_index().drop(columns='consultant')
            dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)'] = [int(x) for x in dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)']]
            dfLifie['تعداد اقساط در سال'] = [int(x) for x in dfLifie['تعداد اقساط در سال']]
            dfLifie['مبلغ کل حق بیمه'] = dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)'] * dfLifie['تعداد اقساط در سال']
            dfLifie = dfLifie[['تاريخ شروع', 'مبلغ کل حق بیمه']]
            dfLifie.columns = ['تاریخ عملیات', 'مبلغ کل حق بیمه']
            dfLifie = dfLifie.dropna(subset=['تاریخ عملیات'])
            dfLifie['تاریخ عملیات'] = [timedate.dateToInt(x) for x in dfLifie['تاریخ عملیات']]
            dfLifie = dfLifie[dfLifie['تاریخ عملیات']>OnPeroid]
            dfLifie['groupMain'] = 'زندگی'
        else:
            dfLifie = pd.DataFrame()
        assing = pd.DataFrame(pishkarDb['AssingIssuing'].find({'username':username,'cunsoltant':data['cunsoltant']},{'_id':0,'username':0}))
        if len(assing)>0:
            stndrd = pd.DataFrame(pishkarDb['standardfee'].find({'username':username},{'_id':0, 'field':1, 'groupMain':1,'groupSub':1}))
            df = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0, 'رشته':1,
                'کد رایانه صدور بیمه نامه':1,'شماره الحاقیه':1, 'مورد بیمه':1,'تاريخ بيمه نامه يا الحاقيه':1, 'تاریخ عملیات':1,'مبلغ کل حق بیمه':1, 'comp': 1, 'additional':1 }))
            df = df.set_index(['comp','کد رایانه صدور بیمه نامه']).join(assing.set_index(['comp','کد رایانه صدور بیمه نامه']),how='left')
            df['cunsoltant'] = df['cunsoltant'].fillna(False)
            df = df[df['cunsoltant']!=False].reset_index().drop(columns='cunsoltant')
            df['تاریخ عملیات'] = [timedate.dateToInt(x) for x in df['تاریخ عملیات']]
            df = df.fillna('')
            df = df.drop_duplicates(subset=['کد رایانه صدور بیمه نامه','شماره الحاقیه','تاريخ بيمه نامه يا الحاقيه','مبلغ کل حق بیمه','comp','کد رایانه صدور بیمه نامه'])
            df['Field'] =  df['رشته'] + ' ('+df['مورد بیمه']+')'
            df['Field'] = [str(x).replace(' ()','') for x in df['Field']]
            df = df.set_index(['Field']).join(stndrd.set_index(['field']),how='left').reset_index()
            df = df[df['تاریخ عملیات']>OnPeroid]
        else:
            df = pd.DataFrame()            
        df = pd.concat([df,dfLifie])
        if len(df)==0:
            return json.dumps({'replay':False,'msg':'بیمه نامه ای یافت نشد'})
        df = df.groupby(by=['groupMain']).sum(numeric_only=True)[['مبلغ کل حق بیمه']]
        df = df.sort_values(by=['مبلغ کل حق بیمه'],ascending=False).reset_index()
        df['rate'] = (df['مبلغ کل حق بیمه'] / df['مبلغ کل حق بیمه'].sum())
        dff = df[df.index<6]
        if dff['rate'].sum()<1:
            dff = pd.concat([dff , pd.DataFrame([{'groupMain':'دیگر','مبلغ کل حق بیمه':df['مبلغ کل حق بیمه'].sum() - dff['مبلغ کل حق بیمه'].sum(),'rate':1-dff['rate'].sum()}])])
        dff['rate'] = [int(x*1000)/10 for x in dff['rate']]
        dff['مبلغ کل حق بیمه'] = [int(x/1000000) for x in dff['مبلغ کل حق بیمه']]
        dff['groupMain'] = [str(x).replace('بیمه','') for x in dff['groupMain']]
        dff['color'] = 0
        color = [ColorRandom() for x in dff['color']]
        dff['color'] = color
        labels = dff['groupMain'].to_list()
        dataa = dff['مبلغ کل حق بیمه'].to_list()
        dff = dff.to_dict('records')
        return json.dumps({'replay':True,'df':{'labels':labels,'datasets':[{'id':0,'data':dataa}]},'color':color,'dff':dff})
    else:
        return ErrorCookie()


def cunsoltantIssunigInsurer(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        assing = pd.DataFrame(pishkarDb['AssingIssuingLife'].find({'username':username,'consultant':data['cunsoltant']},{'_id':0,'username':0}))
        OnToday = timedate.toDayInt()
        OnPeroid = timedate.deltaToDayInt(data['period'],OnToday)
        if len(assing)>0:
            dfLifie = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username},{'تاريخ صدور':1,'حق بیمه هر قسط \n(جمع عمر و پوششها)':1,'تعداد اقساط در سال':1,'comp':1,'شماره بيمه نامه':1}))
            dfLifie = dfLifie.set_index('شماره بيمه نامه').join(assing.set_index('شماره بيمه نامه'),how='left')
            dfLifie['consultant'] = dfLifie['consultant'].fillna(False)
            dfLifie = dfLifie[dfLifie['consultant']!=False].reset_index().drop(columns='consultant')
            dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)'] = [int(x) for x in dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)']]
            dfLifie['تعداد اقساط در سال'] = [int(x) for x in dfLifie['تعداد اقساط در سال']]
            dfLifie['مبلغ کل حق بیمه'] = dfLifie['حق بیمه هر قسط \n(جمع عمر و پوششها)'] * dfLifie['تعداد اقساط در سال']
            dfLifie = dfLifie[['تاريخ صدور', 'مبلغ کل حق بیمه','comp']]
            dfLifie.columns = ['تاریخ عملیات', 'مبلغ کل حق بیمه','comp']
            dfLifie = dfLifie.dropna(subset=['تاریخ عملیات'])
            dfLifie['تاریخ عملیات'] = [timedate.dateToInt(x) for x in dfLifie['تاریخ عملیات']]
            dfLifie = dfLifie[dfLifie['تاریخ عملیات']>OnPeroid]
        else:
            dfLifie = pd.DataFrame()

        assing = pd.DataFrame(pishkarDb['AssingIssuing'].find({'username':username,'cunsoltant':data['cunsoltant']},{'_id':0,'username':0}))
        if len(assing)>0:
            df = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0, 'ردیف':0,
                'شعبه واحد صدور بیمه نامه یا الحاقیه':0, 'واحدصدور بیمه نامه':0, 'نمایندگی بیمه نامه':0,
                'كد رايانه بيمه نامه':0, 'کد رایانه عملیات': 0, 'تاريخ سند دريافتي':0 ,'تاريخ واگذاري':0 ,
                'تاريخ وضعیت وصول':0, 'وضعيت وصول':0 , 'شماره سند دريافتي':0 ,'شرح دريافتي':0 , 'شماره حساب':0,
                'حساب واگذاري':0, 'بانك':0, 'شرح عملیات':0,'username':0, 'additional':0}))
            df = df.set_index(['comp','کد رایانه صدور بیمه نامه']).join(assing.set_index(['comp','کد رایانه صدور بیمه نامه']),how='left')
            df['cunsoltant'] = df['cunsoltant'].fillna(False)
            df = df[df['cunsoltant']!=False].reset_index().drop(columns='cunsoltant')
            df['تاریخ عملیات'] = [timedate.dateToInt(x) for x in df['تاریخ عملیات']]
            df = df.fillna('')
            df = df.drop_duplicates(subset=['شماره بيمه نامه','شماره الحاقیه','تاريخ بيمه نامه يا الحاقيه','مبلغ کل حق بیمه','comp','کد رایانه صدور بیمه نامه'])
            df = df[df['تاریخ عملیات']>OnPeroid]
        else:
            df = pd.DataFrame()
        df = pd.concat([df,dfLifie])
        if len(df)==0:
            return json.dumps({'replay':False,'msg':'بیمه نامه ای یافت نشد'})
        df = df.groupby(by=['comp']).sum(numeric_only=True)[['مبلغ کل حق بیمه']]
        df = df.sort_values(by=['مبلغ کل حق بیمه'],ascending=False).reset_index()
        df['rate'] = (df['مبلغ کل حق بیمه'] / df['مبلغ کل حق بیمه'].sum())
        dff = df[df.index<6]
        if dff['rate'].sum()<1:
            dff = pd.concat([dff,pd.DataFrame([{'comp':'دیگر','مبلغ کل حق بیمه':df['مبلغ کل حق بیمه'].sum() - dff['مبلغ کل حق بیمه'].sum(),'rate':1-dff['rate'].sum()}])])
        dff['color'] = 0
        color = [ColorRandom() for x in dff['color']]
        dff['color'] = color
        dff['مبلغ کل حق بیمه'] = [int(x/1000000) for x in dff['مبلغ کل حق بیمه']]
        dff['rate'] = [int(x*1000)/10 for x in dff['rate']]
        labels = dff['comp'].to_list()
        dataa = dff['مبلغ کل حق بیمه'].to_list()
        dff = dff.to_dict('records')
        return json.dumps({'replay':True,'df':{'labels':labels,'datasets':[{'id':0,'data':dataa}]},'color':color,'dff':dff})
    else:
        return ErrorCookie()
