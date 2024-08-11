import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import numpy as np
import timedate
from customers import splitCode, splitName
import random
import datetime
from SystemMassage import splitCode
import hashlib
from cryptography.fernet import Fernet
import setting
import pyodbc

client = pymongo.MongoClient()

pishkarDb = client['pishkar']

def comparisom(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        fee = pd.DataFrame(pishkarDb['Fees'].find({'username':username,'UploadDate':data['datePeriod']['Show']},{'_id':0,'comp':1,'شرح':1,'رشته':1,'مورد بیمه':1,'کل کارمزد محاسبه شده':1,'کل مبلغ وصول شده':1,'بيمه گذار':1}))
        feeStandard = pd.DataFrame(pishkarDb['standardfee'].find({'username':username},{'_id':0}))
        if len(fee)==0:
            return json.dumps({'replay':False,'msg':'فایل کارمزد تا کنون بارگذاری نشده است'})
        if len(feeStandard)==0:
            return json.dumps({'replay':False,'msg':'هیچ دسته بندی یافت نشد'})
        fee = fee.fillna('')
        feeStandard = feeStandard.fillna(0)
        fee['Field'] =  fee['رشته'] + ' ('+fee['مورد بیمه']+')'
        fee['Field'] = [str(x).replace(' ()','') for x in fee['Field']]
        df = fee.set_index(['Field']).join(feeStandard.set_index(['field']),how='left').reset_index()
        df = df[df['groupMain']!='بیمه زندگی'].drop(columns=['رشته','مورد بیمه','years'])#'baseRateLive','firstFeeRateLive','secendFeeRateLive']),'baseRateLive','firstFeeRateLive','secendFeeRateLive'])
        df['Added'] = [("الحاقيه" not in x) or ("شماره الحاقيه 0" in x) for x in df['شرح']]
        df = df[df['Added']==True]
        df = df[df['کل مبلغ وصول شده']!='']
        df = df[df['کل کارمزد محاسبه شده']!='']
        df['کل کارمزد محاسبه شده'] = [int(x) for x in df['کل کارمزد محاسبه شده']]
        df['کل مبلغ وصول شده'] = [int(x) for x in df['کل مبلغ وصول شده']]
        df = df.dropna()
        df['rate'] = [int(x) for x in df['rate']]
        df['RealFeeRate'] = (df['کل کارمزد محاسبه شده'] / df['کل مبلغ وصول شده'])
        df = df.drop(columns=['شرح','date','username','dateshow','Added','کل مبلغ وصول شده','کل کارمزد محاسبه شده'])
        insurec = pd.DataFrame(pishkarDb['insurer'].find({'username':username},{'نام':1,'بیمه گر':1,'_id':0}))
        insurec = insurec.set_index('نام').to_dict(orient='dict')['بیمه گر']
        df['comp'] = [insurec[x] for x in df['comp']]
        childern = df.copy()
        childern['RealFeeRate'] = [int(x*10000)/100 for x in childern['RealFeeRate']]
        childern['OutLine'] = childern['RealFeeRate'] - childern['rate']
        childern['OutLine'] = [round(x,2) for x in childern['OutLine']]
        childern['Title'] = childern['index']
        if(data['OnBase']=='Group'):
            df = df.groupby(by=['comp','groupMain','groupSub']).mean()
            df['RealFeeRate'] = [int(x*10000)/100 for x in df['RealFeeRate']]
            df['OutLine'] = df['RealFeeRate'] - df['rate']
            df['OutLine'] = [round(x,2) for x in df['OutLine']]
            df['rate'] = [round(x,0) for x in df['rate']]
            df['Title'] = 'همه'
            df = df.reset_index()
            df = df.to_dict(orient='records')
            dff = []
            for i in df:
                child = childern[childern['comp']==i['comp']]
                child = child[child['groupMain']==i['groupMain']]
                child = child[child['groupSub']==i['groupSub']]
                child = child.to_dict('records')
                dict = i
                dict['بيمه گذار'] = ''
                dict['_children'] = child
                dff.append(dict)
            return json.dumps({'replay':True, 'df':dff})
        else:
            df = df.groupby(by=['comp','groupMain','groupSub','index']).mean()
            df['RealFeeRate'] = [int(x*10000)/100 for x in df['RealFeeRate']]
            df['OutLine'] = df['RealFeeRate'] - df['rate']
            df['OutLine'] = [round(x,2) for x in df['OutLine']]
            df['rate'] = [round(x,0) for x in df['rate']]
            df = df.reset_index()
            df['Title'] = df['index']
            df = df.to_dict(orient='records')
            dff = []
            for i in df:
                child = childern[childern['comp']==i['comp']]
                child = child[child['groupMain']==i['groupMain']]
                child = child[child['groupSub']==i['groupSub']]
                child = child[child['index']==i['index']]
                child = child.to_dict('records')
                dict = i
                dict['بيمه گذار'] = ''
                dict['_children'] = child
                dff.append(dict)
            return json.dumps({'replay':True, 'df':dff})

    else:
        return ErrorCookie()


def comparisomForFrocast(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        fee = pd.DataFrame(pishkarDb['Fees'].find({'username':username},{'_id':0,'comp':1,'شرح':1,'رشته':1,'مورد بیمه':1,'کل کارمزد محاسبه شده':1,'کل مبلغ وصول شده':1}))
        feeStandard = pd.DataFrame(pishkarDb['standardfee'].find({'username':username},{'_id':0}))
        if len(fee)==0:
            return json.dumps({'replay':False,'msg':'فایل کارمزد تا کنون بارگذاری نشده است'})
        if len(feeStandard)==0:
            return json.dumps({'replay':False,'msg':'هیچ دسته بندی یافت نشد'})
        fee = fee.fillna('')
        feeStandard = feeStandard.fillna(0)
        fee['Field'] =  fee['رشته'] + ' ('+fee['مورد بیمه']+')'
        fee['Field'] = [str(x).replace(' ()','') for x in fee['Field']]
        df = fee.set_index(['Field']).join(feeStandard.set_index(['field']),how='left').reset_index()
        df = df[df['groupMain']!='بیمه زندگی'].drop(columns=['رشته','مورد بیمه','years'])#'baseRateLive','firstFeeRateLive','secendFeeRateLive'])
        df['Added'] = [("الحاقيه" not in x) or ("شماره الحاقيه 0" in x) for x in df['شرح']]
        df = df[df['Added']==True]
        df = df[df['کل مبلغ وصول شده']!='']
        df = df[df['کل کارمزد محاسبه شده']!='']
        df['کل کارمزد محاسبه شده'] = [int(x) for x in df['کل کارمزد محاسبه شده']]
        df['کل مبلغ وصول شده'] = [int(x) for x in df['کل مبلغ وصول شده']]
        df['rate'] = df['rate'].fillna(0)
        df['rate'] = [int(x) for x in df['rate']]
        df['RealFeeRate'] = (df['کل کارمزد محاسبه شده'] / df['کل مبلغ وصول شده'])
        df = df.drop(columns=['شرح','date','username','dateshow','Added','کل مبلغ وصول شده','کل کارمزد محاسبه شده'])
        insurec = pd.DataFrame(pishkarDb['insurer'].find({'username':username},{'نام':1,'بیمه گر':1,'_id':0}))
        insurec = insurec.set_index('نام').to_dict(orient='dict')['بیمه گر']
        df['comp'] = [insurec[x] for x in df['comp']]
        if(data['OnBase']=='Group'):
            df = df.groupby(by=['comp','groupMain','groupSub']).mean()
            df['RealFeeRate'] = [int(x*10000)/100 for x in df['RealFeeRate']]
            df['OutLine'] = df['RealFeeRate'] - df['rate']
            df['OutLine'] = [round(x,2) for x in df['OutLine']]
            df['rate'] = [round(x,0) for x in df['rate']]
            df['Title'] = 'همه'
            df = df.reset_index()
            df = df.to_dict(orient='records')
            return json.dumps({'replay':True, 'df':df})
        else:
            df = df.groupby(by=['comp','groupMain','groupSub','index']).mean()
            df['RealFeeRate'] = [int(x*10000)/100 for x in df['RealFeeRate']]
            df['OutLine'] = df['RealFeeRate'] - df['rate']
            df['OutLine'] = [round(x,2) for x in df['OutLine']]
            df['rate'] = [round(x,0) for x in df['rate']]
            df = df.reset_index()
            df['Title'] = df['index']
            df = df.to_dict(orient='records')
            return json.dumps({'replay':True, 'df':df})
    else:
        return ErrorCookie()
    

def CustomerRatingsFee(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['Fees'].find({'username':username},{'_id':0}))
        df['periodInt'] = [timedate.PriodStrToInt(x) for x in df['UploadDate']]
        frm = timedate.DateToPeriodInt(timedate.timStumpTojalali(data['Date']['from']))
        to = timedate.DateToPeriodInt(timedate.timStumpTojalali(data['Date']['to']))
        df = df[df['periodInt']>=frm]
        df = df[df['periodInt']<=to]

        if data['consultant'] !='all':
            consultant = pd.DataFrame(pishkarDb['assing'].find({'username':username,'consultant':data['consultant']})).set_index(['شماره بيمه نامه'])
            consultant['c'] = True
            consultant = consultant[['c']]
            df = df.set_index(['شماره بيمه نامه'])
            df = df.join(consultant,how='left')
            df = df[df['c']==True]
            df = df.reset_index().drop(columns=['c'])
        if len(df)==0:
            return json.dumps({'replay':False,'msg':'هیچ مشتری یافت نشد'})
        df = df.drop_duplicates(subset=['شماره بيمه نامه','UploadDate','comp','كارمزد قابل پرداخت','کد رایانه صدور'])
        standard = pd.DataFrame(pishkarDb['standardfee'].find({'username':username},{'_id':0,'field':1,'groupMain':1}))
        standard = standard.set_index('field')
        standard = standard.fillna(0)
        df = df.fillna('')
        df['Field'] =  df['رشته'] + ' ('+df['مورد بیمه']+')'
        df['Field'] = [str(x).replace(' ()','') for x in df['Field']]
        df = df.set_index('Field').join(standard).reset_index()

        df['count'] = 1
        df = df.fillna(0)
        insurec = pd.DataFrame(pishkarDb['insurer'].find({'username':username},{'نام':1,'بیمه گر':1,'_id':0}))
        insurec = insurec.set_index('نام').to_dict(orient='dict')['بیمه گر']
        df['comp'] = [insurec[x] for x in df['comp']]
        dfcustomers = pd.DataFrame(pishkarDb['customers'].find({'username':username},{'_id':0,'بيمه گذار':1,'کد ملي بيمه گذار':1,'name':1}))
        dfcustomers = dfcustomers.drop_duplicates().set_index('بيمه گذار')
        df = df.set_index('بيمه گذار')
        df = df.join(dfcustomers).reset_index()
        df['کد ملي بيمه گذار'] = df['کد ملي بيمه گذار'].fillna(df['بيمه گذار'])
        df['name'] = df['name'].fillna(df['بيمه گذار'])
        child = df[['کد ملي بيمه گذار','name','comp','كارمزد قابل پرداخت','groupMain','count']]
        df = df.groupby(by=['کد ملي بيمه گذار','name']).sum(numeric_only=True)[['كارمزد قابل پرداخت','count']]
        df = df.reset_index()
        df['_children'] = ''
        df = df.to_dict('records')
        for i in range(len(df)):
            df[i]['_children'] = child[child['کد ملي بيمه گذار']==df[i]['کد ملي بيمه گذار']].to_dict('records')
        return json.dumps({'replay':True, 'df':df})
    else:
        return ErrorCookie()
    

def CustomerRatingsIssuing(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        issuing = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0}))
        issuingLife = pd.DataFrame(pishkarDb['issuingLife'].find({'username':username},{'_id':0,'comp':1,'نام بیمه گذار':1,'تاريخ شروع':1,'شماره بيمه نامه':1,'حق بیمه هر قسط \n(جمع عمر و پوششها)':1,'تعداد اقساط در سال':1}))
        issuing['dateInt'] = [timedate.dateToInt(x) for x in issuing['تاریخ عملیات']]
        issuingLife['dateInt'] = [timedate.dateToInt(x) for x in issuingLife['تاريخ شروع']]
        frm = timedate.dateToInt(timedate.timStumpTojalali(data['Date']['from']))
        to = timedate.dateToInt(timedate.timStumpTojalali(data['Date']['to']))
        issuing = issuing[issuing['dateInt']>=frm]
        issuing = issuing[issuing['dateInt']<=to]
        issuingLife = issuingLife[issuingLife['dateInt']>=frm]
        issuingLife = issuingLife[issuingLife['dateInt']<=to]
        if len(issuing)==0 and len(issuingLife)==0:
            return json.dumps({'replay':False,'msg':'در این بازه زمانی موردی یافت نشد'})
        if data['consultant'] != 'all':
            AssingIssuing = pd.DataFrame(pishkarDb['AssingIssuing'].find({'username':username},{'_id':0,'username':0}))
            AssingIssuing = AssingIssuing.set_index(['کد رایانه صدور بیمه نامه','comp'])

            issuing = issuing.set_index(['کد رایانه صدور بیمه نامه','comp'])
            issuing = issuing.join(AssingIssuing)
            issuing = issuing[issuing['cunsoltant'] == data['consultant']]
            issuing = issuing.reset_index()

            AssingIssuingLife = pd.DataFrame(pishkarDb['AssingIssuingLife'].find({'username':username},{'_id':0,'username':0}))
            AssingIssuingLife = AssingIssuingLife.set_index(['شماره بيمه نامه','consultant'])

            issuingLife = issuingLife.set_index('شماره بيمه نامه')
            issuingLife = issuingLife.join(AssingIssuingLife)
            issuingLife = issuingLife[issuingLife['cunsoltant'] == data['consultant']]
            issuingLife = issuingLife.reset_index()
        standard = pd.DataFrame(pishkarDb['standardfee'].find({'username':username},{'_id':0,'field':1,'groupMain':1}))
        standard = standard.set_index('field')
        standard = standard.fillna(0)
        issuing = issuing[['رشته','کد رایانه صدور بیمه نامه','مورد بیمه','additional','comp','پرداخت کننده حق بیمه','شماره الحاقیه','مبلغ کل حق بیمه','تاريخ بيمه نامه يا الحاقيه']]
        issuing = issuing.drop_duplicates(subset=['رشته','کد رایانه صدور بیمه نامه','مورد بیمه','additional','additional','مبلغ کل حق بیمه'])
        issuing = issuing.fillna('')
        issuing['Field'] =  issuing['رشته'] + ' ('+issuing['مورد بیمه']+')'
        issuing['Field'] = [str(x).replace(' ()','') for x in issuing['Field']]
        issuing = issuing.set_index('Field').join(standard).reset_index()
        issuingLife = issuingLife.drop_duplicates(subset=['شماره بيمه نامه'])
        issuingLife['Field'] = 'زندگی'
        issuingLife['groupMain'] = 'زندگی'
        issuingLife['تعداد اقساط در سال'] = issuingLife['تعداد اقساط در سال'].astype(int)
        issuingLife['حق بیمه هر قسط \n(جمع عمر و پوششها)'] = issuingLife['حق بیمه هر قسط \n(جمع عمر و پوششها)'].astype(int)
        issuingLife['مبلغ کل حق بیمه'] = issuingLife['تعداد اقساط در سال'] * issuingLife['حق بیمه هر قسط \n(جمع عمر و پوششها)']
        issuingLife = issuingLife.rename(columns={'تاريخ شروع':'تاريخ بيمه نامه يا الحاقيه','نام بیمه گذار':'پرداخت کننده حق بیمه'})
        issuingLife = issuingLife.drop(columns=['تعداد اقساط در سال','حق بیمه هر قسط \n(جمع عمر و پوششها)','dateInt'])
        issuing = pd.concat([issuing,issuingLife])
        issuing = issuing.fillna('-')
        issuing['count'] = 1
        child = issuing[['پرداخت کننده حق بیمه','comp','groupMain','رشته','مبلغ کل حق بیمه','count']]
        child['پرداخت کننده حق بیمه'] = [splitCode(x) for x in child['پرداخت کننده حق بیمه']]
        issuing = issuing.groupby(by=['پرداخت کننده حق بیمه']).sum(numeric_only=True)
        issuing['name'] = [splitName(x) for x in issuing.index]
        issuing['code'] = [splitCode(x) for x in issuing.index]
        issuing = issuing.set_index('code')
        customer = pd.DataFrame(pishkarDb['customers'].find({'username':username},{'_id':0,'کد ملي بيمه گذار':1,'code':1,'name':1})).set_index('code')
        issuing = issuing.join(customer,rsuffix='_c').reset_index()
        issuing = issuing.groupby(['name','code']).sum(numeric_only=True).reset_index()
        issuing = issuing.to_dict('records')
        for i in range(len(issuing)):
            issuing[i]['_children'] = child[child['پرداخت کننده حق بیمه']==issuing[i]['code']].to_dict('records')
        return json.dumps({'replay':True, 'df':issuing})
    else:
        return ErrorCookie()
    

def profit(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        cost = {'_id':0,'period':True,'fullName':True,'reward':True}
        for key, value in data['cost'].items():
            keyNew = str(key).replace('بن خواربار','subsidy').replace('مسکن','homing').replace('عیدی','eydi').replace('حق اولاد','childern').replace('سایر مزایایی غیر نقدی','benefit').replace('مالیات','taxe').replace('حق بیمه پرسنل','insuranceWorker').replace('حق بیمه کارفرما','insuranceEmployer').replace('کارمزد اصلی','reward')
            cost[keyNew] = value
        df_pay = pd.DataFrame(pishkarDb['paymentPerson'].find({'username':username}))
        for c in df_pay.columns:
            if c in cost.keys():
                if cost[c] == False:
                    df_pay = df_pay.drop(columns=c)
            else:
                df_pay = df_pay.drop(columns=c)

        for key, value in data['person'].items():
            if value == False:
                df_pay = df_pay[df_pay['fullName']!=key]
        df_inc = pd.DataFrame(pishkarDb['Fees'].find({'username':username},{'_id':0,'رشته':1,
            'مورد بیمه':1, 'کد رایانه صدور':1, 'كارمزد قابل پرداخت':1, 'کل مبلغ وصول شده': 1,
            'UploadDate':1 ,'comp':1 , 'تاریخ صدور بیمه نامه':1,'شماره بيمه نامه':1}))
        df_inc = df_inc.drop_duplicates(subset=['comp','UploadDate','کد رایانه صدور','شماره بيمه نامه','كارمزد قابل پرداخت'])
        df_inc = df_inc[['رشته','كارمزد قابل پرداخت','UploadDate','comp']]
        df_inc = df_inc.rename(columns={'UploadDate':'period'})
        for key, value in data['comp'].items():
            if value == False:
                df_inc = df_inc[df_inc['comp']!=key]
        df_pay = df_pay.groupby(by=['period']).sum(numeric_only=True)
        df_inc = df_inc.groupby(by=['period']).sum(numeric_only=True)
        df_pay['all'] = df_pay.sum(axis=1)
        df_pay = df_pay[['all','reward']]
        df_sum = df_inc.join(df_pay)
        df_sum = df_sum.reset_index()
        df_sum['period'] = df_sum['period'].apply(timedate.PriodStrToInt)
        df_sum = df_sum.sort_values(by=['period'])
        df_sum['rateAll'] = df_sum['all'] / df_sum['كارمزد قابل پرداخت']
        df_sum['rateReward'] = df_sum['reward'] / df_sum['كارمزد قابل پرداخت']
        df_sum['rateAll'] = df_sum['rateAll'].fillna(method='backfill').fillna(method='ffill') * df_sum['كارمزد قابل پرداخت']
        df_sum['rateReward'] = df_sum['rateReward'].fillna(method='backfill').fillna(method='ffill') * df_sum['كارمزد قابل پرداخت']
        df_sum['all'] = df_sum['all'].fillna(df_sum['rateAll'])
        df_sum['reward'] = df_sum['reward'].fillna(df_sum['rateReward'])
        df_sum['all'] = df_sum['all'].astype(int)
        df_sum['reward'] = df_sum['reward'].astype(int)
        df_sum = df_sum.drop(columns=['rateAll','rateReward'])
        df_sum = df_sum.fillna(0)
        df_sum = df_sum.reset_index().drop(columns='index')
        df_sum = df_sum[df_sum.index>df_sum.index.max()-int(data['period'])]
        df_sum['profit'] = df_sum['كارمزد قابل پرداخت'] - df_sum['all']
        df_sum['profitNoReward'] = df_sum['كارمزد قابل پرداخت'] - df_sum['all'] + df_sum['reward']
        df_sum['CostFix'] =df_sum['all'] - df_sum['reward']

        df_rate = df_sum.copy()
        df_rate['CostToIncom'] = df_rate['all'] / df_rate['كارمزد قابل پرداخت']
        df_rate['CostNoFeeToIncom'] = (df_rate['all']-df_rate['reward']) / df_rate['كارمزد قابل پرداخت']
        df_rate['FeeToIncom'] = df_rate['reward'] / df_rate['كارمزد قابل پرداخت']
        df_rate['FeeToCost'] = df_rate['reward'] / df_rate['all']
        df_rate['ChangRateProfit'] = ((df_rate['كارمزد قابل پرداخت'] / df_rate['كارمزد قابل پرداخت'].shift(1))-1).fillna(0)
        df_rate['ChangRateCost'] = ((df_rate['all'] / df_rate['all'].shift(1))-1).fillna(0)
        df_rate['ChangRateFee'] = ((df_rate['reward'] / df_rate['reward'].shift(1))-1).fillna(0)
        df_rate['ChangRateIncom'] = ((df_rate['كارمزد قابل پرداخت'] / df_rate['كارمزد قابل پرداخت'].shift(1))-1).fillna(0)
        df_rateCulomns = ['period','CostToIncom','CostNoFeeToIncom','FeeToIncom','FeeToCost','ChangRateProfit','ChangRateCost','ChangRateFee','ChangRateIncom']
        df_rate = df_rate[df_rateCulomns]
        for c in df_rateCulomns:
            if c != 'period':
                df_rate[c] = df_rate[c]*10000
                df_rate[c] = df_rate[c].astype(int)/100
        datasets = []
        for c in df_sum.columns:
            if c != 'period':
                name = str(c).replace('كارمزد قابل پرداخت','درامد').replace('all','هزینه').replace('reward','متغییر').replace('profitNoReward','سود بدون متغییر ').replace('profit','سود').replace('CostFix','هزینه ثابت')
                Color = 'rgb('+ str(random.randint(90,255)) + ',' + str(random.randint(90,255))+ ',' + str(random.randint(90,255)) +')'
                dic = {'label':name,'data':df_sum[c].to_list(),'borderColor': Color,'backgroundColor': Color}
                datasets.append(dic)
        chart = {'labels':df_sum['period'].to_list(),'datasets':datasets}
        df_sum = df_sum.to_dict('records')
        df_rate = df_rate.to_dict('records')
        return json.dumps({'replay':True, 'df':df_sum,'dfRate':df_rate,'chart':chart})
    else:
        return ErrorCookie()
    

def optionprofit(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        insurec = pd.DataFrame(pishkarDb['insurer'].find({'username':username},{'نام':1,'بیمه گر':1,'_id':0})).set_index('نام').to_dict(orient='dict')['بیمه گر']
        insurec['کارآفرین'] = 'کارآفرین'
        person = pishkarDb['paymentPerson'].find({'username':username},{'fullName':1})
        person = list(set([x['fullName'] for x in person]))
        person ={x:True for x in person}
        comp = pishkarDb['Fees'].aggregate([{"$group": {"_id": "$comp"}},{"$match": {"_id": {"$ne": None}}},{"$project": {"_id": 0,"comp": "$_id"}}])
        comp = [x['comp'] for x in comp]
        comp = [insurec[x] for x in comp]
        comp = {x:True for x in comp}
        cost = ['بن خواربار','مسکن','عیدی','حق اولاد','سایر مزایایی غیر نقدی','مالیات','حق بیمه پرسنل','حق بیمه کارفرما','کارمزد اصلی']
        cost = {x:True for x in cost}
        return json.dumps({'replay':True, 'data':{'comp':comp,'person':person,'cost':cost}})
    else:
        return ErrorCookie()

def lifestatus(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['RevivalLife'].find({'username':username,'شماره بيمه نامه':data['num']},{'_id':0}))
        df = df.to_dict('records')
        return json.dumps({'replay':True, 'df':df})
    else:
        return ErrorCookie()
    


def report_received(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        frm = timedate.timStumpTojalali(data['date']['from'])
        to = timedate.timStumpTojalali(data['date']['to'])
        frmInt = int(frm.replace('/',''))
        toInt = int(to.replace('/',''))


        fee = pd.DataFrame(pishkarDb['Fees'].find({'username':username},{'_id':0,'UploadDate':1,'تا تاریخ محاسبه':1,'رشته':1,'مورد بیمه':1,'بيمه گذار':1,'شماره بيمه نامه':1,'تاریخ صدور بیمه نامه':1,'كارمزد پرداخت شده در محاسبه هاي قبل':1,'کل کارمزد محاسبه شده':1,'مبلغ كل':1,'comp':1}))
        fee['date'] = fee['تاریخ صدور بیمه نامه'].apply(timedate.dateSlashToInt)
        fee = fee[fee['date']<=toInt]
        fee = fee[fee['date']>=frmInt]
        fee = fee.drop(columns=['تاریخ صدور بیمه نامه'])
        insurec = pd.DataFrame(pishkarDb['insurer'].find({'username':username},{'نام':1,'بیمه گر':1,'_id':0}))
        insurec = insurec.set_index('نام').to_dict(orient='dict')['بیمه گر']
        fee['comp'] = [insurec[x] for x in fee['comp']]
        fee['UploadDate'] = fee['UploadDate'].apply(timedate.PriodStrToInt)
        fee['UploadDate'] = [int(str(x)+'00')<=toInt for x in fee['UploadDate']]
        fee = fee.sort_values(['تا تاریخ محاسبه','UploadDate'])
        fee = fee.drop_duplicates(keep='first',subset='شماره بيمه نامه')
        fee = fee.drop(columns=['تا تاریخ محاسبه'])
        fee['مورد بیمه'] = fee['مورد بیمه'].fillna('')
        fee = fee.fillna(0)
        fee['مبلغ كل'] = fee['مبلغ كل'] * fee['UploadDate']
        fee['received'] = fee['كارمزد پرداخت شده در محاسبه هاي قبل'] + fee['مبلغ كل']
        fee['received'] = fee['received'].apply(int)
        fee['balance'] = fee['کل کارمزد محاسبه شده'] - fee['received']
        fee = fee.drop(columns=['كارمزد پرداخت شده در محاسبه هاي قبل','مبلغ كل'])
        fee = fee.to_dict('records')
        return{'reply':True, 'df':fee}
    else:
        return ErrorCookie()
    

def report_feeperfild(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        paymentPerson = pd.DataFrame(
            pishkarDb['paymentPerson'].find(
                {
                    'period':data['datePeriod']['Show'],
                    'username':username
                },
                {
                    '_id':0,
                    'nationalCode':1,
                    'fullName':1,
                    'reward':1,
                    'SubReward':1,
                    
                }
            )
        )
        if len(paymentPerson) == 0:
            return json.dumps({'reply':False, 'msg':'حقوق و دستمز برای دوره انتخابی محاسبه نشده است'})
            
        paymentPerson['all'] = paymentPerson['reward'] + paymentPerson['SubReward']
        paymentPerson = paymentPerson[paymentPerson['all']>0]
        NumPerson = list(set(paymentPerson['nationalCode'].to_list()))
        paymentPerson = paymentPerson.set_index('nationalCode')
        
        
        
        Fees = pd.DataFrame(
            pishkarDb['Fees'].find(
                {
                    'username':username,
                    'UploadDate':data['datePeriod']['Show']
                },
                {
                    '_id':0,
                    'مبلغ كل':1,
                    'رشته':1,
                    'مورد بیمه':1,
                    'شماره بيمه نامه':1
                }
                
            )
        )
        
        NumFee = list(set(Fees['شماره بيمه نامه'].to_list()))
        Fees = Fees.set_index('شماره بيمه نامه')
        
        assing = pd.DataFrame(
            pishkarDb['assing'].find()
        )
        
        assing = assing[assing['شماره بيمه نامه'].isin(NumFee)]
        assing = assing[assing['consultant'].isin(NumPerson)]
        df = assing[['شماره بيمه نامه','consultant']].set_index('شماره بيمه نامه')
        df = df.join(Fees).reset_index()
        df['مورد بیمه'] = df['مورد بیمه'].fillna('')
        df['Field'] =  df['رشته'] + ' ('+df['مورد بیمه']+')'
        df['Field'] = [str(x).replace(' ()','') for x in df['Field']]
        df['rate'] = 0
        
        cunsoltant = pd.DataFrame(
            pishkarDb['cunsoltant'].find(
                {
                    'username':username,
                },
                {
                    '_id':0,
                    'username':0,
                    'gender':0,
                    'phone':0,
                    'code':0,
                    'salary':0,
                    'childern':0,
                    'freetaxe':0,
                    'freetaxe':0,
                    'insureWorker':0,
                    'insureEmployer':0,
                    'salaryGroup':0,
                    'ConsultantSelected':0
                    
                    
                }
            )
        )
        cunsoltant = cunsoltant[cunsoltant['nationalCode'].isin(NumPerson)]
        
        dff = []
        for i in NumPerson:
            cunsoltant_i = cunsoltant[cunsoltant['nationalCode']==i]
            if len(cunsoltant_i)>0:
                cunsoltant_i = cunsoltant_i.to_dict('records')[0]
                df_i = df[df['consultant']==i]
                if len(df_i)>0:
                    df_i['rate'] = [int(cunsoltant_i[x])/100 for x in df_i['Field'] if x in cunsoltant_i.keys()]
                    df_i['comission'] = df_i['rate'].fillna(0) * df_i['مبلغ كل'].fillna(0)
                    dff.append(df_i)
        
        df = pd.concat(dff)
        df = df[df['comission']>0]
        
        standardfee = pd.DataFrame(
            pishkarDb['standardfee'].find(
                {
                    'username':username,
                },
                {
                    '_id':0,
                    'field':1,
                    'groupMain':1
                }
            )
        )
        standardfee = standardfee.rename(columns={'field':'Field'}).set_index('Field')
        df = df.set_index('Field')
        df = df.join(standardfee).reset_index()
        df = df[['consultant','comission','groupMain']]
        df = df.groupby(by=['consultant','groupMain']).sum().reset_index()
        dfSum = df.groupby(by='consultant').sum(numeric_only=True).reset_index()
        dfSum['groupMain'] = 'sum'
        df = pd.concat([df,dfSum])
        
        df = pd.pivot(df, values='comission',index='consultant',columns='groupMain').reset_index()
        df = df.fillna(0)
        df['other'] = df['sum']
        colum = ['consultant','other','sum','بیمه حوادث شخصی و درمان','بیمه زندگی','بیمه مسئولیت','بیمه وسائط نقلیه موتوری']
        
        for i in df:
            if i not in colum:
                df = df.drop(columns=i)

        for i in df:
            if i not in ['consultant']:
                df[i] = df[i].apply(int)
            if i not in ['consultant','other','sum']:
                df['other'] = df['other'] - df[i]
                
        cunsoltant = cunsoltant[['fristName','lastName','nationalCode']].set_index('nationalCode')
        df = df.set_index('consultant').join(cunsoltant).reset_index()
        df = df.to_dict('records')

        return json.dumps({'reply':True, 'df':df})
    else:
        return ErrorCookie()
    

def groupingDocAccounting(df):

    dff= df[['بدهی باقی مانده','تاریخ سررسید']]
    dff = dff.rename(columns={"بدهی باقی مانده":'amount','تاریخ سررسید':'dadeline'})
    dff = dff.set_index('dadeline')
    dff = dff.to_dict()['amount']
    df = df[df.index==df.index.max()]
    df['latin'] = [dff for x in df.index]


    df['additional'] = df['additional'].apply(str)
    df['شماره بيمه نامه'] = df['شماره بيمه نامه'].apply(str)
    df['پرداخت کننده حق بیمه'] = df['پرداخت کننده حق بیمه'].apply(str)
    df['کد ملي بيمه گذار'] = df['کد ملي بيمه گذار'].apply(str)
    df = df.fillna('')

    df['discription'] = 'بابت بیمه نامه آقای ' + df['پرداخت کننده حق بیمه'] + ' کدملی ' + df['کد ملي بيمه گذار'] + ' رشته ' + df['رشته'] +' '+ df['مورد بیمه'] + ' به شماره ' + df['شماره بيمه نامه'] + ' الحاقیه ' + df['additional'] + ' بیمه گر ' + df['comp']
    df = df.rename(columns={'مبلغ کل حق بیمه':'bede'})
    df['code'] = '99999'
    df = df[['code','discription','latin','bede']]
    df = pd.concat([df,df]).reset_index().drop(columns='index')
    df['bestan'] = df['bede']
    df['bestan'][df.index.min()] = 0
    df['bede'][df.index.max()] = 0
    return df


# رمزنگاری
def dict_to_json_string(dictionary):
    return json.dumps(dictionary, sort_keys=True)

def encrypt(dictionary):
    with open('secret.key', 'rb') as key_file:
        key = key_file.read()
    cipher = Fernet(key)
    return cipher.encrypt(dict_to_json_string(dictionary).encode()).decode()

#رمزگشایی
def json_string_to_dict(json_string):
    return json.loads(json_string)

def decrypt(encrypted_string):
    try:
        with open('secret.key', 'rb') as key_file:
            key = key_file.read()
            
        cipher = Fernet(key)
        return json_string_to_dict(cipher.decrypt(encrypted_string.encode()).decode())
    except:
        return {}


def report_docaccounting(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        dt = timedate.timStumpTojalali(data['date'])
        df = pd.DataFrame(pishkarDb['issuing'].find({'username':username,'تاریخ عملیات':dt},{'_id':0,'کد رایانه صدور بیمه نامه':1,'رشته':1,'مورد بیمه':1,'پرداخت کننده حق بیمه':1,'شماره بيمه نامه':1,'تاريخ بيمه نامه يا الحاقيه':1,'تاریخ سررسید':1,'مبلغ کل حق بیمه':1,'بدهی باقی مانده':1,'comp':1,'additional':1}))
        if len(df) == 0:
            return json.dumps({'reply':False, 'msg':'در این تاریخ هیچ بیمه نام ای صادر نشده است'})
            
        df['code'] = df['پرداخت کننده حق بیمه'].apply(splitCode)

        df = df.set_index(['code','comp'])
        df_nc = pd.DataFrame(pishkarDb['customers'].find({},{'_id':0,'code':1,'کد ملي بيمه گذار':1,'comp':1})).set_index(['code','comp'])
        df = df.join(df_nc,how='left').reset_index()
        null_nc = df['کد ملي بيمه گذار'].isnull().sum()
        if null_nc>0:
            df_null = df.fillna(0)
            df_null = df_null.drop_duplicates(subset='پرداخت کننده حق بیمه')
            df_null = df_null[df_null['کد ملي بيمه گذار']==0][['پرداخت کننده حق بیمه','comp']].to_dict('records')
            txt = 'مشخصات زیر در قسمت مشتریان یافت نشد:\n'
            for i in df_null:
                txt = txt + i['پرداخت کننده حق بیمه'] + ' در شرکت '+i['comp']+'\n'
            return json.dumps({'reply':False, 'msg':txt})
        df['کد رایانه صدور بیمه نامه'] = df['کد رایانه صدور بیمه نامه'].apply(str)
        df['gb'] = df['code']+df['additional']+df['comp']+df['کد رایانه صدور بیمه نامه']
        
        df = df.groupby(by=['gb']).apply(groupingDocAccounting)
        df = df.reset_index()
        df['latin'] = df['latin'].apply(encrypt)
        df = df[['code','discription','latin','bede','bestan']]
        

        df = df.to_dict('records')
        return json.dumps({'reply':True, 'df':df})
    else:
        return ErrorCookie()
    

def CulcBalanceAdjust(dic):
    if len(dic)>0:
        adjust = 0
        for key in dic:
            date = timedate.dateSlashToInt(key)
            toDay = timedate.dateSlashToInt(str(timedate.toDay()).replace('-','/'))
            if date>=toDay:
                adjust = adjust + int(dic[key])
        return adjust
    return 0


def group110(code,code2):
    code = str(code).split('-')
    
    try:
        part1 = code[0]
        part2 = code[1]

        return (part1 == '110') and (part2 == code2)
    except:
        return False

def negetiveToZiro(neg):
    if neg>0:
        return neg
    else:
        return 0
    
def sumGroup(df):
    Bede = df['Bede'].sum()
    Best = df['Best'].sum()
    LtnComm = df['LtnComm'].sum()
    _children = []
    for x in df['_children'].to_list():
        if len(x)>0:
            dfff = pd.DataFrame([{'date': date, 'LtnComm': value} for date, value in x.items()])
            _children.append(dfff)
    if len(_children)>0:
        _children = pd.concat(_children)
        _children = _children[_children['LtnComm']>0]
        _children =_children.groupby('date').sum().reset_index()
        _children = _children.to_dict('records')
    dff = pd.DataFrame([{'Bede':Bede,'Best':Best,'LtnComm':LtnComm,'_children':_children}])
    return dff

def removePassed(passed):
    dic = {}
    if len(passed)>0:
        for key in passed:
            date = timedate.dateSlashToInt(key)
            toDay = timedate.dateSlashToInt(str(timedate.toDay()).replace('-','/'))
            if date>=toDay:
                dic.update({key:passed[key]})
        return dic
    return dic

def coustomer_balance(data):
    code = data['code']
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        conn_str_db = f'DRIVER={{SQL Server}};SERVER={setting.ip_sql_server},{setting.port_sql_server};DATABASE={setting.database};UID={setting.username_sql_server};PWD={setting.password_sql_server}'
        conn = pyodbc.connect(conn_str_db)
        query = f"SELECT * FROM DOCB"
        df = pd.read_sql(query, conn)
        df['110'] = [group110(x,code) for x in df['Acc_Code']]
        df = df[df['110']==True]        
        df = df[['Acc_Code','Bede','Best','LtnComm']]
        df = df.fillna('')
        df['_children'] = df['LtnComm'].apply(decrypt)
        df['_children'] = df['_children'].apply(removePassed)
        df['LtnComm'] = df['_children'].apply(CulcBalanceAdjust)
        df = df.groupby('Acc_Code').apply(sumGroup)
        df['balance_bede'] = df['Bede'] - df['Best']
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
        df = df.to_dict('records')
        return json.dumps({'reply':True, 'df':df})
    else:
        return ErrorCookie()

def coustomer_balance_api(data):
    code = data['code']
    key = data['key']
    if key=='farasahm':
        conn_str_db = f'DRIVER={{SQL Server}};SERVER={setting.ip_sql_server},{setting.port_sql_server};DATABASE={setting.database};UID={setting.username_sql_server};PWD={setting.password_sql_server}'
        conn = pyodbc.connect(conn_str_db)
        query = f"SELECT * FROM DOCB"
        df = pd.read_sql(query, conn)
        df['110'] = [group110(x,code) for x in df['Acc_Code']]
        df = df[df['110']==True]        
        df = df[['Acc_Code','Bede','Best','LtnComm']]
        df = df.fillna('')
        df['_children'] = df['LtnComm'].apply(decrypt)
        df['_children'] = df['_children'].apply(removePassed)
        df['LtnComm'] = df['_children'].apply(CulcBalanceAdjust)
        df = df.groupby('Acc_Code').apply(sumGroup)
        df['balance_bede'] = df['Bede'] - df['Best']
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
        df = df.to_dict('records')
        return json.dumps({'reply':True, 'df':df})
    else:
        return ErrorCookie()
    


# for Farasahm SMS Panel
def issuing_api (data):
    key = data['key']
    if key !='farasahm':
        return json.dumps({'reply' : False})
    issuing =pishkarDb['issuing'].find({},{'_id':0})
    df_issuing = pd.DataFrame(issuing)
    df_issuing = df_issuing.drop (
        columns=[
            'ردیف',
            'شعبه واحد صدور بیمه نامه یا الحاقیه',
            'كد رايانه بيمه نامه',
            'کد رایانه صدور بیمه نامه',
            'شماره بيمه نامه',
            'کد رایانه عملیات',
            'شماره الحاقیه',
            'شماره سند دريافتي',
            'شرح دريافتي',
            'شماره حساب',
            'بانك',
            'واحدصدور بیمه نامه',
            'حساب واگذاري',
            'نمایندگی بیمه نامه',
            'تاريخ واگذاري',
            'تاريخ سند دريافتي',
            'تاريخ وضعیت وصول',
            'وضعيت وصول',
            'شرح عملیات',
            'username',
            'longTime',
            'expier',
            'بدهی باقی مانده',
            'مدت زمان',
            ])
    customers =pishkarDb['customers'].find({},{'_id':0 , 'بيمه گذار' :1 , 'تلفن همراه' :1 , 'کد ملي بيمه گذار':1})
    df_customers = pd.DataFrame(customers)
    df = pd.merge(df_issuing, df_customers, left_on='پرداخت کننده حق بیمه', right_on='بيمه گذار', how='inner')
    df['بیمه گذار'] = df['پرداخت کننده حق بیمه']   
    df = df.drop(columns=['پرداخت کننده حق بیمه', 'بيمه گذار'])
    df = df.rename(columns={'تلفن همراه' : 'شماره تماس' , 'کد ملي بيمه گذار':'کد ملی' , 'بیمه گذار':'نام و نام خانوادگی' })
    df = df.to_dict('records')

    return json.dumps({'dict_df' : df})
    
    