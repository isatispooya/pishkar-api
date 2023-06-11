import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import numpy as np
import timedate
from customers import splitCode, splitName
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

        print(issuing.isnull().sum())


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
    