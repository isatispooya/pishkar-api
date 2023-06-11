import json
from pickle import TRUE
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import datetime
import timedate
import numpy as np
client = pymongo.MongoClient()
pishkarDb = client['pishkar']
import time

def feeRate(feild,consultant):
    try: r = int(consultant[feild])/100
    except: r = 0
    return r

def get(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        consultantList = list(pishkarDb['cunsoltant'].find({'username':username},{'_id':0}))
        minimum = pd.DataFrame(pishkarDb['minimumSalary'].find({'username':username},{'_id':0,'username':0}))
        benefit = pd.DataFrame(pishkarDb['benefit'].find({'username':username, 'date':data['period']['Show']}))
        if len(benefit)>0:
            benefit = benefit[['benefit','nationalCode']].set_index('nationalCode')
        if len(minimum)==0: return json.dumps({'replay':False,'msg':'حداقل دستمزد روزانه ثبت نشده'})
        df = pd.DataFrame()
        for c in consultantList:
            try:active = c['active']
            except:active = True
            if active:
                fixPay = {}
                fullName = c['gender'] +' '+ c['fristName'] +' '+ c['lastName']
                if c['salary']:
                    act = pishkarDb['act'].find_one({'username':username,'nationalCode':c['nationalCode'],'period':data['period']['Show']},{'_id':0})
                    yearBasePay = str(data['period']['Show']).split('-')[0].replace(' ','')
                    minimumDaily = int(list(minimum[minimum['year']==yearBasePay]['value'])[0])
                    MinMaxEydi = [minimumDaily*(60/365),minimumDaily*(90/365)]
                    salaryGruop = c['salaryGroup']
                    salary = pishkarDb['salary'].find_one({'username':username,'year':yearBasePay,'gruop':salaryGruop})
                    if salary == None: return json.dumps({'replay':False,'msg':f'گروه حقوق {salaryGruop}یافت نشد'})
                    taxDf = pishkarDb['taxe'].find_one({'username':username,'year':yearBasePay})
                    if act == None: return json.dumps({'replay':False,'msg':f'کارکرد {fullName} ثبت نشده است'})
                    if taxDf == None: return json.dumps({'replay':False,'msg':'سطوح معافیت های مالیاتی برای سال انتخابی ثبت نشده است'})
                    fixPay['base'] = int(act['act']) * (int(salary['daily']) + int(salary['sanavat']))
                    fixPay['act'] = int(act['act'])
                    fixPay['baseDaily'] = int(salary['daily'])
                    fixPay['sanavatDaily'] = int(salary['sanavat'])
                    fixPay['subsidy'] = round((int(act['act'])/30) * int(salary['subsidy']))
                    fixPay['homing'] = round((int(act['act'])/30) * int(salary['homing']))
                    fixPay['childern'] = round(((int(act['act'])/30) * int(salary['childern'])) * int(c['childern']))
                    fixPay['eydi'] = round(((fixPay['baseDaily']+fixPay['sanavatDaily'])*(60/365))*int(act['act']))
                    if fixPay['eydi']<(MinMaxEydi[0]*int(act['act'])):fixPay['eydi'] = MinMaxEydi[0]*int(act['act'])
                    if fixPay['eydi']>(MinMaxEydi[1]*int(act['act'])):fixPay['eydi'] = MinMaxEydi[1]*int(act['act'])
                    fixPay['eydi'] = round(fixPay['eydi'])
                    fixPay['insuranceWorker'] = round((fixPay['base']+fixPay['subsidy']+fixPay['homing'])*(int(c['insureWorker'])/100))
                    fixPay['insuranceEmployer'] = round((fixPay['base']+fixPay['subsidy']+fixPay['homing'])*(int(c['insureEmployer'])/100))
                    fixPay['paybeforTax'] = fixPay['base'] + fixPay['subsidy'] + fixPay['homing'] + fixPay['childern'] + fixPay['eydi']
                    inTax = fixPay['paybeforTax'] - fixPay['homing'] - fixPay['insuranceWorker']
                    fixPay['inTax'] = inTax
                    taxDf = pd.DataFrame.from_dict({'level':[taxDf['incomeLevel1'],taxDf['incomeLevel2'],taxDf['incomeLevel3'],taxDf['incomeLevel4'],taxDf['incomeLevel5'],taxDf['incomeLevel6'],taxDf['incomeLevel7']],'rate':[taxDf['taxeLevel1'],taxDf['taxeLevel2'],taxDf['taxeLevel3'],taxDf['taxeLevel4'],taxDf['taxeLevel5'],taxDf['taxeLevel6'],taxDf['taxeLevel7']]})
                    taxDf = taxDf.replace('',0)
                    taxDf['level'] = [int(x) for x in taxDf['level']]
                    taxDf['rate'] = [int(x)/100 for x in taxDf['rate']]
                    taxDf = taxDf[taxDf['level']>0].dropna()
                    taxDf['rate'] = taxDf['rate'].shift(-1)
                    taxDf = taxDf.fillna(method='ffill')
                    if len(taxDf) == 0 : fixPay['taxe'] = 0
                    else:
                        taxDf = taxDf.sort_values(by=['level']).reset_index().drop(columns=['index'])
                        taxDf['inTax'] = 0
                        for t in taxDf.index:
                            inTax = inTax - taxDf['level'][t]
                            if inTax>0:
                                taxDf['inTax'][t] = inTax
                        taxDf['taxe'] = taxDf['inTax'] * taxDf['rate']
                        fixPay['taxe'] = round(taxDf['taxe'].sum())
                else:
                    fixPay = {'paybeforTax':0,'inTax':0,'insuranceEmployer':0,'insuranceWorker':0,'sanavatDaily':0,'baseDaily':0,'act':0,'taxe':0,'base':0,'subsidy':0,'homing':0,'childern':0,'eydi':0}
                fixPay['fullName'] = fullName
                fixPay['nationalCode'] = c['nationalCode']
                fixPay['freeTaxe'] = fixPay['taxe'] * int(c['freetaxe'])/100
                dfFees = pd.DataFrame(pishkarDb['Fees'].find({'username':username,'UploadDate':data['period']['Show']},{'_id':0,'username':0}))
                if len(dfFees)==0:return json.dumps({'replay':False,'msg':'هیچ بیمه نامه به مشاوران تخصیص نیافته'})
                dfFees = dfFees.drop_duplicates(subset=['شماره بيمه نامه','کد رایانه صدور','كارمزد قابل پرداخت']).set_index('شماره بيمه نامه')
                dfFees = dfFees.dropna(subset=['تاریخ صدور بیمه نامه'])
                dfFees['تاریخ صدور بیمه نامه'] = [timedate.diffTime2(x) for x in dfFees['تاریخ صدور بیمه نامه']]
                dfFees = dfFees[dfFees['تاریخ صدور بیمه نامه']<=1825]
                assing = pd.DataFrame(pishkarDb['assing'].find({'username':username,'consultant':c['nationalCode']},{'_id':0,'username':0}))
                if len(assing)==0:fixPay['reward'] = 0
                else:
                    dfFees = dfFees.join(assing.set_index('شماره بيمه نامه'),how='left')
                    dfFees = dfFees.dropna(subset=['consultant'])
                    if len(dfFees)==0: fixPay['reward'] = 0
                    else:
                        dfFees['feild'] = dfFees['رشته'].fillna('')+' ('+dfFees['مورد بیمه'].fillna('')+')'
                        dfFees['feild'] = [str(x).replace(' ()','') for x in dfFees['feild']]
                        dfFees['feeRate'] = [feeRate(x,c) for x in dfFees['feild']]
                        dfFees['fee'] = dfFees['feeRate'] * dfFees['كارمزد قابل پرداخت']
                        fixPay['reward'] = round(dfFees['fee'].sum())
                berancheDF = pishkarDb['branches'].find_one({'username':username,'managementBranche':fixPay['nationalCode']})
                if berancheDF!=None:
                    valueBranche = pishkarDb['ValueBranche'].find_one({'username':username,'name':berancheDF['BranchesName'],'dateShow':data['period']['Show']})
                    if valueBranche!= None:
                        valueBrancheList = valueBranche['valueBranche']
                        for vb in valueBrancheList:
                            fixPay[vb['title']] = vb['value']
                df = df.append(fixPay,ignore_index=True)
        df = df.fillna(0)
        df['SubReward'] = 0
        for i in df.index:
            berancheDF = pishkarDb['branches'].find_one({'username':username,'managementBranche':df['nationalCode'][i]})
            if berancheDF!=None:
                SubConsultantList = berancheDF['SubConsultantList']
                for nc in SubConsultantList:
                    dff = df[df['nationalCode']==nc]
                    indexDf = list(dff.index)[0]
                    summ = dff['reward'].sum()
                    df['SubReward'][i] = df['SubReward'][i] + summ
                    df['SubReward'][indexDf] = summ * -1
        if len(benefit)>0:
            df = df.set_index('nationalCode').join(benefit,how='left').reset_index().fillna(0)
        else:
            df['benefit'] = 0

        integrations = [x for x in consultantList if x['fristName']=='تلفیق']
        df = df.set_index('nationalCode')
        for i in integrations:
            nationalCode = i['nationalCode']
            mony = df['reward'][nationalCode]
            if mony >0:
                consultantSelect = i['ConsultantSelected']
                for j in consultantSelect:
                    share = round((int(j['fee'])/100) * mony)
                    nationalCodeSelect = j['code']
                    df['reward'][nationalCodeSelect] = df['reward'][nationalCodeSelect] + share
            df = df.drop(nationalCode)
        df = df.reset_index()

        df['benefit'] = [float(x) for x in df['benefit']]
        df['paybeforTax'] = df['paybeforTax'] + df['benefit']
        df['afterPay'] = df['paybeforTax'] - df['taxe'] + df['freeTaxe'] - df['insuranceWorker']
        baseList = ['base', 'act', 'baseDaily', 'sanavatDaily', 'subsidy', 'homing',
            'childern', 'eydi', 'insuranceWorker', 'insuranceEmployer',
            'paybeforTax', 'inTax', 'taxe', 'fullName', 'nationalCode', 'freeTaxe',
            'reward','SubReward', 'afterPay','benefit']
        addedList = [ x for x in df.columns if x not in baseList]
        df['PayBalance'] = df['SubReward'] + df['reward']

        for i in addedList:
            df[i] = [float(x) for x in df[i]]
            df['PayBalance'] = df['PayBalance'] + df[i]
        

        return json.dumps({'replay':True,'pay':df.to_dict(orient='records')})
    else:
        return ErrorCookie()

    
def perforator(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        assing = pd.DataFrame(pishkarDb['assing'].find({'username':username,'consultant':data['nationalCode']},{'_id':0,'username':0}))
        insurec = pd.DataFrame(pishkarDb['insurer'].find({'username':username},{'نام':1,'بیمه گر':1,'_id':0}))
        insurec = insurec.set_index('نام').to_dict(orient='dict')['بیمه گر']
        if len(assing)==0:
            return json.dumps({'replay':False, 'msg':'هیچ بیمه نامه ای یافت نشد'})
        df = pd.DataFrame(pishkarDb['Fees'].find({'username':username,'UploadDate':data['date']['Show']}))
        if len(df)==0:
            return json.dumps({'replay':False, 'msg':'هیچ بیمه نامه ای یافت نشد'})

        df = df.drop_duplicates(subset=['شماره بيمه نامه','کد رایانه صدور','كارمزد قابل پرداخت'],keep='last')

        df = df.set_index('شماره بيمه نامه')
        df = df.dropna(subset=['تاریخ صدور بیمه نامه'])
        df['diffTime'] = [timedate.diffTime2(x) for x in df['تاریخ صدور بیمه نامه']]
        df = df[df['diffTime']<=1825]
        df = df.join(assing.set_index('شماره بيمه نامه'),how='left').reset_index()
        df = df.dropna(subset=['consultant'])
        if len(df)==0:
            return json.dumps({'replay':False, 'msg':'هیچ بیمه نامه ای یافت نشد'})
        df['feild'] = df['رشته'].fillna('')+' ('+df['مورد بیمه'].fillna('')+')'
        df['feild'] = [str(x).replace(' ()','') for x in df['feild']]
        consultant = pishkarDb['cunsoltant'].find_one({'username':username,'nationalCode':data['nationalCode']},{'_id':0})
        df['feeRate'] = [feeRate(x,consultant) for x in df['feild']]
        df['fee'] = df['feeRate'] * df['كارمزد قابل پرداخت']
        df['fee'] = [round(x) for x in df['fee']]
        df = df[['تاریخ صدور بیمه نامه','رشته','بيمه گذار','comp','شماره بيمه نامه','fee']]
        df['comp'] = [insurec[x] for x in df['comp']]
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()

def getbenefit(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        dfBenefit = pd.DataFrame(pishkarDb['benefit'].find({'username':username,'date':data['today']['Show']},{'_id':0,'nationalCode':1,'benefit':1}))
        dfConsultant = pd.DataFrame(pishkarDb['cunsoltant'].find({'username':username},{'_id':0,'fristName':1,'lastName':1,'nationalCode':1}))
        dfConsultant = dfConsultant[dfConsultant['fristName']!='تلفیق']
        if len(dfConsultant)==0:
            return json.dumps({'replay':False,'msg':'هیچ مشاوری یافت نشد'})
        if len(dfBenefit)==0:
            dfConsultant['benefit'] = 0
        else:
            dfConsultant = dfConsultant.set_index('nationalCode').join(dfBenefit.set_index('nationalCode')).reset_index()
        dfConsultant = dfConsultant.fillna(0)
        df = dfConsultant.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()

def setbenefit(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        if pishkarDb['benefit'].find_one({'username':username,'date':data['date']['Show']})!=None:
            pishkarDb['benefit'].delete_many({'username':username,'date':data['date']['Show']})
        df = pd.DataFrame(data['ListBenefit'])[['nationalCode','benefit']]
        df['username'] = username
        df['date'] = data['date']['Show']
        df['timestump'] = data['date']['date']
        df = df.to_dict(orient='records')
        pishkarDb['benefit'].insert_many(df)
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def copylastmonth(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        if pishkarDb['benefit'].find_one({'username':username,'date':data['date']['Show']})!=None:
            pishkarDb['benefit'].delete_many({'username':username,'date':data['date']['Show']})
        df = pd.DataFrame(pishkarDb['benefit'].find({'username':username},{'_id':0}))
        df = df[df['timestump']==df['timestump'].max()]
        df['date'] = data['date']['Show']
        df['timestump'] = data['date']['date']
        df = df.to_dict(orient='records')
        pishkarDb['benefit'].insert_many(df)
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()