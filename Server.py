import json
from flask import Flask, request
import pymongo
import pandas as pd
import warnings
from flask_cors import CORS
import management
import Sing
import sms
import timedate
import feesreports
import assing
import consultant
import Pay
import branche
import issuing
import winreg as reg
import reports
import Authorization
import dashboard
import customers
import SystemMassage
from waitress import serve
import multiprocessing

def addToReg():
    key = reg.OpenKey(reg.HKEY_CURRENT_USER , "Software\Microsoft\Windows\CurrentVersion\Run" ,0 , reg.KEY_ALL_ACCESS) # Open The Key
    reg.SetValueEx(key ,"any_name" , 0 , reg.REG_SZ , __file__)
    reg.CloseKey(key)

addToReg()
warnings.filterwarnings("ignore")
client = pymongo.MongoClient()
app = Flask(__name__)
CORS(app)
app.config['THREADS_PER_PAGE'] = multiprocessing.cpu_count()


@app.route('/sing/verificationphone',methods = ['POST', 'GET'])
def verificationphone():
    data = request.get_json()
    return sms.VerificationPhone(data)

@app.route('/sing/login',methods = ['POST', 'GET'])
def login():
    data = request.get_json()
    return Sing.login(data)

@app.route('/sing/cookie',methods = ['POST', 'GET'])
def cookie():
    data = request.get_json()
    return Sing.cookie(data)

@app.route('/sing/captcha',methods = ['POST', 'GET'])
def captcha():
    data = request.get_json()
    return Sing.captcha(data)

#----------------- Management -----------------
@app.route('/management/profile',methods = ['POST', 'GET'])
def management_profile():
    data = request.get_json()
    return management.profile(data)

@app.route('/management/cunsoltant',methods = ['POST', 'GET'])
def management_cunsoltant():
    data = request.get_json()
    return management.cunsoltant(data)

@app.route('/management/getcunsoltant',methods = ['POST', 'GET'])
def management_getcunsoltant():
    data = request.get_json()
    return management.getcunsoltant(data)


@app.route('/management/getcunsoltantandall',methods = ['POST', 'GET'])
def management_getcunsoltantandall():
    data = request.get_json()
    return management.getcunsoltantAndAll(data)


@app.route('/management/delcunsoltant',methods = ['POST', 'GET'])
def management_delcunsoltant():
    data = request.get_json()
    return management.delcunsoltant(data)

@app.route('/management/workinghour',methods = ['POST', 'GET'])
def management_workinghour():
    data = request.get_json()
    return management.workinghour(data)

@app.route('/management/getworkinghour',methods = ['POST', 'GET'])
def management_getworkinghour():
    data = request.get_json()
    return management.getworkinghour(data)

@app.route('/management/getaworkinghour',methods = ['POST', 'GET'])
def management_getaworkinghour():
    data = request.get_json()
    return management.getaworkinghour(data)

@app.route('/management/delworkinghour',methods = ['POST', 'GET'])
def management_delworkinghour():
    data = request.get_json()
    return management.delworkinghour(data)

@app.route('/management/addinsurer',methods = ['POST', 'GET'])
def management_addinsurer():
    data = request.get_json()
    return management.addinsurer(data)

@app.route('/management/getinsurer',methods = ['POST', 'GET'])
def management_getinsurer():
    data = request.get_json()
    return management.getinsurer(data)

@app.route('/management/delinsurer',methods = ['POST', 'GET'])
def management_delinsurer():
    data = request.get_json()
    return management.delinsurer(data)

@app.route('/management/salary',methods = ['POST', 'GET'])
def management_salary():
    data = request.get_json()
    return management.salary(data)


@app.route('/management/getsalary',methods = ['POST', 'GET'])
def management_getsalary():
    data = request.get_json()
    return management.getsalary(data)

@app.route('/management/delsalary',methods = ['POST', 'GET'])
def management_delsalary():
    data = request.get_json()
    return management.delsalary(data)

@app.route('/management/settax',methods = ['POST', 'GET'])
def management_settax():
    data = request.get_json()
    return management.settax(data)

@app.route('/management/gettax',methods = ['POST', 'GET'])
def management_gettax():
    data = request.get_json()
    return management.gettax(data)

@app.route('/management/deltax',methods = ['POST', 'GET'])
def management_deltax():
    data = request.get_json()
    return management.deltax(data)

@app.route('/management/setsub',methods = ['POST', 'GET'])
def management_setsub():
    data = request.get_json()
    return management.setsub(data)

@app.route('/management/delsub',methods = ['POST', 'GET'])
def management_delsub():
    data = request.get_json()
    return management.delsub(data)


@app.route('/management/getgroupsalary',methods = ['POST', 'GET'])
def management_getgroupsalary():
    data = request.get_json()
    return management.getgroupsalary(data)


@app.route('/management/addbranche',methods = ['POST', 'GET'])
def management_addbranche():
    data = request.get_json()
    return management.addbranche(data)

@app.route('/management/getbranche',methods = ['POST', 'GET'])
def management_getbranche():
    data = request.get_json()
    return management.getbranche(data)

@app.route('/management/delbranche',methods = ['POST', 'GET'])
def management_delbranche():
    data = request.get_json()
    return management.delbranche(data)

@app.route('/branche/copypreviousmonth',methods = ['POST', 'GET'])
def management_copyPreviousMonth():
    data = request.get_json()
    return branche.copyPreviousMonth(data)

@app.route('/management/addminsalary',methods = ['POST', 'GET'])
def management_addminsalary():
    data = request.get_json()
    return management.addminsalary(data)

@app.route('/management/getminsalary',methods = ['POST', 'GET'])
def management_getminsalary():
    data = request.get_json()
    return management.getminsalary(data)


@app.route('/management/delminsalary',methods = ['POST', 'GET'])
def management_delminsalary():
    data = request.get_json()
    return management.delminsalary(data)

@app.route('/management/setsettingsms',methods = ['POST'])
def management_setsettingsms():
    data = request.get_json()
    return management.setsettingsms(data)

@app.route('/management/personalized',methods = ['POST'])
def management_personalized():
    data = request.get_json()
    return management.personalized(data)


@app.route('/management/getsettingsms',methods = ['POST'])
def management_getsettingsms():
    data = request.get_json()
    return management.getsettingsms(data)

@app.route('/management/getallrevivalbydate',methods = ['POST'])
def management_getallrevivalbydate():
    data = request.get_json()
    return management.getallrevivalbydate(data)


#----------------- General -----------------
@app.route('/general/today',methods = ['POST', 'GET'])
def general_today():
    return json.dumps({'today':str(timedate.toDay())})
#----------------- feesreports -----------------
@app.route('/feesreports/uploadfile',methods = ['POST', 'GET'])
def feesreports_uploadfile():
    date = request.form['date']
    cookie = request.form['cookie']
    file =  request.files['feesFile']
    comp = request.form['comp']
    return feesreports.uploadfile(date,cookie,file,comp)

@app.route('/feesreports/getfeesuploads',methods = ['POST', 'GET'])
def feesreports_getfeesuploads():
    data = request.get_json()
    return feesreports.getfeesuploads(data)

@app.route('/feesreports/delupload',methods = ['POST', 'GET'])
def feesreports_delupload():
    data = request.get_json()
    return feesreports.delupload(data)

@app.route('/feesreports/getinsurer',methods = ['POST', 'GET'])
def feesreports_getinsurer():
    data = request.get_json()
    return feesreports.getinsurer(data)

@app.route('/feesreports/getinsurername',methods = ['POST', 'GET'])
def feesreports_getinsurername():
    data = request.get_json()
    return feesreports.getinsurerName(data)

@app.route('/feesreports/getallfeesFile',methods = ['POST', 'GET'])
def feesreports_getallfeesFile():
    cookie = request.form['cookie']
    file =  request.files['feesFile']
    comp = request.form['comp']
    return feesreports.getallfeesFile(cookie,file,comp)

@app.route('/standardfees/get',methods = ['POST', 'GET'])
def standardfees_get():
    data = request.get_json()
    return feesreports.standardfeesget(data)


@app.route('/standardfees/getField',methods = ['POST', 'GET'])
def standardfees_getField():
    data = request.get_json()
    return feesreports.getField(data)

@app.route('/standardfees/addfield',methods = ['POST', 'GET'])
def standardfees_addfield():
    data = request.get_json()
    return feesreports.addfield(data)

@app.route('/standardfees/delfield',methods = ['POST', 'GET'])
def standardfees_delfield():
    data = request.get_json()
    return feesreports.delfield(data)
#----------------- assing -----------------
@app.route('/assing/get',methods = ['POST', 'GET'])
def assing_get():
    data = request.get_json()
    return assing.get(data)

@app.route('/assing/getinsurnac',methods = ['POST', 'GET'])
def assing_getinsurnac():
    data = request.get_json()
    return assing.getinsurnac(data)

@app.route('/assing/set',methods = ['POST', 'GET'])
def assing_set():
    data = request.get_json()
    return assing.set(data)
#----------------- consultant -----------------
@app.route('/consultant/getfees',methods = ['POST', 'GET'])
def consultant_getfees():
    data = request.get_json()
    return consultant.getfees(data)

@app.route('/consultant/setfees',methods = ['POST', 'GET'])
def consultant_setfees():
    data = request.get_json()
    return consultant.setfees(data)

@app.route('/consultant/getatc',methods = ['POST', 'GET'])
def consultant_getatc():
    data = request.get_json()
    return consultant.getatc(data)

@app.route('/consultant/setatc',methods = ['POST', 'GET'])
def consultant_setatc():
    data = request.get_json()
    return consultant.setatc(data)

@app.route('/consultant/actcopylastmonth',methods = ['POST', 'GET'])
def consultant_actcopylastmonth():
    data = request.get_json()
    return consultant.actcopylastmonth(data)

@app.route('/consultant/forcastfee',methods = ['POST', 'GET'])
def consultant_forcastfee():
    data = request.get_json()
    return consultant.forcastfee(data)

@app.route('/consultant/forcastfeelife',methods = ['POST', 'GET'])
def consultant_forcastfeelife():
    data = request.get_json()
    return consultant.forcastfeelife(data)


@app.route('/consultant/addintegration',methods = ['POST', 'GET'])
def consultant_addintegration():
    data = request.get_json()
    return consultant.addintegration(data)

@app.route('/consultant/getintegration',methods = ['POST', 'GET'])
def consultant_getintegration():
    data = request.get_json()
    return consultant.getintegration(data)

@app.route('/consultant/delintegration',methods = ['POST', 'GET'])
def consultant_delintegration():
    data = request.get_json()
    return consultant.delintegration(data)

#----------------- Pay -----------------
@app.route('/pay/get',methods = ['POST', 'GET'])
def pay_get():
    data = request.get_json()
    return Pay.get(data)

@app.route('/pay/perforator',methods = ['POST', 'GET'])
def pay_perforator():
    data = request.get_json()
    return Pay.perforator(data)

@app.route('/pay/getbenefit',methods = ['POST', 'GET'])
def pay_getbenefit():
    data = request.get_json()
    return Pay.getbenefit(data)

@app.route('/pay/setbenefit',methods = ['POST', 'GET'])
def pay_setbenefit():
    data = request.get_json()
    return Pay.setbenefit(data)

@app.route('/benefit/copylastmonth',methods = ['POST', 'GET'])
def pay_copylastmonth():
    data = request.get_json()
    return Pay.copylastmonth(data)

#----------------- assing -----------------
@app.route('/branche/getvalue',methods = ['POST', 'GET'])
def branche_getvalue():
    data = request.get_json()
    return branche.getvalue(data)

@app.route('/branche/addvalue',methods = ['POST', 'GET'])
def branche_addvalue():
    data = request.get_json()
    return branche.addvalue(data)

@app.route('/issuing/cheackadditional',methods = ['POST', 'GET'])
def issuing_CheackAdditional():
    cookie = request.form['cookie']
    file =  request.files['feesFile']
    comp = request.form['comp']
    additional = request.form['additional']
    return issuing.CheackAdditional(cookie,file,comp,additional)

@app.route('/issuing/getdf',methods = ['POST', 'GET'])
def issuing_getdf():
    data = request.get_json()
    return issuing.getdf(data)

@app.route('/issuing/getdflife',methods = ['POST', 'GET'])
def issuing_getdfLife():
    data = request.get_json()
    return issuing.getdfLife(data)

@app.route('/issuing/delfilelife',methods = ['POST', 'GET'])
def issuing_deldfLife():
    data = request.get_json()
    return issuing.deldfLife(data)

@app.route('/issuing/addissuinglifemanual',methods = ['POST', 'GET'])
def issuing_addissuinglifemanual():
    data = request.get_json()
    return issuing.addissuinglifemanual(data)

@app.route('/issuing/addissuinglifemanuallife',methods = ['POST', 'GET'])
def issuing_addissuinglifemanuallife():
    data = request.get_json()
    return issuing.delissuingmanuallife(data)

@app.route('/issuing/getnuminslist',methods = ['POST', 'GET'])
def issuing_getnuminslist():
    data = request.get_json()
    return issuing.getnuminslist(data)


@app.route('/inssuing/getcunsoltant',methods = ['POST', 'GET'])
def issuing_get():
    data = request.get_json()
    return issuing.getcunsoltant(data)

@app.route('/inssuing/getcunsoltantlife',methods = ['POST', 'GET'])
def issuingLife_get():
    data = request.get_json()
    return issuing.getcunsoltantLife(data)

@app.route('/inssuing/assingcunsoltant',methods = ['POST', 'GET'])
def issuing_assingcunsoltant():
    data = request.get_json()
    return issuing.assingcunsoltant(data)

@app.route('/inssuing/assingcunsoltantlife',methods = ['POST', 'GET'])
def issuing_assingcunsoltantLife():
    data = request.get_json()
    return issuing.assingcunsoltantLife(data)

@app.route('/issuing/getissuingmanual',methods = ['POST', 'GET'])
def issuing_getissuingmanual():
    data = request.get_json()
    return issuing.getissuingmanual(data)

@app.route('/issuing/getissuinglifemanual',methods = ['POST', 'GET'])
def issuing_getIssuingLifeManual():
    data = request.get_json()
    return issuing.getIssuingLifeManual(data)


@app.route('/issuing/addissuingmanual',methods = ['POST', 'GET'])
def issuing_addissuingmanual():
    data = request.get_json()
    return issuing.addissuingmanual(data)

@app.route('/issuing/delissuingmanual',methods = ['POST', 'GET'])
def issuing_delissuingmanual():
    data = request.get_json()
    return issuing.delissuingmanual(data)

@app.route('/issuing/delfile',methods = ['POST', 'GET'])
def issuing_delfile():
    data = request.get_json()
    return issuing.delfile(data)

@app.route('/issuing/getadditional',methods = ['POST', 'GET'])
def issuing_getadditional():
    data = request.get_json()
    return issuing.getadditional(data)

@app.route('/issuing/addaditional',methods = ['POST', 'GET'])
def issuing_addaditional():
    data = request.get_json()
    return issuing.addaditional(data)

@app.route('/issuing/sales',methods = ['POST', 'GET'])
def issuing_sales():
    data = request.get_json()
    return issuing.sales(data)

@app.route('/report/comparisom',methods = ['POST', 'GET'])
def report_comparisom():
    data = request.get_json()
    return reports.comparisom(data)

@app.route('/authorization/lincenslist',methods = ['POST', 'GET'])
def Authorization_lincenslist():
    data = request.get_json()
    return Authorization.lincenslist(data)

@app.route('/authorization/getsub',methods = ['POST', 'GET'])
def management_getsub():
    data = request.get_json()
    return Authorization.getsub(data)




@app.route('/dashboard/issunigsum',methods = ['POST', 'GET'])
def dashboard_issunigSum():
    data = request.get_json()
    return dashboard.issunigSum(data)

@app.route('/dashboard/feesum',methods = ['POST', 'GET'])
def dashboard_feeSum():
    data = request.get_json()
    return dashboard.FeeSum(data)

@app.route('/dashboard/issunigFeild',methods = ['POST', 'GET'])
def dashboard_issunigFeild():
    data = request.get_json()
    return dashboard.issunigFeild(data)

@app.route('/dashboard/issuniginsurer',methods = ['POST', 'GET'])
def dashboard_issuniginsurer():
    data = request.get_json()
    return dashboard.issuniginsurer(data)

@app.route('/dashboard/feesum',methods = ['POST', 'GET'])
def dashboard_FeeSum():
    data = request.get_json()
    return dashboard.FeeSum(data)

@app.route('/dashboard/feefeild',methods = ['POST', 'GET'])
def dashboard_FeeFeild():
    data = request.get_json()
    return dashboard.FeeFeild(data)

@app.route('/dashboard/feeinsurence',methods = ['POST', 'GET'])
def dashboard_FeeInsurence():
    data = request.get_json()
    return dashboard.FeeInsurence(data)

@app.route('/dashboard/cunsoltantissunigsum',methods = ['POST', 'GET'])
def dashboard_cunsoltantissunigsum():
    data = request.get_json()
    return dashboard.cunsoltantIssunigSum(data)

@app.route('/dashboard/cunsoltantissunigFeild',methods = ['POST', 'GET'])
def dashboard_cunsoltantIssunigFeild():
    data = request.get_json()
    return dashboard.cunsoltantIssunigFeild(data)

@app.route('/dashboard/cunsoltantissuniginsurer',methods = ['POST', 'GET'])
def dashboard_cunsoltantissuniginsurer():
    data = request.get_json()
    return dashboard.cunsoltantIssunigInsurer(data)


@app.route('/reports/customerratingsfee',methods = ['POST', 'GET'])
def reports_CustomerRatingsFee():
    data = request.get_json()
    return reports.CustomerRatingsFee(data)

@app.route('/reports/customerratingsissuing',methods = ['POST', 'GET'])
def reports_CustomerRatingsIssuing():
    data = request.get_json()
    return reports.CustomerRatingsIssuing(data)


@app.route('/customers/uploadfile',methods = ['POST', 'GET'])
def customers_uploadfile():
    cookie = request.form['cookie']
    file =  request.files['File']
    comp = request.form['comp']
    return customers.uploadfile(cookie,file,comp)

@app.route('/customers/customergroup',methods = ['POST', 'GET'])
def customers_customergroup():
    data = request.get_json()
    return customers.customergroup(data)

@app.route('/customers/customermanual',methods = ['POST', 'GET'])
def customers_customermanual():
    data = request.get_json()
    return customers.customermanual(data)

@app.route('/customers/delcustomergroup',methods = ['POST', 'GET'])
def customers_delCustomerGroup():
    data = request.get_json()
    return customers.delCustomerGroup(data)

@app.route('/customers/delcustomermanual',methods = ['POST', 'GET'])
def customers_delCustomerManual():
    data = request.get_json()
    return customers.delCustomerManual(data)

@app.route('/customers/edit',methods = ['POST', 'GET'])
def customers_Edit():
    data = request.get_json()
    return customers.Edit(data)

@app.route('/systemmassage/alarm',methods = ['POST', 'GET'])
def systemmassage_Alarm():
    data = request.get_json()
    return SystemMassage.Alarm(data)

@app.route('/systemmassage/customercheck',methods = ['POST', 'GET'])
def systemmassage_customerCheck():
    data = request.get_json()
    return SystemMassage.customerCheck(data)

@app.route('/systemmassage/standardfeecheck',methods = ['POST', 'GET'])
def systemmassage_standardFeeCheck():
    data = request.get_json()
    return SystemMassage.standardFeeCheck(data)

@app.route('/systemmassage/counsultantfee',methods = ['POST', 'GET'])
def systemmassage_counsultantFee():
    data = request.get_json()
    return SystemMassage.counsultantFee(data)

@app.route('/systemmassage/revival',methods = ['POST', 'GET'])
def systemmassage_revival():
    data = request.get_json()
    return SystemMassage.revival(data)


@app.route('/systemmassage/liferevival',methods = ['POST', 'GET'])
def systemmassage_lifeRevival():
    data = request.get_json()
    return SystemMassage.lifeRevival(data)

@app.route('/systemmassage/liferevivalyear',methods = ['POST', 'GET'])
def systemmassage_liferevivalyear():
    data = request.get_json()
    return SystemMassage.liferevivalyear(data)

@app.route('/systemmassage/customerincomp',methods = ['POST', 'GET'])
def systemmassage_customerincomp():
    data = request.get_json()
    return SystemMassage.customerincomp(data)

@app.route('/systemmassage/customerincompnocode',methods = ['POST', 'GET'])
def systemmassage_customerincompnocode():
    data = request.get_json()
    return SystemMassage.customerincompnocode(data)


@app.route('/issuing/revival',methods = ['POST', 'GET'])
def issuing_revival():
    data = request.get_json()
    return issuing.revival(data)

@app.route('/issuing/liferevival',methods = ['POST', 'GET'])
def issuing_revivalLife():
    data = request.get_json()
    return issuing.revivalLife(data)

@app.route('/issuing/liferevivalgroup',methods = ['POST', 'GET'])
def issuing_liferevivalgroup():
    data = request.get_json()
    return issuing.liferevivalgroup(data)


@app.route('/issuing/addenLife',methods = ['POST', 'GET'])
def issuing_addenLife():
    data = request.get_json()
    return issuing.addenLife(data)

@app.route('/issuing/AddLifeAdden',methods = ['POST', 'GET'])
def issuing_AddLifeAdden():
    data = request.get_json()
    return issuing.AddLifeAdden(data)

@app.route('/issuing/deladdenlife',methods = ['POST', 'GET'])
def issuing_deladdenlife():
    data = request.get_json()
    return issuing.deladdenlife(data)
    
@app.route('/issuing/getinsurerbynum',methods = ['POST', 'GET'])
def issuing_getinsurerbynum():
    data = request.get_json()
    return issuing.getinsurerbynum(data)


@app.route('/issuing/revivalall',methods = ['POST', 'GET'])
def issuing_revivalall():
    data = request.get_json()
    return issuing.revivalall(data)



@app.route('/issuing/lifefile',methods = ['POST', 'GET'])
def issuing_lifefile():
    cookie = request.form['cookie']
    file =  request.files['feesFile']
    comp = request.form['comp']
    return issuing.lifefile(cookie,file,comp)


@app.route('/issuing/nofee',methods = ['POST', 'GET'])
def issuing_nofee():
    data = request.get_json()
    return issuing.noFee(data)

@app.route('/report/optionprofit',methods = ['POST', 'GET'])
def report_optionprofit():
    data = request.get_json()
    return reports.optionprofit(data)

@app.route('/report/profit',methods = ['POST', 'GET'])
def report_profit():
    data = request.get_json()
    return reports.profit(data)


if __name__ == '__main__':
    #serve(app, host="0.0.0.0", port=8080,threads= 8)
    app.run(host='0.0.0.0', debug=True)