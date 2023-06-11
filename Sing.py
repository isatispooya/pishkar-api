import json
import pymongo
import crypto
import timedate
from lincens import FnclincensListCounsoltant , FnclincensList
from captcha import captchaGenerate
import ast

client = pymongo.MongoClient()
pishkarDb = client['pishkar']






def login(data):
    phone = data['phone']
    cookieGenerate = {'main':phone,'sub':'0'}
    if pishkarDb['username'].find_one({'phone':phone})==None:
        if pishkarDb['sub'].find_one({'subPhone':phone})==None:
            regDate = str(timedate.toDay())
            limDate = str(timedate.deltaTime(30))
            pishkarDb['username'].insert_one({'phone':phone, 'name':'', 'email':'', 'address':'', 'phonework':'', 'company':'','registerdate':regDate, 'limitDate':limDate, 'management':True})

        else:
            sub = pishkarDb['sub'].find_one({'subPhone':phone})
            cookieGenerate['sub'] = sub['subPhone']
            cookieGenerate['main'] = sub['username']
            return json.dumps({'replay':True,'Cookie':crypto.encrypt(cookieGenerate).decode()})
    return json.dumps({'replay':True,'Cookie':crypto.encrypt(cookieGenerate).decode()})

def cookie(data):
    try:cookie = str(data['cookie']).encode()
    except:cookie = str(data).encode()
    cookie = crypto.decrypt(cookie)
    cookie = ast.literal_eval(cookie)
    user = pishkarDb['username'].find_one({'phone':cookie['main']},{'_id':0})
    if user==None:
        return json.dumps({'replay':False})
    else:
        AuthorizationUser = pishkarDb['sub'].find_one({'username':cookie['main'],'subPhone':cookie['sub']},{'_id':0,'subPhone':0,'username':0})
        if AuthorizationUser != None:
            AuthorizationUser['all']=False
        else:
            AuthorizationUser = {'all':True}
        limAccunt = timedate.diffTime(user['limitDate'])
        user['limAccunt'] = limAccunt
        AuthorizationUser = FnclincensList(AuthorizationUser)
        return json.dumps({'replay':True,'user':user,'Authorization':AuthorizationUser})

def ErrorCookie():
    return json.dumps({'replay':False,'msg':'خطا در شناسایی کاربر لطفا مجدد تلاش کنید'})
    


def captcha(data):
    cg = captchaGenerate()
    return json.dumps({'cptcha':cg[0],'img':cg[1]})