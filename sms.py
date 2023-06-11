import json
import requests
import random

frm ='30001526'
usrnm = 'isatispooya'
psswrd ='5246043adeleh'

def SendSms(snd,txt):
    resp = requests.get(url=f'http://tsms.ir/url/tsmshttp.php?from={frm}&to={snd}&username={usrnm}&password={psswrd}&message={txt}').json()
    return resp

def VerificationPhone(data):
    snd = data['phone']
    code = random.randint(10000,99999)
    text = 'کد تایید پیشکار \n' + str(code)
    res = SendSms(snd,text)
    print(text)
    return json.dumps({'replay':True,'code':code})

def CheckDeliver(smsId):
    resp = requests.get(url=f'http://tsms.ir/url/tsmshttp.php?from={frm}&username={usrnm}&password={psswrd}&deliver20={smsId}')
    return {'statusCode':resp.status_code,'content':resp.content}
