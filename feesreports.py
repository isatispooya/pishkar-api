import json
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
import timedate
client = pymongo.MongoClient()
pishkarDb = client['pishkar']


def uploadfile(date,cookier,file,comp):
    user = cookie(cookier)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        duplic = pd.DataFrame(pishkarDb['Fees'].find({'UploadDate':date,'comp':comp,'username':username}))
        if len(duplic) == 0:
            df = pd.read_excel(file, dtype={'شماره بيمه نامه':str})
            columns = (pishkarDb['insurer'].find_one({'username':username, 'نام':comp},{'_id':0, 'نام':0,'username':0,'بیمه گر':0}))
            for i in columns:
                if columns[i] in df.columns:
                    df.rename(columns={columns[i]:i})
                else:
                    return json.dumps({'replay':False, 'msg':f'فایل فاقد ستون {columns[i]} است'})
            df['UploadDate'] = date
            df['comp'] = comp
            df['username'] = username
            df= df.to_dict(orient='records')
            pishkarDb['Fees'].insert_many(df)
            return json.dumps({'replay':True, 'len':len(df)})
        else:
            return json.dumps({'replay':False, 'msg':f'فایل گزارش شرکت ({comp}) برای ({date}) قبلا ثبت شده است.'})
    else:
        return ErrorCookie()


def getfeesuploads(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['Fees'].find({'username':username}))
        if len(df)>0:
            df = df[['comp','UploadDate','کد رایانه صدور','شماره بيمه نامه','كارمزد قابل پرداخت']]
            if df['comp'][df.index.min()] in ['خاور میانه','ایران'] :
                df = df.drop_duplicates()
            df = df.groupby(by=['comp','UploadDate']).sum(numeric_only=True).reset_index()
            df = df[['comp','UploadDate','كارمزد قابل پرداخت']]
            insurec = pd.DataFrame(pishkarDb['insurer'].find({'username':username},{'نام':1,'بیمه گر':1,'_id':0}))
            insurec = insurec.set_index('نام').to_dict(orient='dict')['بیمه گر']
            try:df['insurec'] = [insurec[x] for x in df['comp']]
            except: json.dumps({'replay':False})
            df['UploadDate'] =[timedate.PriodStrToIntWDash(x) for x in df['UploadDate']]
            df = df.to_dict(orient='records')
            return json.dumps({'df':df})
        else:
            return json.dumps({'df':None})
    else:
        return ErrorCookie()

def delupload(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        period = timedate.dateToPriod(data['date'].replace('-','/')).replace(' ', ' - ')
        pp = pishkarDb['Fees'].delete_many({'username':username,'UploadDate':period,'comp':data['comp']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()


def getinsurer(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        insurer = pd.DataFrame(pishkarDb['insurer'].find({'username':username}))
        if len(insurer)>0:
            insurer = list(set(insurer['نام']))
            return json.dumps({'replay':True, 'insurer':insurer})
        else:
            return json.dumps({'replay':False, 'msg':'هیچ بیمه گذاری ثبت نشده'})
    else:
        return ErrorCookie()

def getinsurerName(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        insurer = pd.DataFrame(pishkarDb['insurer'].find({'username':username}))
        if len(insurer)>0:
            insurer = list(set(insurer['بیمه گر']))
            return json.dumps({'replay':True, 'insurer':insurer})
        else:
            return json.dumps({'replay':False, 'msg':'هیچ بیمه گذاری ثبت نشده'})
    else:
        return ErrorCookie()
    
def getallfeesFile(cookier,file,comp):
    user = cookie(cookier)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.read_excel(file)
        columns = (pishkarDb['insurer'].find_one({'username':username, 'نام':comp},{'_id':0, 'نام':0,'username':0}))
        for i in columns:
            if columns[i] in df.columns:
                df.rename(columns={columns[i]:i})
            else:
                return json.dumps({'replay':False, 'msg':f'فایل فاقد ستون {columns[i]} است'})
        df = df.drop_duplicates(subset=['کد رایانه صدور','كارمزد قابل پرداخت'])
        df = int(df['كارمزد قابل پرداخت'].sum())
        return json.dumps({'Allfees':df})
    else:
        return ErrorCookie()


def standardfeesget(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['standardfee'].find({'username':username},{'_id':0}))
        df = df.to_dict(orient='records')
        return json.dumps({'replay':True,'df':df})
    else:
        return ErrorCookie()


def getField(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        df = pd.DataFrame(pishkarDb['Fees'].find({'username':username},{'_id':0,'مورد بیمه':1,'رشته':1}))
        dfissuing = pd.DataFrame(pishkarDb['issuing'].find({'username':username},{'_id':0,'مورد بیمه':1,'رشته':1}))
        df = pd.concat([df,dfissuing])
        df = df.fillna('')
        df['Field'] = df['رشته'] + ' '+ '('+ df['مورد بیمه'] + ')'
        df = [str(x).replace(' ()','') for x in df['Field']]
        df = list(set(df))
        GroupField = {
                'بیمه آتش سوزی':['منازل مسکونی','خطرات غیر صنعتی','خطرات صنعتی'],
                'بیمه باربری':['کالاهای وارداتی','کالاهای داخلی و صادراتی','باربری به نفع بانک'],
                'بیمه وسائط نقلیه موتوری':['وسائط نقلیه سواری','بارکش','اتوبوس-مینی بوس','انواع موتورسیکلت، دوچرخه','ماشین آلات کشاورزی','ریلی'],
                'بیمه مسئولیت':['بیمه اجباری شخص ثالث ','کشتی، شناور','متصدیان حمل و نقل','تعهد پرداخت حقوق گمرکی','سایر مسئولیت ها'],
                'بیمه حوادث شخصی و درمان':['حوادث انفرادی','حوادث گروهی','درمان انفرادی','درمان گروهی','حوادث راننده','مسافرتی','دندان پزشکی انفرادی','دندان پزشکی گروهی'],
                'اعتبار':['داخلی','صادرات کالا و حدمات'],
                'کشاورزی':['درمان و تلف','محصولات زراعی و باغی'],
                'سایر بیمه':['وجوه در صندوق و در حمل','عدم النفع','صداقت و امانت','مهندسی،عیوب ساختمان','وسایل نقلیه هوایی،حوادث خدمه','شناور و حوادث خدمه','اکتشاف و استخراج نفت','دزدی با شکست حرز','شکست شیشه','مرهونات به نفع بانک','دام و طیور'],
                'بیمه زندگی':['خطر فوت زمانی','شرط حیات','مختلط']
            }
        return json.dumps({'replay':True,'Field':df,'GroupField':GroupField})
    else:
        return ErrorCookie()

def addfield(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        if data['GroupFieldSelected']['main']=='بیمه زندگی':
            rate = ''
            years = data['years']

        else:
            rate = data['rate']
            years = ''

        pishkarDb['standardfee'].delete_many({'username':username,'field':data['field'],'dateshow':data['date']['Show']})
        pishkarDb['standardfee'].insert_one({'username':username,'field':data['field'],'rate':rate,'dateshow':data['date']['Show'],'date':data['date']['date'],'groupMain':data['GroupFieldSelected']['main'],'groupSub':data['GroupFieldSelected']['sub'],'years':years})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()

def delfield(data):
    user = cookie(data)
    user = json.loads(user)
    username = user['user']['phone']
    if user['replay']:
        pishkarDb['standardfee'].delete_many({'username':username,'field':data['field'],'dateshow':data['date']})
        return json.dumps({'replay':True})
    else:
        return ErrorCookie()
