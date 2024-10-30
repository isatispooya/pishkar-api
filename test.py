from pymongo import MongoClient
import pandas as pd
port = 27017
pishkarDb= MongoClient('192.168.11.11' , port)
pishkarDb = pishkarDb['pishkar']


df = pd.read_excel('df.xlsx',dtype=str)
for i in df.index:
    code = df['کد رایانه صدور'][i]
    ins = pishkarDb['Fees'].find_one({'کد رایانه صدور':int(code)})
    num = ins['شماره بيمه نامه']
    nc = df['مشاور'][i]
    dup = pishkarDb['AssingIssuing'].find_one({'کد رایانه صدور':num}) == None
    if dup:
        pishkarDb['Fees'].insert_one({'شماره بيمه نامه':num,'consultant':nc,'username':'09131533223'})

