import json
import timedate
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
from timedate import ColorRandom
client = pymongo.MongoClient()
pishkarDb = client['pishkar']




df = pd.DataFrame(pishkarDb['issuingLife'].find({'comp':'خاورمیانه'}))
listDf = list(set(df['شماره بيمه نامه'].to_list()))

for i in listDf:
    pishkarDb['AssingIssuingLife'].update_one({'شماره بيمه نامه':int(i)},{'$set':{'شماره بيمه نامه':i}})
    pishkarDb['assing'].update_one({'شماره بيمه نامه':int(i)},{'$set':{'شماره بيمه نامه':i}})
    print(i)
