import json
import timedate
import pymongo
import pandas as pd
from Sing import cookie, ErrorCookie
from timedate import ColorRandom
client = pymongo.MongoClient()
pishkarDb = client['pishkar']

def are_all_characters_digits(input_string):
    for char in input_string:
        if not char.isdigit():
            return False
    return True
    

df = pd.DataFrame(pishkarDb['customers'].find({}))
df['s'] = df['تلفن همراه'].apply(are_all_characters_digits)
df = df[df['s']==False]
for i in df.index:
    id = df['_id'][i]
    pishkarDb['customers'].update_many({'_id':id},{'$set':{'تلفن همراه':''}})
    print(id)