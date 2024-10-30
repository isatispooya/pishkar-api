
from persiantools.jdatetime import JalaliDate
from persiantools.jdatetime import JalaliDateTime
from dateutil.relativedelta import relativedelta
import datetime
import time
import random
import pandas as pd

def toDaySlash():
    now = datetime.datetime.now()
    return str(JalaliDate(datetime.date(now.year, now.month, now.day))).replace('-','/')

def toDayInt():
    now = datetime.datetime.now()
    return int(str(JalaliDate(datetime.date(now.year, now.month, now.day))).replace('-',''))

def deltaToDayInt(period,date):
    now = str(date)
    start = JalaliDate(int(now[:4]), int(now[4:6]), int(now[6:8])).to_gregorian()
    start = start - datetime.timedelta(int(period))
    return int(str(JalaliDate(datetime.date(start.year, start.month, start.day))).replace('-',''))

def toDay():
    now = datetime.datetime.now()
    return JalaliDate(datetime.date(now.year, now.month, now.day))

def deltaTime(toDay):
    now = datetime.datetime.now()
    delta = now + datetime.timedelta(toDay)
    return JalaliDate(datetime.date(delta.year, delta.month, delta.day))


def diffTime(deltaTime):
    now = str(datetime.datetime.now().date())
    t = str(deltaTime).split('-')
    d1 = str(JalaliDate(int(t[0]), int(t[1]), int(t[2])).to_gregorian())
    res = (datetime.datetime.strptime(d1, "%Y-%m-%d") - datetime.datetime.strptime(now, "%Y-%m-%d")).days
    return res

def diffTime2(deltaTime):
    now = str(datetime.datetime.now().date())
    try:
        t = str(deltaTime).split('/')
        d1 = str(JalaliDate(int(t[0]), int(t[1]), int(t[2])).to_gregorian())
    except:
        t = str(deltaTime).replace('/','').replace('-','')
        t1 = t[:4]
        t2 = t[4:6]
        t3 = t[6:8]
        d1 = str(JalaliDate(int(t1), int(t2), int(t3)).to_gregorian())
    res = (datetime.datetime.strptime(now, "%Y-%m-%d") - datetime.datetime.strptime(d1, "%Y-%m-%d")).days
    return res

def timStumpTojalali(timeStump):
    kk = str(JalaliDate.fromtimestamp((int(timeStump)/1000)))
    kk = kk.replace('-','/')
    return kk


def dateSlashToInt(date):
    try:
        date = int(date.replace('/',''))
    except:
        date = 0
    return date


def dateToPriod (date):
    intDate = str(date).split('/')
    year = intDate[0]
    mondth = intDate[1].replace('02','اردیبهشت').replace('01','فروردین').replace('03','خرداد').replace('04','تیر').replace('05','مرداد').replace('06','شهریور').replace('07','مهر').replace('08','آبان').replace('09','آذر').replace('10','دی').replace('11','بهمن').replace('12','اسفند')
    return year +' '+mondth

def dateToPriodMonth (date):
    intDate = str(date).split('/')
    mondth = intDate[1].replace('02','اردیبهشت').replace('01','فروردین').replace('03','خرداد').replace('04','تیر').replace('05','مرداد').replace('06','شهریور').replace('07','مهر').replace('08','آبان').replace('09','آذر').replace('10','دی').replace('11','بهمن').replace('12','اسفند')
    return mondth

def dateToInt (date):
    intDate = str(date).split('/')
    return int(intDate[0]+intDate[1]+intDate[2])

def intToDate (date):
    intDate = str(date)
    return intDate[0:4]+'/'+intDate[4:6]+'/'+intDate[6:8]


def dateToStandard (date):
    intDate = str(date).split('/')
    if len(intDate[1])==1:
        intDate[1] = '0'+intDate[1]
    if len(intDate[2])==1:
        intDate[2] = '0'+intDate[2]
    return intDate[0]+'/'+intDate[1]+'/'+intDate[2]


def PriodStrToInt (date):
    intDate = str(date).split('-')
    try:
        year = intDate[0]
        mondth = intDate[1].replace('اردیبهشت','01').replace('فروردین','02').replace('خرداد','03').replace('تیر','04').replace('مرداد','05').replace('شهریور','06').replace('مهر','07').replace('آبان','08').replace('آذر','09').replace('دی','10').replace('بهمن','11').replace('اسفند','12')
        dataInt = (year+mondth).replace(' ','')
        dataInt = int(dataInt)
        return dataInt
    except:
        return date

def PriodStrToIntWDash (date):
    intDate = str(date).split('-')
    try:
        year = intDate[0]
        mondth = intDate[1].replace('فروردین','01').replace('اردیبهشت','02').replace('خرداد','03').replace('تیر','04').replace('مرداد','05').replace('شهریور','06').replace('مهر','07').replace('آبان','08').replace('آذر','09').replace('دی','10').replace('بهمن','11').replace('اسفند','12')
        dataInt = (year+'-'+mondth).replace(' ','')
        dataInt = str(dataInt)
        return dataInt
    except:
        return date

def PeriodAndForwardPeriodInt(date):
    intDate = str(date).split('-')
    year = intDate[0].replace(' ','')
    mondth = intDate[1].replace(' ','').replace('اردیبهشت','01').replace('فروردین','02').replace('خرداد','03').replace('تیر','04').replace('مرداد','05').replace('شهریور','06').replace('مهر','07').replace('آبان','08').replace('آذر','09').replace('دی','10').replace('بهمن','11').replace('اسفند','12')
    if mondth == '01':
        year = str(int(year) - 1)
        mondth = '12'
    else:
        mondth = str(int(mondth)-1)
        if len(mondth) == 1:
            mondth = '0' + mondth
    backward = year + mondth
    return backward

def DateToPeriodInt(date):
    intDate = str(date).split('/')
    try:
        if len(intDate[1])==1:
            intDate[1] = '0'+intDate[1]
        return int(intDate[0]+intDate[1])
    except:
        return 0

def PeriodIntToPeriodStr(date):
    dateStr = str(date)
    year = dateStr[0:4]
    mon = dateStr[4:6].replace('01','اردیبهشت').replace('02','فروردین').replace('03','خرداد').replace('04','تیر').replace('05','مرداد').replace('06','شهریور').replace('07','مهر').replace('08','آبان').replace('09','آذر').replace('10','دی').replace('11','بهمن').replace('12','اسفند')
    return year + ' - '+ mon

def FeeFeildStart(date,period):
    if(int(period)<=12):
        strDate = str(date)
        year = int(strDate[0:4])
        mon = int(strDate[4:6]) - int(period)
        if mon<=0:
            year = year - 1
            mon = mon + 12
        year = str(year)
        mon = str(mon)
        if len(mon)==1:
            mon = '0' + mon
        start = year + mon
    else:
        start = 0
    return start


def DiffTwoDateInt(end,start):
    de = str(end)
    de = JalaliDate(int(de[:4]), int(de[4:6]), int(de[6:8])).to_gregorian()
    ds = str(start)
    ds = JalaliDate(int(ds[:4]), int(ds[4:6]), int(ds[6:8])).to_gregorian()
    diff = de - ds
    return diff.days


def PersianToGregorian(x):
    year = int(str(x).split('/')[0])
    mont = int(str(x).split('/')[1])
    day = int(str(x).split('/')[2])
    return JalaliDate(year, mont, day).to_gregorian()

def PersianToGregorianWeek(x):
    calendar =  PersianToGregorian(x).isocalendar()
    year = calendar[0]
    mont = calendar[1]
    calendar = JalaliDate.to_jalali(datetime.datetime.strptime(str(year) + ' - ' + str(mont) + ' - 1', '%Y - %W - %w'))
    return str(calendar)

def PersianToGregorianMonth(x):
    year = int(str(x).split('/')[0])
    mont = int(str(x).split('/')[1])
    if len(str(mont))==1: mont = '0' + str(mont)
    calendar = str(year) + '/' + str(mont) + '/' + '01'
    return calendar

def ceil(f): 
  return int(f) + (1 if f-int(f) else 0)

def PersianToGregorianTrimester(x):
    year = int(str(x).split('/')[0])
    mont = ceil(int(str(x).split('/')[1])/3)
    if mont == 1: mont = '01'
    elif mont == 2: mont = '04'
    elif mont == 3: mont = '07'
    elif mont == 4: mont = '09'
    return str(year)+'/'+str(mont)+'/'+'01'

def PersianToGregorianSixter(x):
    year = int(str(x).split('/')[0])
    mont = ceil(int(str(x).split('/')[1])/6)
    if mont == 1: mont = '01'
    elif mont == 2: mont = '0'
    return str(year)+'/'+str(mont)+'/'+'01'


def PersianToGregorianYearter(x):
    calendar =  PersianToGregorian(x).isocalendar()
    year = calendar[0]
    return str(year)+'/'+'01'+'/'+'01'


def GregorianPlus(x,plus):
    return x + datetime.timedelta(days=plus)

def GregorianToPersian(x):
    return JalaliDate(x)

def Diff2DatePersian(x, y):
    base1 = JalaliDate(int(str(x).split('/')[0]), int(str(x).split('/')[1]), int(str(x).split('/')[2])).to_gregorian()
    base2 = JalaliDate(int(str(y).split('/')[0]), int(str(y).split('/')[1]), int(str(y).split('/')[2])).to_gregorian()
    diff = base1 - base2
    return (diff.days)

def PersianToTimetump (x):
    s = str(PersianToGregorian(x))
    s = time.mktime(datetime.datetime.strptime(s, "%Y-%m-%d").timetuple())*1000
    return s

def htmlcolor(r, g, b):
    def _chkarg(a):
        if isinstance(a, int): # clamp to range 0--255
            if a < 0:
                a = 0
            elif a > 255:
                a = 255
        elif isinstance(a, float): # clamp to range 0.0--1.0 and convert to integer 0--255
            if a < 0.0:
                a = 0
            elif a > 1.0:
                a = 255
            else:
                a = int(round(a*255))
        else:
            raise ValueError('Arguments must be integers or floats.')
        return a
    r = _chkarg(r)
    g = _chkarg(g)
    b = _chkarg(b)
    return '#{:02x}{:02x}{:02x}'.format(r,g,b)


def ColorRandom():
    r = random.randint(0,175)
    g = random.randint(0,175)
    b = random.randint(0,175)
    return htmlcolor(r, g, b)


def qestList(years,date,amount):
    gregorian = PersianToGregorian(date)
    lister = []
    for y in range(years):
        if y <5:
            gregorianNow = gregorian + datetime.timedelta(days=(365*int(y)))
            for m in range(int(amount)):
                if m>0:
                    gregorianNow = gregorianNow + relativedelta(months=int(12/m))
                lister.append(JalaliDate.to_jalali(gregorianNow.year, gregorianNow.month, gregorianNow.day).strftime("%Y/%m/%d"))
    return lister

def qestListNoLimet(years,date,amount,endDate):
    gregorian = PersianToGregorian(date)
    endDatea = PersianToGregorian(endDate)

    endDate = min(endDatea,PersianToGregorian(str(int(years) + int(date[0:4])) + date[4:]),datetime.datetime.now().date())
    df = pd.date_range(start=gregorian,end=endDate,freq= str(int(12/int(amount)))+'M').shift(-1, freq='M').sort_values().shift(int(str(gregorian).split('-')[-1]), freq='D')

    df = [str(GregorianToPersian(x)) for x in df]
    return df


def qestListNoLimetByTF(years,date,amount,endDate,to,frm):
    '''
    years:مدت بیمه نامه به سال
    date:تاریخ شروع بیمه نامه به شمسی
    amount:تعداد اقساط در سال
    endDate:تاریخ انقضا بیمه نامه به شمسی
    to:حداکثر تاریخ لیست قست به شمسی
    frm:حداقل تاریخ لیست قست به شمسی
    '''
    gregorian = PersianToGregorian(date) #تاریخ شروع بیمه نامه به میلادی
    endDatea = PersianToGregorian(endDate) #تاریخ انقضای بیمه نامه به میلادی
    if to ==False:
        endDate = min(endDatea,PersianToGregorian(str(int(years) + int(date[0:4])) + date[4:]),datetime.datetime.now().date())
    else:
        lto = PersianToGregorian(intToDate(to))
        endDate = min(endDatea,PersianToGregorian(str(int(years) + int(date[0:4])) + date[4:]),lto)
    df = pd.date_range(start=gregorian,end=endDate,freq= str(int(12/int(amount)))+'M').sort_values().shift(-1, freq='M').sort_values().shift(int(str(gregorian).split('-')[-1]), freq='D')
    if frm != False: 
        lfrm = pd.to_datetime(PersianToGregorian(intToDate(frm)))
        df = pd.DataFrame({'تاریخ': df})
        df = df[df['تاریخ'] > lfrm]
        df = [str(GregorianToPersian(x)) for x in df['تاریخ']]
    else:
        df = [str(GregorianToPersian(x)) for x in df]
    return df

def dateToMonthDay(date):
    date = str(date).split('/')
    md = date[1]+date[2]
    md = int(md)




def CheckPhone(phone):
    phone = str(phone)
    if len(phone) == 10 and phone[0]!=0:
        phone = '0'+phone
    if len(phone)!=11:
        return {'replay':False}
    return {'replay':True,'phone':phone}
