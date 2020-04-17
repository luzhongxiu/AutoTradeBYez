import tushare as ts
from sqlalchemy import *
import pandas as pd


# 把df导入到sql的tablename中
def df_sql(tablename, df, **kwargs):
    con = create_engine('sqlite:////Users/luzhongxiu/documents/database/tushare.db', encoding='utf-8')
    conn = con.connect()
    df.to_sql(tablename, conn, if_exists="replace", index=False)
    conn.close()
    con.dispose()


# 从tablenanme中导出df
def get_table(tablename):
    con = create_engine('sqlite:////Users/luzhongxiu/documents/database/tushare.db', encoding='utf-8')
    conn = con.connect()
    cmd = "SELECT * FROM " + tablename
    df = pd.read_sql(cmd, conn)
    conn.close()
    con.dispose()
    return df


# 判断差值
def gap_number(number, **kwargs):
    df_tushare = ts.pro_bar(ts_code='600050.SH', start_date='2018-01-01 09:00:00', end_date='2020-01-01 09:00:00', freq="15min")
    df_tushare = df_tushare.iloc[::-1]
    rows = df_tushare.shape[0]
    number = 0
    for i in range(rows-1):
        gap = (float(df_tushare.iloc[i]["close"])-float(df_tushare.iloc[i+1]["close"]))/float(df_tushare.iloc[i]["close"])
        if gap >= abs(0.01):
            number = number + 1
    print(number)


# 计算利润
def cac_prof(df_tushare):
    rows = df_tushare.shape[0]
    print(rows)
    number = 0
    number1 = 0
    prof = 0
    i = 0
    for j in range(i+1, rows-i):
        # gap_percent = (float(df_tushare.iloc[j]["close"])-float(df_tushare.iloc[i]["close"]))/float(df_tushare.iloc[i]["close"])
        gap = float(df_tushare.iloc[j]["open"])-float(df_tushare.iloc[i]["open"])
        if gap >= 0.1:
            i = j
            number1 += 1
            prof = prof + int(gap/0.1*300/100)*100*0.1
        if gap <= -0.1:
            i = j
    print(number1)
    print(prof)


# 计算chinaunicom的利润
def get_df():
    df = get_table("chinaunicom")
    df = df[(df["trade_time"].str.contains(pat="2018"))]
    # print(df)
    i = 0
    number = 0
    number1 = 0
    prof = 0
    while i < df.shape[0]:
        for j in range(i+1, df.shape[0]):
            gap = float(df.iloc[j]["open"])-float(df.iloc[i]["open"])
            if gap >= 0.07:
                number = number+1
                i = j
                prof = prof + int(gap/0.1*300/100)*100*0.1*0.86
                print(prof)
                break
            if gap <= -0.09:
                number1 += 1
                prof = prof - int(gap/0.1*300/100)*100*float(df.iloc[j]["open"])*0.0014
                i = j
                break
            if j == df.shape[0]-1:
                return


def calculate_prof(stock_number, init_position, balance, ):
    pass


def select_stock_amp(amp, **kwargs):
    pass


def main():
    # 筛选回落较小，股价小幅度波动次数较多的股票/etf
    # select_stock(gap_percent, amp, **kwargs)
    #
    # calculate_prof(stock_number, init_position, balance, )
    #
    # #ezquotation
    # get_info()
    #
    # #eztrader
    # eztrader()
    pass


def avg_open(ts, ts_code, startdate, enddate,**kwargs):
    df = ts.daily(ts_code=ts_code, start_date=startdate, end_date=enddate)
    avg = float(df["open"].mean())
    return avg


def max_open(ts, ts_code, startdate, enddate,**kwargs):
    df = ts.daily(ts_code=ts_code, start_date=startdate, end_date=enddate)
    max = float(df["open"].max())
    return max


def min_open(ts, ts_code, startdate, enddate,**kwargs):
    df = ts.daily(ts_code=ts_code, start_date=startdate, end_date=enddate)
    min = float(df["open"].min())
    return min


def retreat(maxprice, minprice,avg):
    retreat = (maxprice - minprice)/avg*100
    return retreat


def amp(avgprice, gap):
    amp = (gap - avgprice)/gap*100
    return amp


# 寻找至为止的差值
def find_stock_amp(ts):
    df_all = ts.stock_basic(is_hs="", exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    list1 = []
    for i in range(df_all.shape[0]-1):
        stock_number = df_all.iloc[i]["ts_code"]
        df = ts.daily(ts_code=stock_number, start_date="20180101", end_date="20200412")
        avg = avg_open(ts, stock_number, "20180101", "20200410")
        maxprice = max_open(ts, stock_number, "20180101", "20200410")
        minprice = min_open(ts, stock_number, "20180101", "20200410")
        temp = [stock_number, avg, maxprice, minprice]
        list1.append(temp)
        print("正常吗", i)
    df = pd.DataFrame(list1, columns=["stocknumber", "avg", "maxprice", "minprice"])
    df_sql("ALLamp", df)
    return list


# 计算差距波动差gap_rate%的次数
def cal_wave_time(df, gap_rate):
    i = 0
    number = 0
    for j in range(i+1, df.shape[0]):
        rate = (df.iloc[j]["open"]-df.iloc[i]["open"])/df.iloc[i]["open"]
        if j == df.shape[0]-1:
            return number
        if abs(rate) >= gap_rate:
            number = number + 1
            i = j
        else:
            pass
    return number


# 计算所有的wavetime
def all_wavetime():
    df = get_table("merge_amp")
    list_number = []
    for i in range(df.shape[0]):
        ts_code = df.iloc[i]["ts_code"]
        df1 = ts.pro_bar(ts_code, start_date="2018-01-01 09:00:00", end_date="2020-04-10 09:00:00", freq="D")
        temp = [ts_code, cal_wave_time(df1, 0.01)]
        print(i, temp)
        list_number.append(temp)

    df_final = pd.DataFrame(list_number, columns=["ts_code", "wave_time"])
    df_sql("all_wavetime", df_final)


def cal_profit(df,total):
    profit = total/df["avg"]*0.01*df["wave_time"]
    df["profit"] = profit
    return df





