from req_py_libraries import *

# dump data to json file


def process_data(tshares,fuid,rdate,coll,coll_name):
    try:
        coll['fuid_'+str(fuid)]
    except:
        coll['fuid_'+str(fuid)] = {}

    coll['fuid_'+str(fuid)]['fdate_'+str(rdate)] = tshares
    dump_json(data=coll,path=coll_name)



def setup_process_data(fuid,df,rdate,suid_df):
    df = df[(df.sshprn_type == 'SH') & (df.put_call == '_') & (df.class_title != 'OPTIONS') & (
                df.class_title != 'CALL') & (df.class_title != 'PUT')]
    df = df[~df.class_title.str.contains('bond', case=False)]
    df = df[~df.class_title.str.contains('note', case=False)]
    df = df[~df.class_title.str.contains('debt', case=False)]
    cusips = df.cusip.unique()
    for cusip in cusips:
        tdf = df[(df.cusip == cusip)]
        tshares=pd.to_numeric(tdf.sshprn).sum()

        t_stock = suid_df[(suid_df.cusip_sedol == cusip) | (suid_df.cusip == cusip)]

        if len(t_stock):
            directory = '/scry/inst_inv/'
            t_stock_uid = t_stock.iloc[0]['uid']

            coll_name = directory + str(t_stock_uid) + '.json'
            try:
                coll = read_json(coll_name)
            except:
                with open(coll_name, 'w') as outfile:
                    json.dump({}, outfile)
                coll = read_json(coll_name)
            process_data(int(tshares), fuid, rdate, coll, coll_name)

def insti_process2(rdate,insti_cik,df,uid_df):
    directory = '/scry/stocks_usa/funds.json'
    funds = read_json(directory)
    insti_cik = str(int(insti_cik))
    fuid = [x['UID'] for x in funds if x['content']['cik'].find(insti_cik)==0]
    rdate = datetime.strptime(rdate, '%m-%d-%Y').date()

    if len(fuid) == 0:
        momoflag = False
    else:

        fuid = fuid[0]

        setup_process_data(fuid=fuid, df=df, rdate=rdate, suid_df=uid_df)
        momoflag = True

    return momoflag

