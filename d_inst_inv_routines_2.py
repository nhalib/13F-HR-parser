#get the top 10 funds by shares held in the stock , and stats on inst_inv :
# increase,decrease in positions (number of funds who have increased and decreased positions)
# new positions , closed positions

from req_py_libraries import *

def get_stocks():
    directory = '/scry/stocks_usa/exch_tick_cik.json'
    suids = read_json(directory)
    suids = [x['UID'] for x in suids if x['content']['sector'].find('semiconductor') >= 0]
    return suids

def get_latest_assets(suid):
    directory = '/scry/securities/'+str(suid)+'/assets.json'
    assets = read_json(directory)
    bobo = {}
    for date in assets.keys():
        try:
            tdate = datetime.strptime(date,'%Y-%m-%d').date()
            bobo[tdate] = assets[date]['q']['os']
        except:
            pass
    try:
        top_date = sorted(bobo)[-1]
        return bobo[top_date]
    except:
        return False

def core_process_1(suid,coll,tdate):
    present_pos = 0
    inc_pos = 0
    inc_amnt = 0
    dec_pos = 0
    dec_amnt = 0
    new_pos = 0
    new_amnt = 0
    close_pos = 0
    close_amnt = 0
    same_pos = 0
    fund_list = []

    for key in coll.keys():
        d1 = {}

        elist = []
        cont0 = False
        resps = coll[key]
        for fdate in resps.keys():
            ddict1 = {}
            ddict1['position'] = int(resps[fdate])
            ddict1['filing_date'] = datetime.strptime(fdate.replace('fdate_', ''), '%Y-%m-%d').date()
            elist.append(ddict1)
            cont0 = True
        if cont0:
            df = pd.DataFrame(elist)
            df = df.sort_values(by='filing_date', ascending=False)

            cont = True

            if cont:

                try:
                    l0 = int(df.iloc[1, 1])  # latest-1 position # if doesn't exist
                except:
                    l0 = 0
                l1 = int(df.iloc[0, 1])  # latest position
                diff = l1 - l0
                if diff > 0 and l0 == 0:  # new position w.r.t previous quarter
                    new_pos = new_pos + 1
                    new_amnt = new_amnt + l1
                    present_pos = present_pos + l1
                if diff > 0:  # increase in shares
                    present_pos = present_pos + l1
                    inc_pos = inc_pos + 1
                    inc_amnt = inc_amnt + diff

                if diff < 0:  # decrease or closing position
                    present_pos = present_pos + l1
                    if l1 == 0:
                        close_pos = close_pos + 1
                        close_amnt = close_amnt + abs(diff)
                        dec_pos = dec_pos + 1
                        dec_amnt = dec_amnt + abs(diff)
                    else:
                        dec_pos = dec_pos + 1
                        dec_amnt = dec_amnt + abs(diff)
                if diff == 0:  # no change
                    present_pos = present_pos + l1
                    same_pos = same_pos + 1
        d1['fuid'] = key.replace('fuid_', '')
        d1['position'] = l1
        fund_list.append(d1)

    directory = '/scry/stocks_usa/funds.json'
    funds = read_json(directory)  # read all inst_inv funds

    if cont0:
        top_10_list = []
        try:
            tdirectory = '/scry/securities/'
            coll_name = tdirectory + str(suid) + '/inst_inv.json'
            suid_inst_data = read_json(coll_name)
            break_out = False
        except:
            print(suid, 'db missing')
            break_out = True
        if not break_out:
            fund_list = pd.DataFrame(fund_list)
            fund_list = fund_list.sort_values(by='position', ascending=False)
            fund_list = fund_list.iloc[0:10]
            for index, row in fund_list.iterrows():
                fuid = row['fuid']
                fname = [x['content']['fund_name'] for x in funds if x['UID'] == int(fuid)]
                top_10_list.append(str(fname[0]) + ':' + str(fuid))
            top_10 = '|'.join(x for x in top_10_list)

            os = get_latest_assets(suid=suid)

            if os:
                suid_inst_data[str(tdate)] ={}
                temp = {}
                temp['os'] = str(os)
                temp['inc_pos'] = str(inc_pos)
                temp['inc_amnt'] = str(inc_amnt)
                temp['dec_pos'] = str(dec_pos)
                temp['dec_amnt'] = str(dec_amnt)
                temp['close_pos'] = str(close_pos)
                temp['close_amnt'] = str(close_amnt)
                temp['new_pos'] = str(new_pos)
                temp['new_amnt'] = str(new_amnt)
                temp['same_pos'] = str(same_pos)
                temp['present_pos'] = str(present_pos)
                temp['top_10'] = str(top_10)
                suid_inst_data[str(tdate)] = temp

                dump_json(path=coll_name, data=suid_inst_data)


def base(suid_list,tdate=datetime.now().date()):

    for suid in suid_list:
        try:
            directory = '/scry/inst_inv/'
            coll_name = directory + str(suid) + '.json'
            suid_inst_data = read_json(coll_name)
            proceed_flag = True
        except:
            proceed_flag = False

        if proceed_flag:
            core_process_1(suid=suid,coll=suid_inst_data,tdate=tdate)


def couple_0():

    fuid_list = read_json(path='be/fuid_list.json')
    suids = get_stocks()
    suid_list = set()
    for fuid in fuid_list:
        for suid in suids:
            directory = '/scry/inst_inv/'
            coll_name = directory + str(suid) + '.json'
            try:
                suid_inst_data = read_json(coll_name)
                doc = suid_inst_data['fuid_'+str(fuid)] # if fuid is present in the stock
                suid_list.add(suid)
            except:
                pass

    base(suid_list)



