from req_py_libraries import *

#load json file
def read_json(path):
    with open(path) as json_data:
        data = json.load(json_data)
        json_data.close()
    return data

# dump data to json file
def dump_json(path,data):
    with open(path, 'w') as outfile:
        json.dump(data, outfile)


def setup_insti_heat(suid_list):
    udirectory = '/scry/stocks_usa/exch_tick_cik.json'
    stock_db = read_json(path=udirectory)
    for suid in suid_list:
        try:
            directory = '/scry/securities/'+str(suid)+'/inst_inv.json'
            inst_data = read_json(directory)
            cont_flag = True
        except:
            cont_flag = False
        if cont_flag:
            rslt = inst_data
            if len(rslt) > 0:
                os = float(rslt[0]['content']['os'])
                if os > 0:
                    inc_frac = float(rslt[0]['content']['inc_amnt'])/os
                    dec_frac = float(rslt[0]['content']['dec_amnt'])/os
                    close_frac = float(rslt[0]['content']['close_amnt'])/os
                    new_frac = float(rslt[0]['content']['new_amnt'])/os
                    inc_pos = float(rslt[0]['content']['inc_pos'])
                    dec_pos = float(rslt[0]['content']['dec_pos'])
                    new_pos = float(rslt[0]['content']['new_pos'])
                    close_pos = float(rslt[0]['content']['close_pos'])
                    present_pos = float(rslt[0]['content']['present_pos'])
                    same_pos = float(rslt[0]['content']['same_pos'])
                    tpos = inc_pos + dec_pos + new_pos + close_pos + same_pos
                    p1 = 1 - np.exp(-4 * (present_pos/tpos))
                    p2 = 1 - np.exp(-1.5 * (inc_pos/tpos) * inc_frac)
                    p3 = 1 - np.exp(-2.5 * (new_pos/tpos) * new_frac)
                    p4 = np.exp(-2 * (dec_pos/tpos) * dec_frac)
                    p5 = np.exp(-2.5 * (close_pos/tpos) * close_frac)
                    score = (0.5*p1 + 0.125*p2 + 0.125*p3 + 0.125*p4 + 0.125*p5)/1
                    heat = format(score,'.2f')
                    tgt = [i for i, d in enumerate(stock_db) if d['UID'] == int(suid)]
                    stock_db[tgt[0]]['content']['insti_heat'] = heat
                else:
                    inc_pos = float(rslt[0]['content']['inc_pos'])
                    dec_pos = float(rslt[0]['content']['dec_pos'])
                    new_pos = float(rslt[0]['content']['new_pos'])
                    close_pos = float(rslt[0]['content']['close_pos'])
                    same_pos = float(rslt[0]['content']['same_pos'])
                    tpos = inc_pos + dec_pos + new_pos + close_pos + same_pos

                    p2 = 1 - np.exp(-1.5 * (inc_pos / tpos))
                    p3 = 1 - np.exp(-2.5 * (new_pos / tpos))
                    p4 = np.exp(-2 * (dec_pos / tpos))
                    p5 = np.exp(-2.5 * (close_pos / tpos))
                    score = ( 0.125 * p2 + 0.125 * p3 + 0.125 * p4 + 0.125 * p5) / 1
                    heat = format(score, '.2f')
                    tgt = [i for i, d in enumerate(stock_db) if d['UID'] == int(suid)]
                    stock_db[tgt[0]]['content']['insti_heat'] = heat
    dump_json(path=udirectory,data=stock_db)