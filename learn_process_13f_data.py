#author : nikhil.n
# version 1.0
# parse out the Form 4 details

from req_py_libraries import *
from d_allocate_insti_v2 import insti_process2
from d_inst_inv_routines_1 import update_latest_filing,top_clean_up_sfuid
from d_inst_inv_routines_2 import couple_0
from f13_company_listings import listings_routine
#data already passes in as

def url_response(url):
    boolman = False
    try:
        response = requests.get(url)
        response = response.content.decode('utf-8')
        boolman = True
    except ValueError:
        response = 'Error 101.Page not readable'
    return [boolman, response]


def f13parser(links,uid_df):
    try:
        link = links.split(';')[0]
        primary_link = links.split(';')[1]
    except:
        link = ''
        primary_link = ''
    final_flag = True
    error_flag = False
    filing_cik = 0
    statman = False
    rdate = str(datetime.now().date())
    try:
        url_resp = url_response(link)
        prime_resp = url_response(primary_link)
    except:
        #print('no information table')
        error_flag = True
    if link == '' or primary_link == '':
        error_flag = True
    if not error_flag:
        data = xmltodict.parse(url_resp[1])
        samp = data.keys()
        samp = list(samp)[0]
        samp = samp.split(':')
        if len(samp)>1:
            v1 = samp[0]+':'+'informationTable'
            v11 = samp[0] + ':'+'infoTable'
            data = data[v1][v11]
            noI = samp[0]+':'+'nameOfIssuer'
            toC = samp[0]+':'+'titleOfClass'
            cus = samp[0]+':'+'cusip'
            m = samp[0]+':'+'shrsOrPrnAmt'
            m1 = samp[0]+':'+'sshPrnamt'
            m2 = samp[0]+':'+'sshPrnamtType'
            pc = samp[0]+':'+'putCall'
        elif len(samp) == 1:
            v1 = 'informationTable'
            v11 = 'infoTable'
            data = data[v1][v11]
            noI = 'nameOfIssuer'
            toC = 'titleOfClass'
            cus = 'cusip'
            m = 'shrsOrPrnAmt'
            m1 = 'sshPrnamt'
            m2 = 'sshPrnamtType'
            pc = 'putCall'



        data1 = xmltodict.parse(prime_resp[1])
        filing_cik = data1['edgarSubmission']['headerData']['filerInfo']['filer']['credentials']['cik']
        fund_name = data1['edgarSubmission']['formData']['coverPage']['filingManager']['name']
        rdate = data1['edgarSubmission']['formData']['coverPage']['reportCalendarOrQuarter'] #filing date
        da_date = data1['edgarSubmission']['formData']['signatureBlock']['signatureDate']

        try:
                row_list = []
                for uni in data:
                    dict1 = {}

                    dict1['issuer'] =uni[noI]
                    dict1['class_title'] = uni[toC]
                    cusip_num = uni[cus]
                    dict1['cusip'] = cusip_num[0:6]
                    dict1['sshprn'] = uni[m][m1]
                    dict1['sshprn_type'] = uni[m][m2]
                    try:
                        dict1['put_call'] = uni[pc]
                    except:
                        dict1['put_call'] = '_'
                    row_list.append(dict1)

                final_flag = True


        except:
                row_list = []
                uni = data
                dict1 = {}
                dict1['issuer'] = uni[noI]
                dict1['class_title'] = uni[toC]
                cusip_num = uni[cus]
                dict1['cusip'] = cusip_num[0:6]
                dict1['sshprn'] = uni[m][m1]
                dict1['sshprn_type'] = uni[m][m2]
                try:
                    dict1['put_call'] = uni[pc]
                except:
                    dict1['put_call'] = '_'
                row_list.append(dict1)

                final_flag = True
                if dict1['sshprn'] == '0':
                    final_flag = False

        if final_flag:
            df = pd.DataFrame(row_list)
            statman = True
            statman = insti_process2(rdate,filing_cik,df,uid_df)
    return(final_flag,filing_cik,rdate,statman,da_date)

def setup_uid_df():
    directory = '/scry/stocks_usa/exch_tick_cik.json'
    stocks = read_json(directory)
    stocks = [[x['UID'],x['content']['cusip'],x['content']['cusip_sedol']] for x in stocks if x['content']['sector'].find('semiconductor')>=0]
    flist = []
    for val in stocks:
        dict1 = {}
        dict1['uid'] = val[0]
        dict1['cusip'] = val[1]
        dict1['cusip_sedol'] = val[2]
        flist.append(dict1)
    df = pd.DataFrame(flist)
    return(df)

def daily_13f(type=1):
    uid_df = setup_uid_df()
    fuid_list = []

    directory = '/scry_be/to_process/process_13hr.json'
    new_invs = read_json(directory)

    for iter, val in enumerate(new_invs):
        parse_stat = False
        try:
            if True:
                if (new_invs[iter]['content']['stat'] == 0) or (new_invs[iter]['content']['stat'] == 1):
                    link = new_invs[iter]['content']['link']
                    [parse_stat, filing_cik, rdate, proceed, da_date] = f13parser(link, uid_df)
                    if proceed:
                        update_latest_filing(fcik=filing_cik, rdate=rdate)
                        fuid = top_clean_up_sfuid(filing_cik=filing_cik, fund_latest=rdate)
                        if fuid:
                            fuid_list.append(fuid)
        except:
            parse_stat = False
        if parse_stat:
            new_invs[iter]['content']['stat'] = 1

        if iter % 1000 == 0:
            temp_fuids = read_json(path='be/fuid_list.json')
            for temp_fuid in fuid_list:
                temp_fuids.append(temp_fuid)
            dump_json(path='be/fuid_list.json',data=temp_fuids)
            fuid_list = [] # clear out buffer

    dump_json(path=directory, data=new_invs)


def daily_13f_b():
    #update status of institutional investments  in all (affected) stocks from current line of process_13hr
    couple_0()

    #after all processes , clean out be/fuid_list.json
    dump_json(path='be/fuid_list.json', data=[])


def main_13hr():
    listings_routine()
    #print('Completed getting files')
    daily_13f(type=1)
    print('Completed processing')
    daily_13f_b()

main_13hr()