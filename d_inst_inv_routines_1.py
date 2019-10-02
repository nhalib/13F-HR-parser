#separate routine needed later on to truncate collection-documents
from req_py_libraries import *


def update_latest_filing(fcik,rdate):
    fcik = str(int(fcik))
    rdate = datetime.strptime(rdate, '%m-%d-%Y').date()

    directory = '/scry/stocks_usa/funds.json'
    funds = read_json(directory)


    for iter,fund in enumerate(funds):
        if fund['UID'] == str(fcik):
            if fund['content']['latest'] == None:
                funds[iter]['content']['latest'] = str(rdate)
            else:
                cdate = datetime.strptime(fund['content']['latest'],'%m-%d-%Y').date()
                if cdate < rdate:
                    funds[iter]['content']['latest'] = str(rdate)

    dump_json(directory,funds)

def top_clean_up_sfuid(filing_cik,fund_latest):
    fund_latest = datetime.strptime(fund_latest, '%m-%d-%Y').date()
    filing_cik = str(int(filing_cik))
    directory = '/scry/stocks_usa/funds.json'
    funds = read_json(directory)
    fuid = False

    directory = '/scry/stocks_usa/exch_tick_cik.json'
    suids = read_json(directory)
    suids = [x['UID'] for x in suids if x['content']['sector'].find('semiconductor') >= 0]

    for iter, fund in enumerate(funds):
        if fund['content']['cik'] == filing_cik:
            fuid = fund['UID']

            for val in suids:
                directory = '/scry/inst_inv/'
                coll_name = directory + str(val) + '.json'
                suid_inst_data = read_json(coll_name)

                try:
                    doc = suid_inst_data['fuid_' + str(fuid)]
                    try:
                        val = float(doc['fdate_' + str(fund_latest)])  # check if entry for latest valid filing exists
                    except:
                        suid_inst_data['fuid_' + str(fuid)]['fdate_' + str(fund_latest)] = 0
                        dump_json(path=coll_name, data=suid_inst_data)
                except:
                    pass


            break

    return fuid

