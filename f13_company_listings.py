#Fetch company filings from SEC as .atom file
from req_py_libraries import *
from sec_data_class import sec_data


def hr13_scrape_routine(class_var,cik,cutoff):
    super_flag = True
    ll = 0
    ul = 100
    filing_list = []
    while super_flag:
        link = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK="
        l3 = "&type="
        form = "13F-HR%25"
        l4 = "&dateb=&owner=include&"
        l5 = "start="
        l6 = str(ll)
        l7 = "&count="
        l8 = str(ul)
        l9 = "&output=atom"
        link = link + cik + l3 + form + l4 + l5 + l6 + l7 + l8 + l9
        #datec = datetime.strptime(cutoff, "%Y-%m-%d").date()
        datec = cutoff
        feed = feedparser.parse(link)
        feed = feed['entries']
        for i in range(0,len(feed)):
                datef = feed[i]['filing-date']
                datef = datetime.strptime(datef, "%Y-%m-%d").date()
                ftype = feed[i]['filing-type']
                flink = feed[i]['filing-href']
                if (datef>datec and ftype =='13F-HR'):
                    filing_list.append(flink)
                    #super_flag = False
                if (datef > datec and ftype == '13F-HR/A'):
                    filing_list.append(flink)
                    #super_flag = False
                    #class_var.process_13F(flink,cursor)
                elif (datef<datec and ftype == '13F-HR'):
                    super_flag = False
                    break
        if len(feed) == 0:
            super_flag = False
        ll = ll + 100

    filing_list = reversed(filing_list)  # direction of time
    for filing in filing_list:
        try:
            class_var.process_13F(filing)
        except:
            pass

def listings_routine():
    directory = '/scry_be/to_process/process_13hr.json'
    dump_json(path=directory,data=[]) # truncate for cumul parsing


    directory = '/scry/stocks_usa/funds.json'
    funds = read_json(directory)

    x = sec_data()
    cutoff = datetime.strptime('2019-01-01','%Y-%m-%d').date()
    for val in funds:
        cik = val['content']['cik']
        hr13_scrape_routine(x,cik,cutoff)