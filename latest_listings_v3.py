#author : Nikhil N.
#grab latest filed listings on SEC site and parse them if 13F-HR or 13F-HR/A
# run as crondb

from req_py_libraries import *
from sec_data_class import *


def url_response(url):
    boolman = False
    try:
        response = requests.get(url)
        response = response.content.decode('utf-8')
        boolman = True
    except ValueError:
        response = 'Error 101.Page not readable'
    return [boolman, response]

def find_filings(class_var,link):
    base_url = "https://www.sec.gov"

    response = url_response(link)
    response = BeautifulSoup(response[1],'html.parser')
    docs = response.find_all("a")
    count = 0
    for doc in docs:
        ftype = doc.get_text()
        ftype = re.compile(ftype)
        if ftype.match('13F-HR') or ftype.match('13F-HR/A'):
            flink = base_url + doc['href']
            class_var.process_13F(flink)


def main_routine():
    x = sec_data()
    inst_link = 'https://www.sec.gov/cgi-bin/current?q1=0&q2=6&q3='
    find_filings(class_var=x, link=inst_link)

