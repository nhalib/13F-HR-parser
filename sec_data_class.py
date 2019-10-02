#version : 1
#author : Nikhil N
#SCRY internal use only

#download financial statements and save temp to local directory

# Subject to modifications ;Lookout for changes in formatting of index page
# f4,13f,10Q
# SEC DATA extraction class


from req_py_libraries import *


class sec_data():

    def __init__(self):
        self.base_url = "https://www.sec.gov"

    def finstatement_urls(self,cik):
            link = "/Archives/edgar/data/" +cik+ "/"
            return(link)

    def main_filed_page(self,cik,acc_num): # main page of filed_document
            p1 = "https://www.sec.gov/Archives/edgar/data/"
            p2 = cik
            p3 = "/"
            p4 = acc_num.replace("_","")
            p5 = "/"
            p6 = acc_num
            p7 ="-index.htm"
            main_page = p1 + p2 + p3 + p4 + p5 + p6 + p7
            return(main_page)


    def url_response(self,url):
        boolman = False
        try:
            http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())  # Certified Websites filter
            response = http.request('GET', url)
            if (str(response.getheader('Content-Type')).find('html') >= 0):
                boolman = True
        except ValueError:
            response = 'Error 101.Page not readable'
        return [boolman, response]



    def feed_to_f13db(self,link):
        directory = '/scry_be/to_process/process_13hr.json'
        to_process = read_json(directory)

        if len(to_process) == 0:
            temp = {}
            temp['UID'] = 0
            temp['content'] = {}
            temp['content']['link'] = link
            temp['content']['stat'] = 0
        else:
            latest = to_process[-1]['UID']
            temp = {}
            temp['UID'] = latest + 1
            temp['content'] = {}
            temp['content']['link'] = link
            temp['content']['stat'] = 0
        to_process.append(temp)
        dump_json(path=directory,data=to_process)



    def process_13F(self,flink,type=1):
        url = flink
        flag,responseu = self.url_response(url)
        link = ''
        if(flag):
            response1 = BeautifulSoup(responseu.data, "html.parser")
            response1 = response1.find("table",{"summary":"Document Format Files"})
            responses = response1.find_all("td", string="INFORMATION TABLE")
            for response in responses:
                responsel = response.parent
                link_responses = responsel.find_all("a")
                for resp in link_responses:
                    text = resp.get_text()
                    text_eval = text.lower()
                    if (text_eval.endswith(".xml")):
                        link = self.base_url + resp['href']
                        break
            response1 = BeautifulSoup(responseu.data, "html.parser")
            response1 = response1.find("table", {"summary": "Document Format Files"})
            responses = response1.find_all("td", string="13F-HR")
            for response in responses:
                responsel = response.parent
                link_responses = responsel.find_all("a")
                for resp in link_responses:
                    text = resp.get_text()
                    text_eval = text.lower()
                    if (text_eval.endswith(".xml")):
                        link = link + ';'+self.base_url + resp['href']
                        break
            responses = response1.find_all("td", string="13F-HR/A")
            for response in responses:
                responsel = response.parent
                link_responses = responsel.find_all("a")
                for resp in link_responses:
                    text = resp.get_text()
                    text_eval = text.lower()
                    if (text_eval.endswith(".xml")):
                        link = link + ';'+self.base_url + resp['href']
                        break

            try:
                if type ==1:
                    self.feed_to_f13db(link)

            except:
                print('13F-HR cannot be processed')