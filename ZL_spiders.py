import requests
from urllib.parse import urlencode
import pymongo
import threading
from queue import Queue

MAX_PAGE = '48'


class GetUrl(threading.Thread):
    def __init__(self, url_queue, html_queue):
        super().__init__()
        self.url_queue = url_queue
        self.html_queue = html_queue
        self.headers = {
            'Accept':  'application/json, text/plain, */*',
            # 'Accept-Encoding': 'gzip, deflate, br',  # 压缩导致乱码情况
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Cookie': 'adfbid2=0; sts_deviceid=16541dfca2c6a2-0769ca14588327-2711639-2073600-16541dfca2d6f9; _jzqx=1.1534408510.1534408510.1.jzqsr=hao123%2Ecom|jzqct=/link/https/.-; dywec=95841923; adfbid=0; sts_sg=1; sts_chnlsid=Unknown; zp_src_url=https%3A%2F%2Fwww.baidu.com%2Flink%3Furl%3DPXCfK894pRQ3pCnJG5dYKjtz7VkBOt_1ZXLrGEJZynnkJOpydvbqAwA_mfZ_WdGG%26wd%3D%26eqid%3D8ba346dc0002e595000000035be3c812; __utmc=269921210; ZP_OLD_FLAG=false; dywea=95841923.1440329155479984400.1534408510.1541734627.1543473992.6; sts_sid=1675e3810f0262-0c8d620437ae5d-2711639-2073600-1675e3810f21358; _jzqa=1.4372173638961846000.1534408510.1534408510.1543473992.2; _jzqc=1; _jzqy=1.1543473992.1543473992.1.jzqsr=baidu|jzqct=%E6%99%BA%E8%81%94%E6%8B%9B%E8%81%98.-; _jzqckmp=1; __xsptplus30=30.2.1543473992.1543473992.1%232%7Csp0.baidu.com%7C%7C%7C%25E6%2599%25BA%25E8%2581%2594%25E6%258B%259B%25E8%2581%2598%7C%23%23T11ExXGtfLji8XycqLf48G31U7VV9mXL%23; qrcodekey=a386f29df8bb4ed2a01bc2c95bc3cd20; _jzqb=1.2.10.1543473992.1; firstchannelurl=https%3A//passport.zhaopin.com/login; lastchannelurl=https%3A//ts.zhaopin.com/jump/index_new.html%3Futm_source%3Dbaidupcpz%26utm_medium%3Dcpt%26utm_provider%3Dpartner%26sid%3D121113803%26site%3Dnull; jobRiskWarning=true; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22166f1c59625293-05eaf210974b09-2711639-2073600-166f1c596262f0%22%2C%22%24device_id%22%3A%22166f1c59625293-05eaf210974b09-2711639-2073600-166f1c596262f0%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E8%87%AA%E7%84%B6%E6%90%9C%E7%B4%A2%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22https%3A%2F%2Fwww.baidu.com%2Flink%22%2C%22%24latest_referrer_host%22%3A%22www.baidu.com%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC%22%2C%22%24latest_utm_source%22%3A%22baidupcpz%22%2C%22%24latest_utm_medium%22%3A%22cpt%22%7D%7D; dywez=95841923.1543474016.6.5.dywecsr=baidu|dyweccn=(organic)|dywecmd=organic; __utma=269921210.1976649304.1534408510.1543473992.1543474016.7; __utmz=269921210.1543474016.7.5.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; urlfrom=121114583; urlfrom2=121114583; adfcid=www.baidu.com; adfcid2=www.baidu.com; Hm_lvt_38ba284938d5eddca645bb5e02a02006=1541654550,1543473992,1543474016; dyweb=95841923.5.10.1543473992; __utmb=269921210.3.10.1543474016; LastCity=%E5%85%A8%E5%9B%BD; LastCity%5Fid=489; ZL_REPORT_GLOBAL={%22sou%22:{%22actionid%22:%22c1aab1c7-f477-49fd-8bc6-95c7d4a785d3-sou%22%2C%22funczone%22:%22smart_matching%22}%2C%22//jobs%22:{%22actionid%22:%222460962c-c399-44c2-95f4-04325362d325-jobs%22%2C%22funczone%22:%22dtl_best_for_you%22}}; GUID=998d053ee2f24890a209c29c68caedb5; Hm_lpvt_38ba284938d5eddca645bb5e02a02006=1543474438; sts_evtseq=27',
            'Host': 'fe-api.zhaopin.com',
            'Origin': 'https://sou.zhaopin.com',
            'Referer': 'https://sou.zhaopin.com/?p=49&jl=489&kw=%E7%88%AC%E8%99%AB&kt=3',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
            }

    def run(self):
        while True:
            if self.url_queue.empty():
                break
            url = self.url_queue.get()
            self.get_html(url)


    def get_html(self, page):
        '''
        拼接网址，并请求以json格式返回
        :return:
        '''
        response = requests.get(url=page, headers=self.headers)
        self.parse_html(response.json())

    def parse_html(self, json):
        '''
        解析返回内容
        :param json:
        :return:
        '''
        if json:
            items = json.get('data').get('results')    # results节点
            for item in items:
                result = {}
                result['city'] = item.get('city').get('items')[0].get('name')  # 城市
                result['company'] = item.get('company').get('name')             #   公司名称
                result['type'] = item.get('company').get('type').get('name')   # 公司类型
                result['size'] = item.get('company').get('size').get('name')   #  公司大小
                result['eduLevel'] = item.get('eduLevel').get('name')  #  要求学历
                result['salary'] = item.get('salary')      #  薪资
                result['workingExp'] = item.get('workingExp').get('name')  # 要求工作年限
                result['welfare'] = item.get('welfare')   # 福利
                self.html_queue.put(result)

class Save(threading.Thread):
    def __init__(self, url_queue, html_queue):
        super().__init__()
        self.url_queue = url_queue
        self.html_queue = html_queue

    def run(self):
        while True:
            if self.html_queue.empty() and self.url_queue.empty():
                break
            result = self.html_queue.get()
            self.save_result(result)

    def save_result(self, res):
        '''
        保存到MongoDB数据库
        :param res:
        :return:
        '''
        client = pymongo.MongoClient(host='localhost', port=27017)
        db = client['ZLDXC']
        result = db['DXCjob']
        result.insert(res)

def main():
    '''
    主函数
    '''
    url_queue = Queue()
    html_queue = Queue()
    for i in range(int(MAX_PAGE)+1):
        parse_url = {
            'start': str(i),
            'pageSize': '60',
            'cityId': '489',
            'workExperience': '-1',
            'education': '-1',
            'companyType': '-1',
            'employmentType': '-1',
            'jobWelfareTag': '-1',
            'kw': '爬虫',
            'kt': '3',
            '_v': '0.31890103',
            'x-zp-page-request-id': 'c8ffebc418124252b3922a6655113c32-1543474437774-827889',
        }
        join_url = 'https://fe-api.zhaopin.com/c/i/sou?'
        url = join_url + urlencode(parse_url)
        url_queue.put(url)

    wait_url = []
    for x in range(3):  # 开启三条爬取线程
        geturl = GetUrl(url_queue, html_queue)
        geturl.start()
        wait_url.append(geturl)

    wait_db = []
    for x in range(3):  # 开启三条储存线程
        save = Save(url_queue, html_queue)
        save.start()
        wait_db.append(save)

    for w in wait_url:
        w.join()
    for w in wait_db:
        w.join()


if __name__ == '__main__':
    main()
    print('-----所有线程结束爬取----')



