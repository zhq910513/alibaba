import re
import time
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from common.log_out import log_err
from dbs.pipelines import MongoPipeline

requests.packages.urllib3.disable_warnings()

ua = UserAgent()


class Company(object):
    def __init__(self, category_info):
        self.category = category_info.get('category')
        self.category_last = category_info.get('category_last')
        self.suppliers = 0
        self.page_count = 2
        self.select_company()

    def select_company(self, page=1):
        for _next in range(page, 500):
            print(f'访问 {self.category_last} 中第 {_next} 页')
            try:
                self.company_list(_next)
                if _next > self.page_count:
                    MongoPipeline('categories').update_item({'category': None},
                                                            {'category': self.category, 'status': 1})
                    break

                time.sleep(3)
            except Exception as error:
                log_err(error)

    def company_list(self, _next, retry=0):
        try:
            link = f'https://www.alibaba.com/trade/search?indexArea=company_en&f1=y&page={_next}&keyword={self.category_last.replace(" ", "_").replace("&", "%2526")}&viewType=L&n=38'
            print(link)
            headers = {
                'Host': 'www.alibaba.com',
                'Connection': 'keep-alive',
                'Pragma': 'no-cache',
                'Cache-Control': 'no-cache',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'upgrade-insecure-requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Dest': 'empty',
                'Referer': 'https://www.alibaba.com/sw.js?v=2.13.21&_flasher_manifest_=https://s.alicdn.com/@xconfig/flasher_classic/manifest',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Cookie': 't=2c857f23e225bc60e09b0ca32223cc2f; ali_apache_id=11.9.13.76.1649658628862.428828.7; cna=v4lAGupGjjwCAXQYQ3wbPELx; _bl_uid=9glOe1wtun5czp539kIFq1Rqekgy; xman_us_f=x_locale=zh_CN&x_l=1&last_popup_time=1649658981810&x_user=CN|zhang|hongqiang|cnfm|234391883&no_popup_today=n; xman_i=aid=649942267; sgcookie=E1003taJsbqMtHeDIolo2Sgg%2Fefh%2BX%2BtgqZbpwsuqUspRMgS%2BCVOjIIvnROJxeaUFHAXDXRTASFFJSke05ossKAZnnyA0WRPa%2BurBqfUzrorsFI%3D; intl_common_forever=gv4ww3ErFuaNfMuIuRIf/xlEGu38Xx9a+1be7dhDmZux3cmsi2L3fA==; xman_f=HnQVAt1JkRcg0033/XA/dwPKIkbOsOYm9kq+oCuko0nLF0Z6gWvHUExCD874KXA6HVT0FXbbdlTm+650nn+ujSGYZsVo9OCiv6q9iG7D0/B6y6ROvCt5B2uNm3Q9hH4Ji+repe/QCetdScMVkcia0j7K4PZSQvmiPptss+NZZBo9pY34o4vVophIrK8hhLYQWKBDHiXekS92gohp6eiqKVEGFXlO7EYlpKzRSC7h8MmjqBclscXGDL8aYND4+ixf5QLiIYTP5EdvpholyNvd/KOad5D0XbNgdGfazClsEMUPXXaeaHNvY4koNHHdvdyRvh+28g7kUtdf9/seQs24CV8DikZ5sUCAzHVhnGxjX2XquwzuXmOq4DKcgEppZUAeYwqwHVV7txTXYJeW5mR2wA==; _m_h5_tk=18e21f705bf360d06f3f005cfc4d1b43_1649840799598; _m_h5_tk_enc=b2ac9121591012b5cbc9bde7a41fc307; xman_t=Jjtd+FyQYSbSZOaYWm3zoySW6z2LOs2rDvjyWi3CQ/2wLseU6Ywkkmg4AgayTgIdMAdw7Gan9zCsSB1fEANlBDEHhjCU1jEe; intl_locale=en_US; ali_apache_track=mt=2|mid=cn1524238420khty; XSRF-TOKEN=c27fdae6-dc94-4cc1-bf39-7d7b1e05f17c; cookie2=185c45d35c94950fa998416a20811cc2; _tb_token_=fbeb36969f7bb; acs_usuc_t=acs_rt=9b505d88078c4e4ab0e0822e4b458872; _csrf_token=1649832532617; _samesite_flag_=true; atm-whl=-1%260%260%260; atm-x=__ll%3D-1; xlly_s=1; sc_g_cfg_f=sc_b_currency=CNY&sc_b_locale=en_US&sc_b_site=CN; _ga=GA1.2.1005698432.1649839935; _gid=GA1.2.10215052.1649839935; history=company%5E%0A230928954%24%0A218638708%24%0A50139898%24%0A29018801885; x5sec=7b22736370726f78793b32223a2234356135333833646432313037653934373032363163353835636334323564394350574133704947454b3656756f476f78366576554443362f664e2f227d; JSESSIONID=85AD5D883B62B6C069AD0C3B42174B8F; ali_apache_tracktmp=; tfstk=ctRdBv0whcmHblQ9upHMz5Ec3SucZLnRfysuwQKQC14StgFRiSADM08EdasVvVC..; l=eBEbTjVrLiDh4iLEBOfwourza77OSIRAguPzaNbMiOCP_o5H5AXFB62khnYMC3GVhsdDR3-T6YgYBeYBq3tSnxvOmcgsr4Dmn; isg=BDAwbfmRJmRgI_pkEXSMqXk1Af6CeRTDo_1q2CqB_Ate5dCP0onkU4bVOe2F9cyb'
            }
            resp = requests.get(url=link, headers=headers, verify=False)
            if resp.status_code == 200:
                parse_data = self.company_list_parse(resp.text)
                if not parse_data:
                    if retry < 3:
                        time.sleep(3)
                        return self.company_list(_next, retry + 1)
                    else:
                        print('没有数据')
                        return
                if isinstance(parse_data, str):
                    print(f'没有找到匹配关键词 {self.category_last} 的数据')
                    if _next == 1:
                        _status = 400
                    else:
                        _status = 1
                    MongoPipeline('categories').update_item({'category': None},
                                                            {'category': self.category, 'status': _status})
                else:
                    MongoPipeline('company_list').update_item({'link': None}, parse_data)

                if _next == 1:
                    MongoPipeline('categories').update_item({'category': None},
                                                            {'category': self.category, 'suppliers': self.suppliers})
            else:
                print(resp.status_code)
        except Exception as error:
            if retry < 3:
                return self.company_list(_next, retry + 1)
            else:
                log_err(error)

        time.sleep(5)

    def company_list_parse(self, html):
        if 'No matches found for' in str(html):
            return 'No matches'

        data_list = []
        soup = BeautifulSoup(html, 'lxml')

        try:
            items = soup.find_all('h2', {'class': 'title ellipsis'})
            if items:
                for item in items:
                    try:
                        company_name = item.find('a').get_text().replace('\n', '').replace('\t', '').replace('\r',
                                                                                                             '').strip()
                        company_link = item.find('a').get('href')
                        company_domain = urlparse(company_link).netloc
                        contact_link = f'https://{company_domain}/contactinfo.html'
                        data_list.append({
                            'category': self.category,
                            'company_name': company_name,
                            'link': contact_link
                        })
                    except:
                        pass
        except Exception as error:
            log_err(error)

        try:
            for span in soup.find('div', {'class': 'ui-breadcrumb ml'}).find_all('span'):
                try:
                    _text = span.get_text()
                    if _text:
                        _count = re.search('\d+,?\d+|\d+', _text)
                        if _count:
                            self.suppliers = int(_count[0].replace(',', ''))
                except:
                    pass
        except Exception as error:
            log_err(error)

        try:
            pages = []
            if soup.find('div', {'class': 'ui2-pagination-pages'}):
                for a in soup.find('div', {'class': 'ui2-pagination-pages'}).find_all('a'):
                    num = a.get_text().replace('\n', '').replace('\t', '').replace('\r', '').strip()
                    try:
                        pages.append(int(num))
                    except:
                        pass
            if pages:
                self.page_count = max(pages[:-1])
        except Exception as error:
            log_err(error)

        return data_list
