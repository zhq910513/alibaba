import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

from common.log_out import log_err
from dbs.pipelines import MongoPipeline

requests.packages.urllib3.disable_warnings()
ua = UserAgent()


class Category(object):
    def all_category_page(self):
        try:
            headers = {
                'User-Agent': ua.random
            }
            resp = requests.get(url='https://www.alibaba.com/Products', headers=headers, verify=False)
            if resp.status_code == 200:
                parse_data = self.category_parse(resp.text)
                if not parse_data: return
                MongoPipeline('categories').update_item({'category': None}, parse_data)
            else:
                print(resp.status_code)
        except Exception as error:
            log_err(error)

    @staticmethod
    def category_parse(html):
        data_list = []
        try:
            soup = BeautifulSoup(html, 'lxml')
            cg_main = soup.find('div', {'class': 'cg-main'})
            if cg_main:
                for item in cg_main.find_all('div', {'class': 'item util-clearfix'}):
                    category_1 = item.find('h3').get_text().replace('\n', '').replace('\t', '').replace('\r',
                                                                                                        '').strip()
                    sub_items = item.find_all('div', {'class': 'sub-item'})
                    if sub_items:
                        for sub in sub_items:
                            category_2 = sub.find('h4').find('a').get_text().replace('\n', '').replace('\t',
                                                                                                       '').replace('\r',
                                                                                                                   '').strip()
                            if sub.find('ul'):
                                for li in sub.find_all('li'):
                                    category_3 = li.find('a').get_text().replace('\n', '').replace('\t', '').replace(
                                        '\r', '').strip()
                                    category = f'{category_1} | {category_2} | {category_3}'
                                    data_list.append({
                                        'category': category,
                                        'category_last': category_3
                                    })
        except Exception as error:
            log_err(error)
        return data_list
