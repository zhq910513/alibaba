import json
import random
import re
import time
from urllib.parse import unquote, urlparse

import requests
from fake_useragent import UserAgent

from common.log_out import log_err
from dbs.pipelines import MongoPipeline

requests.packages.urllib3.disable_warnings()

ua = UserAgent()
ctoken = 's6nntbfeerf_'
cookies = 't=2c857f23e225bc60e09b0ca32223cc2f; ali_apache_id=11.9.13.76.1649658628862.428828.7; cna=v4lAGupGjjwCAXQYQ3wbPELx; xman_us_f=x_locale=zh_CN&x_l=1&last_popup_time=1649658981810&x_user=CN|zhang|hongqiang|cnfm|234391883&no_popup_today=n; xman_i=aid=649942267; ali_apache_track=mt=2|mid=cn1524238420khty; sc_g_cfg_f=sc_b_currency=CNY&sc_b_locale=en_US&sc_b_site=CN; _ga=GA1.2.1005698432.1649839935; history=company%5E%0A230928954%24%0A218638708%24%0A50139898%24%0A29018801885; c_csrf=42269a47-f780-42f5-ad2b-a1af63191560; acs_usuc_t=acs_rt=a4433974369d4afb8fd2824c829a6695; cookie2=105e9310b7e67b2e9842b0c37f8ea07a; _tb_token_=53e37e33eb3f6; _samesite_flag_=true; xlly_s=1; _m_h5_tk=8d7054e67e11c7ab5bbdb08f5a2102b6_1650289844787; _m_h5_tk_enc=e91246a338c01d5ee663fffa9e209304; _hvn_login=4; sgcookie=E100LNgrhM%2FWY1miyE2PxAKU7Po03559Bwa%2BoCga0Fzi0f3aEEXvRuD6S99eUioVqv9E07QIo71wgAEBJManPQUBHiCrYdiZAwwNhiPXLb7OPeQ%3D; csg=c6248b7a; ali_apache_tracktmp=""; xman_us_t=ctoken=s6nntbfeerf_&l_source=alibaba&x_user=EogNvqihvNsuqLsA4D4rmxeRuMlWeFodlHOsGBZDHTk=&x_lid=cn1524238420khty&sign=y&need_popup=y; intl_locale=zh_CN; intl_common_forever=jzpbkHIpe1LQKRN8qh3IYT+p3jRrvfNF+X7183ivjHSp3rP8cf7uKg==; xman_f=CqCe8u3U2ElANBw/42dlMPezxP+lD1XesRWwb6wkgh1pl09URltitnfQ4h/hp2eJ31ycmU/5LxGnuqV0/E2ani7X7OHp/+cSokUs2poyGIwQfIJo4x6F8HrMrmL2pIKl119+5WnPV6tF36E/uimQqWQgI/IU2izn2fcNcw3k8V2N9UVOZET/zZo/Ny1fixc6zGx73X8kzXhFfzz+8Xw/gjYx5ehOFQdFvTc8zJhuykWwVUt5paAyVE5dzwywZQKhWJSRSn+cGu4Oamn+7X/vJ1Tm+Jn3bFZ1wKVywq+kQtWpxHwHMgIv9LhTx2c4wiiRvFvbVpXdaNFGwL31zQhJAi4nZ3niKb9oJIBNF8v5gYhH2CLXQHO5CzKSrRNTKDcICjAk6kO7iwo/zRyyce2b9w==; JSESSIONID=9F52AEFCEE954F6E50D0B6572EDC9715; xman_t=u2LGRIicwjRm95uX6mLJpZb3wvwkDX/mOJwGVidRVqReWYPZDqGGSDyxIHHYtGgCR/X2PvxP7CcQKM5Cu3dNAEhqSjrHD64wKrb9kaqOMsPDpaEJpOx/O3q0dfC7aCxzFTCYV27kGm7vcJ/qNeTgJ+LiZF29E8qGskX+iHBYB3IYcYUt/wX3rItO9MtvS0p6AaRlCLsaZ51jIBXwph+HvuklYhh1U/000dG6ghQZxsJbPwIqhRE4bdfe8eC9mv7llQdY8D4rD7DO08R+b2UPV2wOGFqwlCgY+qTSXivQjJYNxORiPL7xa/26dFbgX7shF9ixJagm07gzyqJXt3Lklvz5uKcNZL7c0sm8pbPNuNg82vBj1vLifrWI2RM3lOfzFYjLfzpWzjM7eL+HKEikihl/+Xi8GfjFdTuAXIBhufx+FUg4+0NarEaJYH2osJWr96h3KKkWROhju6hsYFcwx8+W05nXA8MvczXERfcxm9ZwWyIioMrXkPPiVqnkqqeafKE5SpFHbvpMjzeXWMk2XFffF5XOo4FgeKic7OpgwvDR+2E03DjpntNrtpp2GO1NoOByEtP9gKdufFuYGsaz1ZHgqtAdzLP1I4jZRkj+N+fWP3SKqdWpTgOTBVV4/yZXDZaczGZWUHC9vC9j+SwP+oI1mao0IW3NfKHuLysBk68lLeTxp0MAEbGfgmTjUlPcqYV7M43EEUZ7MxH4RVwMmsFAzYxlVS1uXfpLTUYHoTE=; tfstk=ct3hBPjHmDrBEqriceaBP585gYwhZG_zn4us7B2mTaHupquNiOsN02JXsJvXLg1..; l=eBEbTjVrLiDh4OnjBO5ZKurza77TNIOf1sPzaNbMiInca6KlLEUWGNC3dAq9bdtjgtfYFeKy52gX3REyPxUU-xNTmv9RrHGd_xv9-; isg=BIyMSHKfAoKkvBbwXThodf2BXeq-xTBvtq1a8uZMsTfEcS97D9EZ_nSHFXnJOWjH'

class Contact(object):
    def __init__(self, link_info:dict):
        self.link = link_info.get('link')
        self.category = link_info.get('category')
        self.domain = urlparse(self.link).netloc
        self.contact_page()

    def contact_page(self):
        try:
            time.sleep(random.uniform(2, 5))
            headers = {
                'authority': 'miningmachine.en.alibaba.com',
                'method': 'GET',
                'path': '/contactinfo.html?spm=a2700.shop_cp.88.74',
                'scheme': 'https',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'cache-control': 'no-cache',
                'pragma': 'no-cache',
                'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1',
                'user-agent': ua.random
            }
            resp = requests.get(url=self.link, headers=headers, verify=False)
            if resp.status_code == 200:
                parse_data = self.contact_parse(resp.text)
                if not parse_data:
                    print('没有获得联系页面解析数据')
                    return
                MongoPipeline('contacts').update_item({'link': None}, parse_data)
                if parse_data.get('encryptAccountId'):
                    self.contact_info(parse_data.get('encryptAccountId'))
            else:
                print(resp.status_code)
        except Exception as error:
            log_err(error)

    def contact_parse(self, html):
        data = None
        try:
            datas = re.findall("module-data='(.*?)'", html, re.S)
            if datas:
                data = {'link': self.link, 'category': self.category, 'accountFax': None, 'accountPhone': None, 'accountMobileNo': None}
                for _data in datas:
                    module_data = json.loads(unquote(_data))
                    try:
                        if module_data.get('gdc'):
                            gdc = module_data.get('gdc')
                            # pageId
                            if gdc.get('pageId'):
                                data.update({'pageId': gdc.get('pageId')})

                            # siteId
                            if gdc.get('siteId'):
                                data.update({'siteId': gdc.get('siteId')})

                        if module_data.get('mds'):
                            mds = module_data.get('mds')
                            if mds.get('moduleData'):
                                moduleData = mds.get('moduleData')
                                if moduleData.get('data'):
                                    # companyName
                                    if moduleData.get('data').get('companyName') and isinstance(
                                            moduleData.get('data').get('companyName'), str):
                                        data.update({'companyName': moduleData.get('data').get('companyName')})

                                    # companyLogoFileUrl
                                    if moduleData.get('data').get('companyLogoFileUrl'):
                                        companyLogoFileUrl = moduleData.get('data').get('companyLogoFileUrl')
                                        if not str(companyLogoFileUrl).startswith('https:'):
                                            companyLogoFileUrl = 'https:' + companyLogoFileUrl
                                        data.update({'companyLogoFileUrl': companyLogoFileUrl})

                                    # hasGlobalView
                                    if moduleData.get('data').get('hasGlobalView'):
                                        data.update({'hasGlobalView': moduleData.get('data').get('hasGlobalView')})

                                    # supplierMainProducts
                                    if moduleData.get('data').get('supplierMainProducts'):
                                        data.update({'supplierMainProducts': moduleData.get('data').get(
                                            'supplierMainProducts')})

                                    # companyRegisterCountry
                                    if moduleData.get('data').get('companyRegisterCountry'):
                                        data.update({'companyRegisterCountry': moduleData.get('data').get(
                                            'companyRegisterCountry')})

                                    # siteType
                                    if moduleData.get('data').get('siteType'):
                                        data.update({'siteType': moduleData.get('data').get('siteType')})

                                    # supplierStars
                                    if moduleData.get('data').get('supplierStars'):
                                        data.update({'supplierStars': moduleData.get('data').get('supplierStars')})

                                    # baoAccountAmount
                                    if moduleData.get('data').get('baoAccountAmount'):
                                        data.update(
                                            {'baoAccountAmount': moduleData.get('data').get('baoAccountAmount')})

                                    # companyLocation
                                    if moduleData.get('data').get('companyLocation'):
                                        data.update({'companyLocation': moduleData.get('data').get('companyLocation')})

                                    # isVip
                                    if moduleData.get('data').get('isVip'):
                                        data.update({'isVip': moduleData.get('data').get('isVip')})

                                    # companyId
                                    if moduleData.get('data').get('companyId'):
                                        data.update({'companyId': moduleData.get('data').get('companyId')})

                                    # companyJoinYears
                                    if moduleData.get('data').get('companyJoinYears'):
                                        data.update(
                                            {'companyJoinYears': moduleData.get('data').get('companyJoinYears')})

                                    # certificateInfo
                                    if moduleData.get('data').get('certificateInfo'):
                                        data.update({'certificateInfo': moduleData.get('data').get('certificateInfo')})

                                    # companyBusinessType
                                    if moduleData.get('data').get('companyBusinessType'):
                                        data.update(
                                            {'companyBusinessType': moduleData.get('data').get('companyBusinessType')})

                                    # goldCPUrls
                                    if moduleData.get('data').get('goldCPUrls'):
                                        if moduleData.get('data').get('goldCPUrls').get('mainProduct'):
                                            data.update({'mainProduct': moduleData.get('data').get('goldCPUrls').get(
                                                'mainProduct')})

                                    # encryptAccountId
                                    # IDX1OyLQYBDNHuAXHmTuoo5VwqEXAQ-VVNhEL73Qc8AXtYnHSvXW48nnrT5ctjfCdXMr
                                    if moduleData.get('data').get('encryptAccountId'):
                                        data.update(
                                            {'encryptAccountId': moduleData.get('data').get('encryptAccountId')})

                                    # accountCity
                                    if moduleData.get('data').get('accountCity'):
                                        data.update({'accountCity': moduleData.get('data').get('accountCity')})

                                    # accountDisplayName
                                    if moduleData.get('data').get('accountDisplayName'):
                                        data.update(
                                            {'accountDisplayName': moduleData.get('data').get('accountDisplayName')})

                                    # accountZip
                                    if moduleData.get('data').get('accountZip'):
                                        data.update({'accountZip': moduleData.get('data').get('accountZip')})

                                    # accountCountry
                                    if moduleData.get('data').get('accountCountry'):
                                        data.update({'accountCountry': moduleData.get('data').get('accountCountry')})

                                    # accountJobTitle
                                    if moduleData.get('data').get('accountJobTitle'):
                                        data.update({'accountJobTitle': moduleData.get('data').get('accountJobTitle')})

                                    # accountProvince
                                    if moduleData.get('data').get('accountProvince'):
                                        data.update({'accountProvince': moduleData.get('data').get('accountProvince')})

                                    # accountEmail
                                    if moduleData.get('data').get('accountEmail'):
                                        data.update({'accountEmail': moduleData.get('data').get('accountEmail')})

                                    # companyWebSite
                                    if moduleData.get('data').get('companyWebSite'):
                                        data.update({'companyWebSite': moduleData.get('data').get('companyWebSite')})

                                    # companyOperationalAddress
                                    if moduleData.get('data').get('companyOperationalAddress') and isinstance(
                                            moduleData.get('data').get('companyOperationalAddress'), dict):
                                        data.update({'companyOperationalAddress': moduleData.get('data').get(
                                            'companyOperationalAddress').get('value')})
                    except Exception as error:
                        log_err(error)
        except Exception as error:
            log_err(error)
        return data

    def contact_info(self, encryptAccountId):
        try:
            time.sleep(random.uniform(2, 5))
            url = f'https://{self.domain}/event/app/contactPerson/showContactInfo.htm?encryptAccountId={encryptAccountId}&ctoken={ctoken}'
            headers = {
                'authority': 'wfrunxin.en.alibaba.com',
                'method': 'GET', 'scheme': 'https',
                'accept': '*/*',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'cache-control': 'no-cache',
                'cookie': cookies,
                'pragma': 'no-cache',
                'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36'
            }
            resp = requests.get(url=url, headers=headers, verify=False)
            if resp.status_code == 200:
                parse_data = self.contact_info_parse(resp.json())
                print(parse_data)
                if not parse_data: return
                MongoPipeline('contacts').update_item({'link': None}, parse_data)
                MongoPipeline('company_list').update_item({'link': None}, {'link': self.link, 'status': 1})
        except Exception as error:
            log_err(error)

    def contact_info_parse(self, _json):
        data = {}
        try:
            if _json.get('isSuccess'):
                data = {'link': self.link}
                data.update(_json.get('contactInfo'))
        except Exception as error:
            log_err(error)
        return data
