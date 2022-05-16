import time
from multiprocessing.pool import ThreadPool

from dbs.pipelines import MongoPipeline
from spiders.company_list_spider import Company
from spiders.contact_spider import Contact

class ThreadScheduler:
    def __init__(self, _obj):
        self.list = _obj['list']
        self.func = _obj['func']
        self.processes = 4

    @staticmethod
    def pool(processes):
        return ThreadPool(processes=processes)

    def thread_handle(self, Async=True):
        if not self.func:return
        pool = self.pool(self.processes)
        thread_list = []
        for _ls in self.list:
            if Async:
                out = pool.apply_async(func=self.func, args=(_ls,))  # 异步
            else:
                out = pool.apply(func=self.func, args=(_ls,))  # 同步
            thread_list.append(out)
        pool.close()
        pool.join()


def select_company():
    info_list = MongoPipeline('categories').find({'status': None}).limit(10)

    spider_info = {
        'type': 'spiders',
        'func': Company,
        'list': info_list
    }
    ThreadScheduler(spider_info).thread_handle()


def select_contact():
    info_list = MongoPipeline('company_list').find({'status': None}).limit(10)

    spider_info = {
        'type': 'spiders',
        'func': Contact,
        'list': info_list
    }
    ThreadScheduler(spider_info).thread_handle()


if __name__ == '__main__':
    while True:
        select_contact()
        time.sleep(10)
