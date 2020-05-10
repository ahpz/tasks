# -*- coding: utf-8 -*-
# @Time      : 2020/5/8 22:40
# @Author    : pengzhi
# @File      : console
# @Software  : PyCharm
# @Desc
import os
import logging
logging.basicConfig(level=logging.DEBUG)
from master import Master
from service import Service

#全局配置

DEFAULT_WORKER_PROCESS_NUM = 10  # 默认work数
DEFAULT_MASTER_MSG_NUM = 1  #

def handle_message(msg):
    pass


# doas -p ad.product.i18n_dpa_message python console.py
def main():
    """
    1
    :return:
    """
    logging.info("the service start ....")
    worker_num = os.environ.get('WORKER_PROCESS_NUM', DEFAULT_WORKER_PROCESS_NUM)  # 单个TCE 实例worker数
    msg_num = os.environ.get('MASTER_MSG_NUM', DEFAULT_MASTER_MSG_NUM)
    logging.info("Worker process num:%s, Single query message num:%s", worker_num, msg_num)
    master = Master(handle_message, worker_process_num=worker_num, master_msg_num=msg_num)
    service = Service(master)
    service.start()


if __name__ == "__main__":
    """
    """
    main()
