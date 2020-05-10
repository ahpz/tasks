# -*- coding: utf-8 -*-
# @Time      : 2020/3/4 22:24
# @Author    : pengzhi
# @File      : service
# @Software  : PyCharm
# @Desc


class Service(object):
    """
    对应TCE集群
    """

    def __init__(self, master, **options):
        self.master = master
        self.options = options

    def start(self):
        """
        pass
        :return:
        """
        self.master.start()

    def stop(self):
        """
        pass
        :return:
        """
        self.master.stop()
