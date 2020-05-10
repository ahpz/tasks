# -*- coding: utf-8 -*-
# @Time      : 2020/3/4 22:23
# @Author    : pengzhi
# @File      : worker
# @Software  : PyCharm
# @Desc 不同的信号不同的标志位 实现优雅关闭
import Queue
import gc
import logging
import os
import signal
import sys
import time
import traceback

from service import Service


class Worker(Service):
    """
    worker 进程
    1 取数 10 个 0.5ms
    以下地址可以查看进程分配了多少个 partition
    监控埋点 失效
    """

    worker_no = -1  # 序号
    process = None  # 进程对象
    pid = -1  # 进程ID
    check_worker_interval = 30  # 心跳时间间隔

    def __init__(self, handle_message_func, queue, **options):
        """

        :param handle_message_func:
        :param worker_msg_num:
        :param options:
        """
        self.is_running = 1  # 初始化服务是启动的
        self.handle_message_func = handle_message_func
        self.queue = queue
        self.status = 0  # 0:空闲 1:忙碌
        self.task_id = 0

    def is_idle(self):
        """

        :return:
        """
        return self.status == 0

    def task_done(self):
        """

        :return:
        """
        self.status = 0

    def task_doing(self):
        """

        :return:
        """
        self.status = 1

    def signal_handler(self, signum, frame):
        """

        :param signum:
        :param frame:
        :return:
        """
        # logging.info("%ssignum:%s,frame:%s%s", System.LOG_BLANKN, signum, frame, System.LOG_BLANKN)
        pass

    def __call__(self, *args, **kwargs):
        while self.is_running:  # 0|1
            logging.info("the worker running ...")
            try:
                # TODO: This should be refactored to call listen in every threads in pool if in thread pool mode.
                self.start()
            except (KeyboardInterrupt, SystemExit):
                logging.warn("Worker:SystemExit, KeyboardInterrupt.")
            except Exception as ex:
                traceback.print_exc()
                logging.error("exception:%s", ex)  # 单次异常不影响子进程
            finally:
                pass

    def start(self):
        """
        1 设置信号和心跳计时器
        2 分配任务:取消息,放入进程池,等待执行完成
        3 执行任务
        worker_type  处理
        :return:
        """
        logging.info("the worker start ...")
        self.signal()
        self.exec_task()

    # 信号处理
    def signal(self):
        """
        遍历队列
        信号处理函数不要使用logging 和主线程lock死锁
        :return:
        """
        logging.info("start keep_heartbeat...")
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        signal.signal(signal.SIGINT, self.handle_sigint)
        signal.signal(signal.SIGALRM, self.handle_sigalarm)
        signal.signal(signal.SIGHUP, self.handle_sighup)
        signal.setitimer(signal.ITIMER_REAL, self.check_worker_interval, self.check_worker_interval)  # 设置心跳

    def handle_sigalarm(self, sig, frame):
        """
        当前执行的任务保活
        :param sig:
        :param frame:
        :return:
        """
        pass

    def handle_sigint(self, sig, frame):
        """
        暴力关闭
        :param sig:
        :param frame:
        :return:
        """
        os._exit(0)

    def handle_sigterm(self, sig, frame):
        """
        暴力关闭
        :param sig:
        :param frame:
        :return:
        """
        os._exit(0)

    def handle_sigquit(self, sig, frame):
        """
        从容关闭
        :param sig:
        :param frame:
        :return:
        """
        self.is_running = 0

    def handle_sighup(self, sig, frame):
        """
        优雅重启
        :param sig:
        :param frame:
        :return:
        """
        self.is_running = 0

    def handle_sigchild(self, sig, frame):
        """

        :param sig:
        :param frame:
        :return:
        """
        pass

    # 任务执行
    def exec_task(self):
        """
        查询任务
        1 检查是否分配到当前实例
        2 待提交(提交）重新放入队列
        3 待轮询 轮询, 若成功则执行，否则放入队列
        :return:
        """
        try:
            while self.is_running:
                st = time.time()
                logging.info("worker pid:%s, self.pid:%s, start fetch:st:%s", os.getpid(), self.pid, st)
                try:
                    pass  # 队列取数 非阻塞
                    self.task_id = self.queue.get(False)
                    self.task_doing()
                    logging.info("worker pid:%s, self.pid:%s, fetch duration:%s", os.getpid(), self.pid, time.time() - st)
                    logging.info("worker pid:%s, self.pid:%s, task_id:%s", os.getpid(), self.pid, self.task_id)
                    logging.info("worker pid:%s, self.pid:%s, exec duration:%s", os.getpid(), self.pid, time.time() - st)
                    self.task_done()
                except Queue.Empty:
                    pass
                    time.sleep(1)  # 睡眠1s
        except Exception as ex:
            logging.exception("worker pid:%s, self.pid:%s, the worker exec task failed:%s, traceback:%s", os.getpid(), self.pid, ex, traceback.format_exc())
        finally:
            self.task_done()
            gc.collect()
        sys.exit(0)  # 进程退出不执行后面代码 sys.exit(0) 引发一个SystemExit 异常做清理工作

    def handle_msg(self, msg):
        """
        1 添加公用埋点函数 支持多线程
        :param optype:
        :param creative_id:
        :return:
        """
        try:
            success = self.handle_message_func(msg)  # 业务方自定义函数处理
        except Exception as ex:
            logging.warn("worker pid:%s, self.pid:%s, the worker handle msg exception:%s, msg:%s, traceback:%s", os.getpid(), self.pid, ex, msg, traceback.format_exc())
            success = False
        return {'pid': os.getpid(), 'msg': msg, 'success': success}

    def after_task_func(self, result):
        """
        pass
        :param result:
        :return:
        """
        logging.info("%spid:%s,result:%s%s", System.LOG_BLANKN, os.getpid(), result, System.LOG_BLANKN)
