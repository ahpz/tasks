# -*- coding: utf-8 -*-
# @Time      : 2020/3/4 22:23
# @Author    : pengzhi
# @File      : master
# @Software  : PyCharm
# @Desc
import errno
import gc
import logging
import multiprocessing
import os
import signal
import time
import traceback
import Queue
from service import Service
from worker import Worker


class Master(Service):
    """
    每个TCE实例对应一个master
    启动worker
    """

    def __init__(self, handle_message_func, worker_process_num=8, master_msg_num=1, **options):
        logging.info("options:%s", options)
        self.worker_process = []
        self.master_msg_num = master_msg_num  # master 单次取task个数
        self.worker_process_num = worker_process_num  # 子进程个数
        self.options = options
        self.handle_message_func = handle_message_func
        self.is_running = 1
        self.queue = multiprocessing.Queue(worker_process_num)

    def set_options(self, options):
        """

        :param options:
        :return:
        """
        self.options = options

    def signal_handler(self, signum, frame):
        """

        :param signum:
        :param frame:
        :return:
        """
        # logging.info("%ssignum:%s,frame:%s%s", System.LOG_BLANKN, signum, frame, System.LOG_BLANKN)
        pass

    def start(self):
        """
        create:0
        submit:1
        success:10
        :return:
        """
        logging.info("master start ...")
        for n in range(self.worker_process_num):
            worker = Worker(self.handle_message_func, self.queue, **self.options)
            worker.no = n  # 进程序号
            process = multiprocessing.Process(target=worker)
            process.daemon = True  # 父进程关闭 会关闭子进程的
            worker.process = process
            process.start()
            worker.pid = process.pid
            self.worker_process.append(worker)

        # wait until the condition is set by stop()
        # https://zhaochj.github.io/2016/08/14/2016-08-14-%E7%BA%BF%E7%A8%8B%E7%9A%84daemon%E4%B8%8Ejoin%E7%89%B9%E6%80%A7/
        while True:
            try:
                logging.info("master running ...")
                time.sleep(1.1)
                if not self.is_running:
                    break
                self.wait_and_respawn_workers()
                self.check_and_respawn_workers() # 其他非
                self.wait_and_respawn_workers()
                self.check_and_assign_tasks()
            except (SystemExit, KeyboardInterrupt):
                logging.warn("Master:SystemExit, KeyboardInterrupt")
                break
            except Exception as ex:
                logging.error("exception:%s", ex)
                traceback.print_exc()

        for p in multiprocessing.active_children():
            os.kill(p.pid, signal.SIGKILL)
            p.join()  # 子进程
        os._exit(0)

    def check_and_assign_tasks(self):
        """
        1 方案1: 检测子进程空闲
        2 方案2: 判断队列是否空 采用此方案
        :return:
        """
        try:
            limit = 0
            # 方案1：
            for idx in range(len(self.worker_process)):
                worker = self.worker_process[idx]
                if worker.is_idle():
                    limit += 1
            # 分配limit 个任务到 queue
            limit = min(limit, self.worker_process_num - self.queue.qsize())
            # 方案2:

            logging.info("assign %s tasks ...", limit)
            try:
                for i in xrange(limit):
                    self.queue.put(1)
            except Queue.Full:
                pass

        except Exception as ex:
            logging.warn("check_and_assign_tasks exception:%s, traceback:%s", ex, traceback.format_exc())
    def check_and_respawn_workers(self):
        """
        检测子进程 个数<self.worker_process_num 则拉起对应进程
        :return:
        """
        try:
            p = psutil.Process(os.getpid())
            #self.logger.info("check_and_respawn_workers p.pid:%s", p.pid)
            if len(p.children()) >= len(self.worker_process):
                return True
            self.logger.info("check_and_respawn_workers len(childrens):%s < self.worker_process:%s", len( p.children()), self.worker_process)
            pids = [child.pid for child in p.children()]
            pids = set(pids)
            for idx in range(len(self.worker_process)):
                worker = self.worker_process[idx]
                if worker.pid not in pids:
                    try:
                        multiprocessing.current_process()._children.discard(worker.process)
                    except Exception as ex:
                        self.logger.warning(u"exception:%s, pid:%s,", ex, worker.pid)
                    w = multiprocessing.Process(target=worker)
                    w.daemon = True
                    worker.process = w
                    w.start()
                    self.logger.warning("check_and_respawn_workers exit process worker.pid:%s, worker.no:%s, new create process: w.pid:%s ******", worker.pid, worker.worker_no, w.pid)
                    worker.pid = w.pid  # 否则是None
        except Exception as ex:
            self.logger.warning("check_children_process exception:%s", ex)
            
    def wait_and_respawn_workers(self):
        """
        监听僵死子进程
        :return:
        """
        try:
            while True:
                cpid, status = os.waitpid(-1, os.WNOHANG)  # WHOHAING 木有子进程则立即返回
                if cpid == 0:
                    logging.warn("All child process healthy.")
                    break  # No child process was immediately available.
                exitcode = status >> 8
                logging.error('worker with pid %s exit with exitcode %s, starting new worker...', cpid, exitcode)
                for idx in range(len(self.worker_process)):
                    worker = self.worker_process[idx]
                    if worker.pid == cpid:  # 当前进程对象需要关闭
                        w = multiprocessing.Process(target=worker)
                        w.daemon = True
                        worker.process = w
                        w.start()
                        worker.pid = w.pid  # 否则是None
        except OSError as e:
            logging.warn("The wait_and_respawn_workers OSError:%s", e)
            if e.errno == errno.ECHILD:
                pass  # Current process has no existing unwaited-for child processes.
            else:
                raise
        finally:
            gc.collect()

    # 信号处理方法
    def signal(self):
        """
        设置信号处理方法
        注意信号处理方法中 不要使用logging 会导致多线程死锁 （和主线程logging lock 导致死锁)
        :return:
        """
        logging.info("start signal...")
        signal.signal(signal.SIGCHLD, self.handle_sigchild)  # linux 才可以设置忽略
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        signal.signal(signal.SIGINT, self.handle_sigint)
        signal.signal(signal.SIGHUP, self.handle_sighup)
        signal.signal(signal.SIGQUIT, self.handle_sigquit)

    def handle_sigalarm(self, sig, frame):
        """
        检查本master 分配的任务 超过2*hearttime 未执行 则重新分配
        :param sig:
        :param frame:
        :return:
        """
        # self.check_child_processes() #支持动态调整work个数
        pass

    def handle_sigint(self, sig, frame):
        """
        暴力关闭
        父进程关闭 子进程由于设置了daemon=0 也会关闭
        :param sig:
        :param frame:
        :return:
        """
        os._exit(0)

    def handle_sigterm(self, sig, frame):
        """
        暴力关闭
        父进程关闭 子进程由于设置了daemon=0 也会关闭
        :param sig:
        :param frame:
        :return:
        """
        os._exit(0)  # daemon=True 的子进程会被终止

    def handle_sigquit(self, sig, frame):
        """
        从容关闭
        :param sig:
        :param frame:
        :return:
        """
        for p in multiprocessing.active_children():
            os.kill(p.pid, signal.SIGQUIT)
            p.join()  # 子进程
        self.is_running = 0  # respawn 不会重新拉起子进程

    def handle_sighup(self, sig, frame):
        """
        优雅重启
        :param sig:
        :param frame:
        :return:
        """
        for p in multiprocessing.active_children():
            os.kill(p.pid, signal.SIGHUP)
            p.join()  # 子进程
        self.is_running = 1  # respawn 会自动重新拉起子进程

    def handle_sigchild(self, sig, frame):
        """
         此信号捕获 不处理，通过另外方式 检测子进程异常关闭 重启它

        :param sig:
        :param frame:
        :return:
        """
        pass
