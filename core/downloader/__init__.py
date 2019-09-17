from __future__ import absolute_import
import random
import warnings
from time import time
from datetime import datetime
from collections import deque

import six
from twisted.internet import reactor, defer, task

from scrapy.utils.defer import mustbe_deferred
from scrapy.utils.httpobj import urlparse_cached
from scrapy.resolver import dnscache
from scrapy import signals
from .middleware import DownloaderMiddlewareManager
from .handlers import DownloadHandlers


class Slot(object):
    """Downloader slot"""

    def __init__(self, concurrency, delay, randomize_delay):
        self.concurrency = concurrency
        self.delay = delay
        self.randomize_delay = randomize_delay

        self.active = set()  # 正在下载集合
        self.queue = deque()  # 下载任务队列
        self.transferring = set()
        self.lastseen = 0
        self.latercall = None

    def free_transfer_slots(self):
        return self.concurrency - len(self.transferring)  # 最大并发数减去正在下载的，就是还可以安排下载的数

    def download_delay(self):
        if self.randomize_delay:
            return random.uniform(0.5 * self.delay, 1.5 * self.delay)
        return self.delay

    def close(self):
        if self.latercall and self.latercall.active():
            self.latercall.cancel()

    def __repr__(self):
        cls_name = self.__class__.__name__
        return "%s(concurrency=%r, delay=%0.2f, randomize_delay=%r)" % (
            cls_name, self.concurrency, self.delay, self.randomize_delay)

    def __str__(self):
        return (
            "<downloader.Slot concurrency=%r delay=%0.2f randomize_delay=%r "
            "len(active)=%d len(queue)=%d len(transferring)=%d lastseen=%s>" % (
                self.concurrency, self.delay, self.randomize_delay,
                len(self.active), len(self.queue), len(self.transferring),
                datetime.fromtimestamp(self.lastseen).isoformat()
            )
        )


def _get_concurrency_delay(concurrency, spider, settings):
    delay = settings.getfloat('DOWNLOAD_DELAY')
    if hasattr(spider, 'download_delay'):
        delay = spider.download_delay

    if hasattr(spider, 'max_concurrent_requests'):
        concurrency = spider.max_concurrent_requests

    return concurrency, delay


class Downloader(object):

    DOWNLOAD_SLOT = 'download_slot'

    def __init__(self, crawler):
        self.settings = crawler.settings
        self.signals = crawler.signals
        self.slots = {}
        self.active = set()
        self.handlers = DownloadHandlers(crawler)  # 初始化DownloadHandlers
        self.total_concurrency = self.settings.getint('CONCURRENT_REQUESTS')  # 从配置中获取设置的并发数
        self.domain_concurrency = self.settings.getint('CONCURRENT_REQUESTS_PER_DOMAIN')  # 同一域名并发数
        self.ip_concurrency = self.settings.getint('CONCURRENT_REQUESTS_PER_IP')  # 同一IP并发数
        self.randomize_delay = self.settings.getbool('RANDOMIZE_DOWNLOAD_DELAY')  # 随机延迟下载时间
        self.middleware = DownloaderMiddlewareManager.from_crawler(crawler)  # 初始化下载器中间件
        self._slot_gc_loop = task.LoopingCall(self._slot_gc)
        self._slot_gc_loop.start(60)

    def fetch(self, request, spider):
        def _deactivate(response):
            self.active.remove(request)
            return response

        self.active.add(request)  # 正在下载的请求，这个用来统计并发量
        dfd = self.middleware.download(self._enqueue_request, request, spider)  # 处理各中间件的process_request方法，然后用self._enqueue_request将request发出下载
        return dfd.addBoth(_deactivate)  # 下载完成后，将该request从正在下载队列中移除

    def needs_backout(self):
        return len(self.active) >= self.total_concurrency

    def _get_slot(self, request, spider):
        key = self._get_slot_key(request, spider)
        if key not in self.slots:
            conc = self.ip_concurrency if self.ip_concurrency else self.domain_concurrency
            conc, delay = _get_concurrency_delay(conc, spider, self.settings)
            self.slots[key] = Slot(conc, delay, self.randomize_delay)  # 给每个域名都实例化一个Slot

        return key, self.slots[key]

    def _get_slot_key(self, request, spider):
        if self.DOWNLOAD_SLOT in request.meta:
            return request.meta[self.DOWNLOAD_SLOT]

        key = urlparse_cached(request).hostname or ''  # 找到域名
        if self.ip_concurrency:  # 同一ip下的并发量
            key = dnscache.get(key, key)

        return key

    def _enqueue_request(self, request, spider):
        """
        逻辑：
        1、为request新建一个deffered，然后把两者绑定在一个元组内一起加入待下载队列
        2、由于新加入了request需求，所以立马调用_process_queue，看是否满足下载条件（队列是否为空，是否达到最大下载并发量）
        3、_process_queue是一个可循环调度的函数，功能就是把下载队列的request发出下载，如果调用这个函数时不满足下载条件就直接返回了
        """
        key, slot = self._get_slot(request, spider)  # 加入下载请求队列
        request.meta[self.DOWNLOAD_SLOT] = key

        def _deactivate(response):
            slot.active.remove(request)
            return response

        slot.active.add(request)
        self.signals.send_catch_log(signal=signals.request_reached_downloader,
                                    request=request,
                                    spider=spider)
        deferred = defer.Deferred().addBoth(_deactivate)
        slot.queue.append((request, deferred))  # 这里只是把这个request加入下载队列
        self._process_queue(spider, slot)  # 激活_process_queue下载任务循环来不断调度下载任务
        return deferred  # 这个deferred需要等到_download中的dfd完成才能算完成（因为chain在一起）

    def _process_queue(self, spider, slot):
        if slot.latercall and slot.latercall.active():
            return

        # Delay queue processing if a download_delay is configured
        now = time()
        delay = slot.download_delay()
        if delay:  # 如果延迟下载参数有配置，则延迟处理队列
            penalty = delay - now + slot.lastseen
            if penalty > 0:
                slot.latercall = reactor.callLater(penalty, self._process_queue, spider, slot)  # 这里注册了一个回调事件到主循环中
                return

        # Process enqueued requests if there are free slots to transfer for this slot
        while slot.queue and slot.free_transfer_slots() > 0:  # 下载队列中有待下载request，并且未达到最大并发量
            slot.lastseen = now
            request, deferred = slot.queue.popleft()   # 从下载队列中取出下载请求
            dfd = self._download(slot, request, spider)  # 开始下载
            dfd.chainDeferred(deferred)  # 估计是dfd完成了，顺便也算deferred完成，所以说是chainDeferred
            # prevent burst if inter-request delays were configured
            if delay:
                self._process_queue(spider, slot)
                break

    def _download(self, slot, request, spider):
        # The order is very important for the following deferreds. Do not change!

        # 1. Create the download deferred
        dfd = mustbe_deferred(self.handlers.download_request, request, spider)

        # 2. Notify response_downloaded listeners about the recent download
        # before querying queue for next request
        def _downloaded(response):
            self.signals.send_catch_log(signal=signals.response_downloaded,
                                        response=response,
                                        request=request,
                                        spider=spider)
            return response
        dfd.addCallback(_downloaded)

        # 3. After response arrives,  remove the request from transferring
        # state to free up the transferring slot so it can be used by the
        # following requests (perhaps those which came from the downloader
        # middleware itself)
        slot.transferring.add(request)

        def finish_transferring(_):
            slot.transferring.remove(request)
            self._process_queue(spider, slot)  # 循环发起下载请求，即，这次下载完成，然后再调用_process_queue看是否还有request需要下载
            return _

        return dfd.addBoth(finish_transferring)

    def close(self):
        self._slot_gc_loop.stop()
        for slot in six.itervalues(self.slots):
            slot.close()

    def _slot_gc(self, age=60):
        mintime = time() - age
        for key, slot in list(self.slots.items()):
            if not slot.active and slot.lastseen + slot.delay < mintime:
                self.slots.pop(key).close()
