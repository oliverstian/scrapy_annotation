"""
This is the Scrapy engine which controls the Scheduler, Downloader and Spiders.

For more information see docs/topics/architecture.rst

"""
import logging
from time import time

from twisted.internet import defer, task
from twisted.python.failure import Failure

from scrapy import signals
from scrapy.core.scraper import Scraper
from scrapy.exceptions import DontCloseSpider
from scrapy.http import Response, Request
from scrapy.utils.misc import load_object
from scrapy.utils.reactor import CallLaterOnce
from scrapy.utils.log import logformatter_adapter, failure_to_exc_info

logger = logging.getLogger(__name__)


class Slot(object):

    def __init__(self, start_requests, close_if_idle, nextcall, scheduler):
        self.closing = False
        self.inprogress = set() # requests in progress
        self.start_requests = iter(start_requests)
        self.close_if_idle = close_if_idle
        self.nextcall = nextcall
        self.scheduler = scheduler
        """
        使用Twisted的主循环reactor来不断的调度执行Engine的"_next_request"方法
        """
        self.heartbeat = task.LoopingCall(nextcall.schedule)

    def add_request(self, request):
        self.inprogress.add(request)

    def remove_request(self, request):
        self.inprogress.remove(request)
        self._maybe_fire_closing()

    def close(self):
        self.closing = defer.Deferred()
        self._maybe_fire_closing()
        return self.closing

    def _maybe_fire_closing(self):
        if self.closing and not self.inprogress:
            if self.nextcall:
                self.nextcall.cancel()
                if self.heartbeat.running:
                    self.heartbeat.stop()
            self.closing.callback(None)


class ExecutionEngine(object):

    def __init__(self, crawler, spider_closed_callback):
        self.crawler = crawler
        self.settings = crawler.settings
        self.signals = crawler.signals
        self.logformatter = crawler.logformatter
        self.slot = None
        self.spider = None
        self.running = False
        self.paused = False
        self.scheduler_cls = load_object(self.settings['SCHEDULER'])  # 从settings中找到Scheduler调度器，找到Scheduler类
        downloader_cls = load_object(self.settings['DOWNLOADER'])  # 同样，找到Downloader下载器类
        self.downloader = downloader_cls(crawler)  # 实例化Downloader
        self.scraper = Scraper(crawler)   # 实例化Scraper，它是引擎连接爬虫类的桥梁
        self._spider_closed_callback = spider_closed_callback

    @defer.inlineCallbacks
    def start(self):
        """Start the execution engine"""
        assert not self.running, "Engine already running"
        self.start_time = time()
        yield self.signals.send_catch_log_deferred(signal=signals.engine_started)
        self.running = True
        self._closewait = defer.Deferred()
        yield self._closewait

    def stop(self):
        """Stop the execution engine gracefully"""
        assert self.running, "Engine not running"
        self.running = False
        dfd = self._close_all_spiders()
        return dfd.addBoth(lambda _: self._finish_stopping_engine())

    def close(self):
        """Close the execution engine gracefully.

        If it has already been started, stop it. In all cases, close all spiders
        and the downloader.
        """
        if self.running:
            # Will also close spiders and downloader
            return self.stop()
        elif self.open_spiders:
            # Will also close downloader
            return self._close_all_spiders()
        else:
            return defer.succeed(self.downloader.close())

    def pause(self):
        """Pause the execution engine"""
        self.paused = True

    def unpause(self):
        """Resume the execution engine"""
        self.paused = False

    def _next_request(self, spider):
        """
        这个_next_request方法有2种调用途径，一种是通过reactor的5s心跳定时启动运行，另一种则是在流程中需要时主动调用。
        """
        slot = self.slot
        if not slot:
            return

        if self.paused:
            return

        while not self._needs_backout(spider):  # 从scheduler中获取request，这个循环的意思是，尽量把队列中的request都安排异步下载，除非是达到最大并发量或其他原因
            if not self._next_request_from_scheduler(spider):  # 这个函数才是真正的发起下载任务
                break

        if slot.start_requests and not self._needs_backout(spider):  # 如果start_requests有数据且不需要等待
            try:
                request = next(slot.start_requests)
            except StopIteration:
                slot.start_requests = None
            except Exception:
                slot.start_requests = None
                logger.error('Error while obtaining start requests',
                             exc_info=True, extra={'spider': spider})
            else:
                self.crawl(request, spider)  # 调用crawl,实际是把request放入scheduler对象的内存队列中，然后又安排马上调用_next_request

        if self.spider_is_idle(spider) and slot.close_if_idle:
            self._spider_idle(spider)

    def _needs_backout(self, spider):
        """
        是否需要等待，取决4个条件
        1. Engine是否stop
        2. slot是否close
        3. downloader下载超过预设
        4. scraper处理response超过预设
        """
        slot = self.slot
        return not self.running \
            or slot.closing \
            or self.downloader.needs_backout() \
            or self.scraper.slot.needs_backout()

    def _next_request_from_scheduler(self, spider):
        slot = self.slot
        request = slot.scheduler.next_request()  # 从scheduler拿出下个request
        if not request:
            return
        d = self._download(request, spider)  # 处理各中间件的process_request方法，然后用self._enqueue_request将request发出下载，并绑定回调函数（如process_response）
        d.addBoth(self._handle_downloader_output, request, spider)
        d.addErrback(lambda f: logger.info('Error while handling downloader output',
                                           exc_info=failure_to_exc_info(f),
                                           extra={'spider': spider}))
        d.addBoth(lambda _: slot.remove_request(request))
        d.addErrback(lambda f: logger.info('Error while removing request from slot',
                                           exc_info=failure_to_exc_info(f),
                                           extra={'spider': spider}))
        d.addBoth(lambda _: slot.nextcall.schedule())
        d.addErrback(lambda f: logger.info('Error while scheduling new request',
                                           exc_info=failure_to_exc_info(f),
                                           extra={'spider': spider}))
        return d

    def _handle_downloader_output(self, response, request, spider):
        assert isinstance(response, (Request, Response, Failure)), response  # 下载结果必须是Request、Response、Failure其一
        # downloader middleware can return requests (for example, redirects)
        if isinstance(response, Request):  # 如果是Request，则再次调用crawl，执行Scheduler的入队逻辑
            self.crawl(response, spider)
            return
        # response is a Response or Failure
        d = self.scraper.enqueue_scrape(response, request, spider)  # 主要是和Spiders和Pipeline交互
        d.addErrback(lambda f: logger.error('Error while enqueuing downloader output',
                                            exc_info=failure_to_exc_info(f),
                                            extra={'spider': spider}))
        return d

    def spider_is_idle(self, spider):
        if not self.scraper.slot.is_idle():
            # scraper is not idle
            return False

        if self.downloader.active:
            # downloader has pending requests
            return False

        if self.slot.start_requests is not None:
            # not all start requests are handled
            return False

        if self.slot.scheduler.has_pending_requests():
            # scheduler has pending requests
            return False

        return True

    @property
    def open_spiders(self):
        return [self.spider] if self.spider else []

    def has_capacity(self):
        """Does the engine have capacity to handle more spiders"""
        return not bool(self.slot)

    def crawl(self, request, spider):
        assert spider in self.open_spiders, \
            "Spider %r not opened when crawling: %s" % (spider.name, request)
        self.schedule(request, spider)  # 把该requests放在内存队列中（前提是指纹唯一，指纹不唯一则drop掉）
        self.slot.nextcall.schedule()  # 下一个loop马上调用_next_request，这样就会一直去找spider中的request然后放入队列

    def schedule(self, request, spider):
        self.signals.send_catch_log(signal=signals.request_scheduled,
                request=request, spider=spider)
        if not self.slot.scheduler.enqueue_request(request):  # request指纹重复则drop，指纹唯一则将request入队
            self.signals.send_catch_log(signal=signals.request_dropped,
                                        request=request, spider=spider)

    def download(self, request, spider):  # 没有调用这个函数？？？
        d = self._download(request, spider)
        d.addBoth(self._downloaded, self.slot, request, spider)
        return d

    def _downloaded(self, response, slot, request, spider):  # 没有调用这个函数？？？
        slot.remove_request(request)
        return self.download(response, spider) \
                if isinstance(response, Request) else response  # 如果返回的是个request（比如从中间件返回），那就再次下载，如果是response则直接返回

    def _download(self, request, spider):
        slot = self.slot
        slot.add_request(request)  # 添加到正在处理的request的集合
        def _on_success(response):
            assert isinstance(response, (Response, Request))
            if isinstance(response, Response):  # 如果下载后结果为Response,返回Response
                response.request = request # tie request to response received
                logkws = self.logformatter.crawled(request, response, spider)
                logger.log(*logformatter_adapter(logkws), extra={'spider': spider})
                self.signals.send_catch_log(signal=signals.response_received, \
                    response=response, request=request, spider=spider)
            return response

        def _on_complete(_):
            slot.nextcall.schedule()  # 这里一个重点，下载完成之后再次调度，即，再次取出request，然后发送请求
            return _

        dwld = self.downloader.fetch(request, spider)  # 处理各中间件的process_request方法，然后用self._enqueue_request将request发出下载，并绑定回调（如process_response）
        dwld.addCallbacks(_on_success)  # 绑定了内部函数，其实就相当于闭包了，所以变量啥的会保存下来
        dwld.addBoth(_on_complete)
        return dwld

    @defer.inlineCallbacks
    def open_spider(self, spider, start_requests=(), close_if_idle=True):
        assert self.has_capacity(), "No free spider slot when opening %r" % \
            spider.name
        logger.info("Spider opened", extra={'spider': spider})
        nextcall = CallLaterOnce(self._next_request, spider)  # 注册_next_request调度方法，循环调度
        scheduler = self.scheduler_cls.from_crawler(self.crawler)  # 初始化scheduler
        start_requests = yield self.scraper.spidermw.process_start_requests(start_requests, spider)  # 调用爬虫中间件，处理种子请求
        slot = Slot(start_requests, close_if_idle, nextcall, scheduler)  # 封装Slot对象
        self.slot = slot
        self.spider = spider
        yield scheduler.open(spider)  # 调用scheduler的open，实例化一个优先级队列，其余啥也没干，返回None
        yield self.scraper.open_spider(spider)  # 这里主要是调用所有itemPipline的open_spider方法
        self.crawler.stats.open_spider(spider)  # 返回None，这个不知道干啥的
        yield self.signals.send_catch_log_deferred(signals.spider_opened, spider=spider)
        slot.nextcall.schedule()  # 发起调度
        slot.heartbeat.start(5)  # 每5秒调用一次CallLaterOnce.schedule

    def _spider_idle(self, spider):
        """Called when a spider gets idle. This function is called when there
        are no remaining pages to download or schedule. It can be called
        multiple times. If some extension raises a DontCloseSpider exception
        (in the spider_idle signal handler) the spider is not closed until the
        next loop and this function is guaranteed to be called (at least) once
        again for this spider.
        """
        res = self.signals.send_catch_log(signal=signals.spider_idle, \
            spider=spider, dont_log=DontCloseSpider)
        if any(isinstance(x, Failure) and isinstance(x.value, DontCloseSpider) \
                for _, x in res):
            return

        if self.spider_is_idle(spider):
            self.close_spider(spider, reason='finished')

    def close_spider(self, spider, reason='cancelled'):
        """Close (cancel) spider and clear all its outstanding requests"""

        slot = self.slot
        if slot.closing:
            return slot.closing
        logger.info("Closing spider (%(reason)s)",
                    {'reason': reason},
                    extra={'spider': spider})

        dfd = slot.close()

        def log_failure(msg):
            def errback(failure):
                logger.error(
                    msg,
                    exc_info=failure_to_exc_info(failure),
                    extra={'spider': spider}
                )
            return errback

        dfd.addBoth(lambda _: self.downloader.close())
        dfd.addErrback(log_failure('Downloader close failure'))

        dfd.addBoth(lambda _: self.scraper.close_spider(spider))
        dfd.addErrback(log_failure('Scraper close failure'))

        dfd.addBoth(lambda _: slot.scheduler.close(reason))
        dfd.addErrback(log_failure('Scheduler close failure'))

        dfd.addBoth(lambda _: self.signals.send_catch_log_deferred(
            signal=signals.spider_closed, spider=spider, reason=reason))
        dfd.addErrback(log_failure('Error while sending spider_close signal'))

        dfd.addBoth(lambda _: self.crawler.stats.close_spider(spider, reason=reason))
        dfd.addErrback(log_failure('Stats close failure'))

        dfd.addBoth(lambda _: logger.info("Spider closed (%(reason)s)",
                                          {'reason': reason},
                                          extra={'spider': spider}))

        dfd.addBoth(lambda _: setattr(self, 'slot', None))
        dfd.addErrback(log_failure('Error while unassigning slot'))

        dfd.addBoth(lambda _: setattr(self, 'spider', None))
        dfd.addErrback(log_failure('Error while unassigning spider'))

        dfd.addBoth(lambda _: self._spider_closed_callback(spider))

        return dfd

    def _close_all_spiders(self):
        dfds = [self.close_spider(s, reason='shutdown') for s in self.open_spiders]
        dlist = defer.DeferredList(dfds)
        return dlist

    @defer.inlineCallbacks
    def _finish_stopping_engine(self):
        yield self.signals.send_catch_log_deferred(signal=signals.engine_stopped)
        self._closewait.callback(None)
