import os
import json
import logging
import warnings
from os.path import join, exists

from queuelib import PriorityQueue

from scrapy.utils.misc import load_object, create_instance
from scrapy.utils.job import job_dir
from scrapy.utils.deprecate import ScrapyDeprecationWarning


logger = logging.getLogger(__name__)


class Scheduler(object):
    """
    Scrapy Scheduler. It allows to enqueue requests and then get
    a next request to download. Scheduler is also handling duplication
    filtering, via dupefilter.

    Prioritization and queueing is not performed by the Scheduler.
    User sets ``priority`` field for each Request, and a PriorityQueue
    (defined by :setting:`SCHEDULER_PRIORITY_QUEUE`) uses these priorities
    to dequeue requests in a desired order.

    Scheduler uses two PriorityQueue instances, configured to work in-memory
    and on-disk (optional). When on-disk queue is present, it is used by
    default, and an in-memory queue is used as a fallback for cases where
    a disk queue can't handle a request (can't serialize it).

    :setting:`SCHEDULER_MEMORY_QUEUE` and
    :setting:`SCHEDULER_DISK_QUEUE` allow to specify lower-level queue classes
    which PriorityQueue instances would be instantiated with, to keep requests
    on disk and in memory respectively.

    Overall, Scheduler is an object which holds several PriorityQueue instances
    (in-memory and on-disk) and implements fallback logic for them.
    Also, it handles dupefilters.
    """
    def __init__(self, dupefilter, jobdir=None, dqclass=None, mqclass=None,
                 logunser=False, stats=None, pqclass=None, crawler=None):
        """
        调度器初始化主要做了两件事：
        1、实例化请求指纹过滤器：用来过滤重复请求，可自己重写替换之；
        2、定义各种不同类型的任务队列：优先级任务队列、基于磁盘的任务队列、基于内存的任务队列；
        
        如果用户在配置文件中定义了JOBDIR，那么则每次把任务队列保存在磁盘中，下次启动时自动加载。
        如果没有定义，那么则使用的是内存队列。

        默认定义的这些队列结构都是后进先出的，也就是说：Scrapy默认的采集规则是深度优先采集！
        如果要改为广度优先采集（即先进先出），需要在settings那里把SCHEDULER_DISK_QUEUE改为
        scrapy.squeues.PickleFifoDiskQueue
        """
        self.df = dupefilter  # 指纹过滤器
        self.dqdir = self._dqdir(jobdir)  # 任务队列文件夹
        self.pqclass = pqclass  # 优先级任务队列类
        self.dqclass = dqclass  # 磁盘任务队列类
        self.mqclass = mqclass  # 内存任务队列类
        self.logunser = logunser  # 日志是否序列化
        self.stats = stats
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        """
        调度器的初始化主要做了2件事：
        实例化请求指纹过滤器：用来过滤重复请求，可自己重写替换之；
        定义各种不同类型的任务队列：优先级任务队列、基于磁盘的任务队列、基于内存的任务队列；
        """
        settings = crawler.settings
        dupefilter_cls = load_object(settings['DUPEFILTER_CLASS'])  # 从配置文件中获取指纹过滤器类
        dupefilter = create_instance(dupefilter_cls, settings, crawler)  # 实例化指纹过滤器
        pqclass = load_object(settings['SCHEDULER_PRIORITY_QUEUE'])  # 基于优先级的任务队列类（priority queue）
        if pqclass is PriorityQueue:  # 子类is父类？？？
            warnings.warn("SCHEDULER_PRIORITY_QUEUE='queuelib.PriorityQueue'"
                          " is no longer supported because of API changes; "
                          "please use 'scrapy.pqueues.ScrapyPriorityQueue'",
                          ScrapyDeprecationWarning)
            from scrapy.pqueues import ScrapyPriorityQueue
            pqclass = ScrapyPriorityQueue

        dqclass = load_object(settings['SCHEDULER_DISK_QUEUE'])  # 基于磁盘的任务队列类（disk queue）
        mqclass = load_object(settings['SCHEDULER_MEMORY_QUEUE'])  # 基于内存的任务队列类（memory queue）
        logunser = settings.getbool('LOG_UNSERIALIZABLE_REQUESTS',
                                    settings.getbool('SCHEDULER_DEBUG'))  # 请求日志序列化开关
        return cls(dupefilter, jobdir=job_dir(settings), logunser=logunser,
                   stats=crawler.stats, pqclass=pqclass, dqclass=dqclass,
                   mqclass=mqclass, crawler=crawler)

    def has_pending_requests(self):
        return len(self) > 0

    def open(self, spider):
        self.spider = spider
        self.mqs = self._mq()  # 实例化一个优先级队列（等价于self.mqs = self.pqclass(self._newmq)）
        self.dqs = self._dq() if self.dqdir else None  # 如果定义了dqdir则实例化基于磁盘的队列
        return self.df.open()  # 调用请求指纹过滤器的open方法

    def close(self, reason):
        if self.dqs:
            state = self.dqs.close()
            self._write_dqs_state(self.dqdir, state)
        return self.df.close(reason)

    def enqueue_request(self, request):
        # 请求入队,若请求过滤器验证重复,返回False。这里可以看到request.dont_filter就是为了避开指纹过滤的
        if not request.dont_filter and self.df.request_seen(request):
            self.df.log(request, self.spider)
            return False
        dqok = self._dqpush(request)
        if dqok:  # 是否保存在磁盘
            self.stats.inc_value('scheduler/enqueued/disk', spider=self.spider)
        else:
            self._mqpush(request)  # 将该request保存在内存队列中
            self.stats.inc_value('scheduler/enqueued/memory', spider=self.spider)
        self.stats.inc_value('scheduler/enqueued', spider=self.spider)
        return True

    def next_request(self):
        request = self.mqs.pop()  # 从优先级队列（priorityQueue）中找到优先级最高的那个内存（或磁盘）队列，并从中弹出request
        if request:
            self.stats.inc_value('scheduler/dequeued/memory', spider=self.spider)
        else:
            request = self._dqpop()  # 从磁盘中找
            if request:
                self.stats.inc_value('scheduler/dequeued/disk', spider=self.spider)
        if request:
            self.stats.inc_value('scheduler/dequeued', spider=self.spider)
        return request

    def __len__(self):
        return len(self.dqs) + len(self.mqs) if self.dqs else len(self.mqs)

    def _dqpush(self, request):
        if self.dqs is None:
            return
        try:
            self.dqs.push(request, -request.priority)
        except ValueError as e:  # non serializable request
            if self.logunser:
                msg = ("Unable to serialize request: %(request)s - reason:"
                       " %(reason)s - no more unserializable requests will be"
                       " logged (stats being collected)")
                logger.warning(msg, {'request': request, 'reason': e},
                               exc_info=True, extra={'spider': self.spider})
                self.logunser = False
            self.stats.inc_value('scheduler/unserializable',
                                 spider=self.spider)
            return
        else:
            return True

    def _mqpush(self, request):
        self.mqs.push(request, -request.priority)

    def _dqpop(self):
        if self.dqs:
            return self.dqs.pop()

    def _newmq(self, priority):
        """ Factory for creating memory queues. """
        return self.mqclass()

    def _newdq(self, priority):
        """ Factory for creating disk queues. """
        path = join(self.dqdir, 'p%s' % (priority, ))
        return self.dqclass(path)

    def _mq(self):
        """ Create a new priority queue instance, with in-memory storage """
        return create_instance(self.pqclass, None, self.crawler, self._newmq,
                               serialize=False)

    def _dq(self):
        """ Create a new priority queue instance, with disk storage """
        state = self._read_dqs_state(self.dqdir)
        q = create_instance(self.pqclass,
                            None,
                            self.crawler,
                            self._newdq,
                            state,
                            serialize=True)
        if q:
            logger.info("Resuming crawl (%(queuesize)d requests scheduled)",
                        {'queuesize': len(q)}, extra={'spider': self.spider})
        return q

    def _dqdir(self, jobdir):
        """ Return a folder name to keep disk queue state at """
        if jobdir:
            dqdir = join(jobdir, 'requests.queue')
            if not exists(dqdir):
                os.makedirs(dqdir)
            return dqdir

    def _read_dqs_state(self, dqdir):
        path = join(dqdir, 'active.json')
        if not exists(path):
            return ()
        with open(path) as f:
            return json.load(f)

    def _write_dqs_state(self, dqdir, state):
        with open(join(dqdir, 'active.json'), 'w') as f:
            json.dump(state, f)
