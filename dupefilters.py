from __future__ import print_function
import os
import logging

from scrapy.utils.job import job_dir
from scrapy.utils.request import referer_str, request_fingerprint

class BaseDupeFilter(object):

    @classmethod
    def from_settings(cls, settings):
        return cls()

    def request_seen(self, request):
        return False

    def open(self):  # can return deferred
        """可重写,完成过滤器的初始化工作"""
        pass

    def close(self, reason):  # can return a deferred
        pass

    def log(self, request, spider):  # log that a request has been filtered
        pass


class RFPDupeFilter(BaseDupeFilter):
    """
    主要作用：过滤重复请求，可自定义过滤规则。
    Request Fingerprint duplicates filter
    """

    def __init__(self, path=None, debug=False):
        self.file = None
        self.fingerprints = set()  # 指纹集合，使用的是set，基于内存
        self.logdupes = True
        self.debug = debug
        self.logger = logging.getLogger(__name__)
        if path:  # 请求指纹可存入磁盘，这个path需要自己在settings中配置，否则为None
            self.file = open(os.path.join(path, 'requests.seen'), 'a+')
            self.file.seek(0)
            self.fingerprints.update(x.rstrip() for x in self.file)

    @classmethod
    def from_settings(cls, settings):
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(job_dir(settings), debug)

    def request_seen(self, request):
        fp = self.request_fingerprint(request)  # 给这个request生成指纹，其实就是用算法生成hash值，不同的request值唯一
        if fp in self.fingerprints:  # 检测这个指纹是否唯一，不唯一则说明这个request重复了
            return True
        self.fingerprints.add(fp)  # 指纹唯一那就添加到fingerprints集合中（其实这里用列表页未必不可，因为添加之前已经检测是否重复了）
        if self.file:
            self.file.write(fp + os.linesep)

    def request_fingerprint(self, request):
        return request_fingerprint(request)

    def close(self, reason):
        if self.file:
            self.file.close()

    def log(self, request, spider):
        if self.debug:
            msg = "Filtered duplicate request: %(request)s (referer: %(referer)s)"
            args = {'request': request, 'referer': referer_str(request) }
            self.logger.debug(msg, args, extra={'spider': spider})
        elif self.logdupes:
            msg = ("Filtered duplicate request: %(request)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
            self.logdupes = False

        spider.crawler.stats.inc_value('dupefilter/filtered', spider=spider)
