"""Download handlers for different schemes"""

import logging
from twisted.internet import defer
import six
from scrapy.exceptions import NotSupported, NotConfigured
from scrapy.utils.httpobj import urlparse_cached
from scrapy.utils.misc import load_object
from scrapy.utils.python import without_none_values
from scrapy import signals


logger = logging.getLogger(__name__)


class DownloadHandlers(object):

    def __init__(self, crawler):
        """
        就是需下载的资源是什么类型，就选用哪一种下载处理器进行网络下载，其中最常用的就是http和https对应的处理器。
        """
        self._crawler = crawler
        self._schemes = {}  # stores acceptable schemes on instancing 存储scheme对应的类路径，后面用于实例化
        self._handlers = {}  # stores instanced handlers for schemes 存储scheme对应的下载器
        self._notconfigured = {}  # remembers failed handlers
        """
        从配置中找到DOWNLOAD_HANDLERS_BASE，构造下载处理器
        注意：这里是调用getwithbase方法，取的是配置中的XXXX_BASE配置。
        
        handlers就是包含了default_settins.py中DOWNLOAD_HANDLERS_BASE项下的所有handler，
        也就是根据下载资源的类型，采用不同的下载器，最常用的就是http和https了
        """
        handlers = without_none_values(
            crawler.settings.getwithbase('DOWNLOAD_HANDLERS'))
        for scheme, clspath in six.iteritems(handlers):  # 存储scheme对应的类路径，后面用于实例化
            self._schemes[scheme] = clspath  # 其实就是把handler复制了一遍，为啥不用深拷贝？clspath就是不同handler类的路径，可用于实例化
            self._load_handler(scheme, skip_lazy=True)  # 这里就把每种handler实例化了，保存在self._handlers中。所以不用深拷贝，这里还有实例化这个步骤

        crawler.signals.connect(self._close, signals.engine_stopped)

    def _get_handler(self, scheme):
        """Lazy-load the downloadhandler for a scheme
        only on the first request for that scheme.
        """
        if scheme in self._handlers:
            return self._handlers[scheme]
        if scheme in self._notconfigured:
            return None
        if scheme not in self._schemes:
            self._notconfigured[scheme] = 'no handler available for that scheme'
            return None

        return self._load_handler(scheme)

    def _load_handler(self, scheme, skip_lazy=False):
        path = self._schemes[scheme]
        try:
            dhcls = load_object(path)
            if skip_lazy and getattr(dhcls, 'lazy', True):
                return None
            dh = dhcls(self._crawler.settings)
        except NotConfigured as ex:
            self._notconfigured[scheme] = str(ex)
            return None
        except Exception as ex:
            logger.error('Loading "%(clspath)s" for scheme "%(scheme)s"',
                         {"clspath": path, "scheme": scheme},
                         exc_info=True, extra={'crawler': self._crawler})
            self._notconfigured[scheme] = str(ex)
            return None
        else:
            self._handlers[scheme] = dh
            return dh

    def download_request(self, request, spider):
        scheme = urlparse_cached(request).scheme  # 找到下载类型，比如http类型
        handler = self._get_handler(scheme)  # 根据类型找到对应的handler
        if not handler:
            raise NotSupported("Unsupported URL scheme '%s': %s" %
                               (scheme, self._notconfigured[scheme]))
        return handler.download_request(request, spider)  

    @defer.inlineCallbacks
    def _close(self, *_a, **_kw):
        for dh in self._handlers.values():
            if hasattr(dh, 'close'):
                yield dh.close()
