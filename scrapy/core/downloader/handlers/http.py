"""Download handlers for http and https schemes"""

from twisted.protocols.policies import ThrottlingFactory
from twisted.internet import reactor

from scrapy.exceptions import NotSupported
from scrapy.utils.misc import load_object
from scrapy.conf import settings
from scrapy import optional_features

ssl_supported = 'ssl' in optional_features

HTTPClientFactory = load_object(settings['DOWNLOADER_HTTPCLIENTFACTORY'])
ClientContextFactory = load_object(settings['DOWNLOADER_CLIENTCONTEXTFACTORY'])


class HttpDownloadHandler(object):

    def __init__(self, httpclientfactory=HTTPClientFactory):
        self.httpclientfactory = httpclientfactory
        self.read_throttling_rate = settings.getfloat('DOWNLOAD_THROTTLE_KB') * 1024

    def download_request(self, request, spider):
        """Return a deferred for the HTTP download"""
        factory = self.httpclientfactory(request)
        tfactory = ThrottlingFactory(factory, readLimit=self.read_throttling_rate)
        self._connect(tfactory)
        return factory.deferred

    def _connect(self, tfactory):
        factory = tfactory.wrappedFactory
        host, port = factory.host, factory.port
        if factory.scheme == 'https':
            if ssl_supported:
                return reactor.connectSSL(host, port, tfactory, \
                        ClientContextFactory())
            raise NotSupported("HTTPS not supported: install pyopenssl library")
        else:
            return reactor.connectTCP(host, port, tfactory)
