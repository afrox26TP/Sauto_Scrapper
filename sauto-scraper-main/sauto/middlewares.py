import os
import random
import logging
from collections import deque
from typing import Iterable
from urllib.parse import urlsplit

from fake_useragent import UserAgent
from scrapy import Request, signals
from scrapy.exceptions import IgnoreRequest
from scrapy.downloadermiddlewares.retry import get_retry_request


def _parse_proxy_list(raw_value: str) -> list[str]:
    proxies: list[str] = []
    for chunk in raw_value.replace("\n", ",").split(","):
        value = chunk.strip()
        if value:
            proxies.append(value)
    return proxies


class RandomUserAgentMiddleware:
    """Middleware to rotate User-Agent for each request using fake-useragent."""

    FALLBACK_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

    def __init__(self):
        self.ua = UserAgent(fallback=self.FALLBACK_UA)

    def process_request(self, request, spider):
        request.headers["User-Agent"] = self.ua.random


class RotatingProxyMiddleware:
    """Assigns proxies to requests and retries with a different proxy on ban-like responses."""

    DEFAULT_BAN_STATUSES = {403, 407, 429, 500, 502, 503, 504}
    RETRY_EXCEPTIONS = ("TimeoutError", "TCPTimedOutError", "ConnectionRefusedError", "ConnectionDone")
    LOGGER = logging.getLogger(__name__)
    ALLOWED_PROXY_SCHEMES = {"http", "https", "socks5h"}

    def __init__(self, proxies: Iterable[str], mode: str, ban_statuses: set[int], strict_mode: bool, request_timeout: float):
        self.mode = (mode or "round_robin").strip().lower()
        self.ban_statuses = ban_statuses or set(self.DEFAULT_BAN_STATUSES)
        self.proxies = deque(proxies)
        self._enabled = bool(self.proxies)
        self.strict_mode = strict_mode
        self.request_timeout = max(1.0, float(request_timeout or 8.0))

    @classmethod
    def _is_truthy(cls, value: str | None, default: bool = False) -> bool:
        if value is None:
            return default
        return str(value).strip().lower() not in {"", "0", "false", "no", "off"}

    @classmethod
    def _sanitize_proxies(cls, proxies: list[str]) -> list[str]:
        safe: list[str] = []
        for proxy in proxies:
            parsed = urlsplit(proxy)
            scheme = (parsed.scheme or "").lower()
            if scheme not in cls.ALLOWED_PROXY_SCHEMES:
                cls.LOGGER.warning(
                    "Ignoring proxy with unsupported scheme '%s': %s",
                    scheme or "<missing>",
                    proxy,
                )
                continue
            # socks5 (without 'h') can leak DNS because resolution may happen locally.
            if scheme == "socks5":
                cls.LOGGER.warning("Ignoring socks5 proxy (use socks5h to force remote DNS): %s", proxy)
                continue
            safe.append(proxy)
        return safe

    @classmethod
    def from_crawler(cls, crawler):
        list_env = os.getenv("SAUTO_PROXY_LIST") or os.getenv("PROXY_LIST") or ""
        one_env = os.getenv("SAUTO_PROXY_URL") or os.getenv("PROXY_URL") or ""
        mode = os.getenv("SAUTO_PROXY_MODE", "round_robin")
        ban_statuses_raw = os.getenv("SAUTO_PROXY_BAN_STATUSES", "")

        proxies = _parse_proxy_list(list_env)
        if one_env.strip():
            proxies.append(one_env.strip())
        proxies = cls._sanitize_proxies(proxies)

        strict_mode = cls._is_truthy(os.getenv("SAUTO_PROXY_STRICT"), default=bool(proxies))
        request_timeout = os.getenv("SAUTO_PROXY_TIMEOUT", "8")

        parsed_ban_statuses: set[int] = set()
        for token in ban_statuses_raw.replace(" ", "").split(","):
            if not token:
                continue
            try:
                parsed_ban_statuses.add(int(token))
            except ValueError:
                continue

        middleware = cls(
            proxies=proxies,
            mode=mode,
            ban_statuses=parsed_ban_statuses or set(cls.DEFAULT_BAN_STATUSES),
            strict_mode=strict_mode,
            request_timeout=float(request_timeout or 8),
        )
        if middleware._enabled:
            crawler.stats.set_value("proxy_pool/size", len(middleware.proxies))
            crawler.stats.set_value("proxy_pool/strict_mode", bool(middleware.strict_mode))
            cls.LOGGER.info(
                "RotatingProxyMiddleware enabled with %s proxies (mode=%s, strict=%s, timeout=%ss).",
                len(middleware.proxies),
                middleware.mode,
                middleware.strict_mode,
                middleware.request_timeout,
            )
        else:
            if middleware.strict_mode:
                cls.LOGGER.warning(
                    "RotatingProxyMiddleware running in strict mode but no proxy is configured. "
                    "Requests will be blocked to prevent direct fallback."
                )
            else:
                cls.LOGGER.info("RotatingProxyMiddleware disabled (no proxies configured).")
        return middleware

    def _choose_proxy(self, current_proxy: str | None = None) -> str | None:
        if not self.proxies:
            return None
        if self.mode == "random":
            candidates = [proxy for proxy in self.proxies if proxy != current_proxy] or list(self.proxies)
            return random.choice(candidates)

        if current_proxy and len(self.proxies) > 1 and self.proxies[0] == current_proxy:
            self.proxies.rotate(-1)
        selected = self.proxies[0]
        self.proxies.rotate(-1)
        return selected

    def process_request(self, request: Request, spider):
        request.meta.setdefault("download_timeout", self.request_timeout)

        if not self._enabled:
            if self.strict_mode and not request.meta.get("disable_proxy"):
                raise IgnoreRequest("Proxy strict mode is enabled and no proxy is configured.")
            return None
        if request.meta.get("disable_proxy"):
            if self.strict_mode:
                raise IgnoreRequest("Proxy strict mode forbids disable_proxy requests.")
            return None
        if request.meta.get("proxy"):
            return None

        proxy = self._choose_proxy()
        if not proxy:
            return None
        request.meta["proxy"] = proxy
        return None

    def process_response(self, request: Request, response, spider):
        if not self._enabled:
            return response

        if response.status not in self.ban_statuses:
            return response

        retry_request = get_retry_request(
            request,
            spider=spider,
            reason=f"proxy_blocked_status_{response.status}",
        )
        if retry_request is None:
            return response

        old_proxy = request.meta.get("proxy")
        new_proxy = self._choose_proxy(current_proxy=old_proxy)
        if not new_proxy:
            if self.strict_mode:
                raise IgnoreRequest("No proxy available for retry in strict mode.")
            return response

        retry_request.meta["proxy"] = new_proxy
        retry_request.meta.setdefault("download_timeout", self.request_timeout)
        return retry_request

    def process_exception(self, request: Request, exception, spider):
        if not self._enabled:
            return None

        exception_name = type(exception).__name__
        if exception_name not in self.RETRY_EXCEPTIONS:
            return None

        retry_request = get_retry_request(
            request,
            spider=spider,
            reason=f"proxy_exception_{exception_name}",
        )
        if retry_request is None:
            return None

        old_proxy = request.meta.get("proxy")
        new_proxy = self._choose_proxy(current_proxy=old_proxy)
        if not new_proxy:
            if self.strict_mode:
                raise IgnoreRequest("No proxy available after exception in strict mode.")
            return None

        retry_request.meta["proxy"] = new_proxy
        retry_request.meta.setdefault("download_timeout", self.request_timeout)
        return retry_request


class SautoSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class SautoDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)
