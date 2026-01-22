import scrapy
import json
from urllib.parse import urlencode

import datetime
import logging


# Set up logger once at module level to prevent handler accumulation
_url_logger = logging.getLogger(f"{__name__}.url_logger")
_url_logger.setLevel(logging.INFO)
if not _url_logger.handlers:
    _handler = logging.FileHandler("sauto_spider.log")
    _handler.setLevel(logging.INFO)
    _handler.setFormatter(logging.Formatter("%(message)s"))
    _url_logger.addHandler(_handler)


def log_url(func):
    def wrapper(self, *args, **kwargs):
        result = func(self, *args, **kwargs)
        for request in result:
            _url_logger.info(f"Date: {datetime.datetime.now()}, scraping url: {request.url}")
            yield request
    return wrapper


class SautoSpider(scrapy.Spider):
    name = "sauto"
    BASE_URL = "https://www.sauto.cz/api/v1/items/search?"

    # pokud by detail endpoint nefungoval -> upravíme jen tenhle řádek
    DETAIL_API_URL = "https://www.sauto.cz/api/v1/items/{}"

    @staticmethod
    def read_params_from_json(file_path: str) -> dict:
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)

    @staticmethod
    def _norm_str(x):
        if x is None:
            return None
        s = str(x).strip()
        if not s or s.lower() == "null":
            return None
        return s

    def _load_strict_filters(self, params: dict):
        # bere přesně z params.json (jak máš na obrázku)
        self.strict_manufacturer_seo = self._norm_str(params.get("manufacturer_seo_name"))
        self.strict_model_seo = self._norm_str(params.get("model_seo_name"))
        self.strict_seller_type = self._norm_str(params.get("seller_type"))  # "soukromy" / "bazar"

        self.logger.info(
            f"Strict filter loaded: manufacturer={self.strict_manufacturer_seo}, "
            f"model={self.strict_model_seo}, seller_type={self.strict_seller_type}"
        )

    def _passes_strict_filter(self, item: dict) -> bool:
        """
        Tohle je to, co ti garantuje že nikdy neprojde Škoda/Opel atd.
        """
        m_cb = item.get("manufacturer_cb") or {}
        mo_cb = item.get("model_cb") or {}

        m_seo = m_cb.get("seo_name")
        mo_seo = mo_cb.get("seo_name")

        if self.strict_manufacturer_seo and m_seo != self.strict_manufacturer_seo:
            return False
        if self.strict_model_seo and mo_seo != self.strict_model_seo:
            return False

        if self.strict_seller_type:
            is_bazar = bool(item.get("premise"))
            if self.strict_seller_type == "bazar" and not is_bazar:
                return False
            if self.strict_seller_type == "soukromy" and is_bazar:
                return False

        return True

    def _extract_total(self, data: dict) -> int:
        candidates = [
            ("pagination", "total"),
            ("meta", "total"),
            ("data", "total"),
            ("total",),
        ]

        for path in candidates:
            cur = data
            ok = True
            for key in path:
                if isinstance(cur, dict) and key in cur:
                    cur = cur[key]
                else:
                    ok = False
                    break
            if ok:
                try:
                    return int(cur)
                except Exception:
                    pass

        return -1

    @log_url
    def start_requests(self):
        params = self.read_params_from_json("params.json")

        # načti strict filtry (lokální ochrana proti mixování značek)
        self._load_strict_filters(params)

        # IMPORTANT: params posíláme na API tak jak jsou
        # (jen vynutíme offset string)
        params["offset"] = str(params.get("offset", "0"))

        url = f"{self.BASE_URL}{urlencode(params)}"
        yield scrapy.Request(
            url=url,
            method="GET",
            callback=self.parse_search,
            errback=self.handle_error,
            meta={"params": params},
            dont_filter=True,
        )

    # -------------------------
    # SEARCH PARSE + pagination
    # -------------------------
    def parse_search(self, response):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON response (search)")
            return

        results = data.get("results", []) or []

        # 1) zpracuj výsledky (ale jen ty co projdou strict filtrem)
        for r in results:
            if not self._passes_strict_filter(r):
                continue

            manufacturer = (r.get("manufacturer_cb") or {}).get("seo_name")
            model = (r.get("model_cb") or {}).get("seo_name")
            ad_id = r.get("id")

            r["manufacturer_name"] = (r.get("manufacturer_cb") or {}).get("name")
            r["model_name"] = (r.get("model_cb") or {}).get("name")

            r["seller_type"] = "bazar" if r.get("premise") else "soukromy"

            if manufacturer and model and ad_id:
                r["url"] = f"https://www.sauto.cz/osobni/detail/{manufacturer}/{model}/{ad_id}"
            else:
                r["url"] = None

            # ---- DETAIL FETCH ----
            if ad_id:
                detail_url = self.DETAIL_API_URL.format(ad_id)
                yield scrapy.Request(
                    url=detail_url,
                    method="GET",
                    callback=self.parse_detail,
                    errback=self.handle_detail_error,
                    meta={"base_item": r},
                    dont_filter=True,
                )
            else:
                r["detail_fetch_ok"] = False
                r["detail_raw"] = None
                yield r

        # 2) pagination (offset + limit)
        params = (response.meta.get("params") or {}).copy()
        limit = int(params.get("limit", 35))
        offset = int(params.get("offset", 0))

        total = self._extract_total(data)

        if total == -1:
            # fallback: pokračuj dokud chodí plný stránky
            if len(results) == limit and limit > 0:
                params["offset"] = str(offset + limit)
                next_url = f"{self.BASE_URL}{urlencode(params)}"
                yield scrapy.Request(
                    url=next_url,
                    method="GET",
                    callback=self.parse_search,
                    errback=self.handle_error,
                    meta={"params": params},
                    dont_filter=True,
                )
            return

        next_offset = offset + limit
        if next_offset < total:
            params["offset"] = str(next_offset)
            next_url = f"{self.BASE_URL}{urlencode(params)}"
            yield scrapy.Request(
                url=next_url,
                method="GET",
                callback=self.parse_search,
                errback=self.handle_error,
                meta={"params": params},
                dont_filter=True,
            )

    # -------------------------
    # DETAIL PARSE (store full json)
    # -------------------------
    def parse_detail(self, response):
        base_item = response.meta.get("base_item") or {}

        try:
            detail = json.loads(response.text)
        except json.JSONDecodeError:
            base_item["detail_fetch_ok"] = False
            base_item["detail_raw"] = None
            yield base_item
            return

        base_item["detail_fetch_ok"] = True
        base_item["detail_raw"] = detail
        yield base_item

    def handle_detail_error(self, failure):
        base_item = failure.request.meta.get("base_item") or {}
        base_item["detail_fetch_ok"] = False
        base_item["detail_raw"] = None
        base_item["detail_error"] = str(failure.value)
        yield base_item

    def handle_error(self, failure):
        request = failure.request
        self.logger.error(f"Request failed: {request.url}, Error: {failure.value}")
