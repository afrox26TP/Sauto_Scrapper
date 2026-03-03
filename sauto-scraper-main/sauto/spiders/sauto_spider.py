import scrapy
import json
from urllib.parse import urlencode
import datetime
import logging
import re
import requests
import os

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
    DETAIL_API_URL = "https://www.sauto.cz/api/v1/items/{}"

    # --- DISCORD ---
    DISCORD_WEBHOOK_URL = 'https://discordapp.com/api/webhooks/1478178620991209615/dmix_llEFt-_C_K4KCKSwBe4tvR37XmDvzrdUhMh2UFOduZ0mua-6tGiSFizXvgn5m_U'
    NOTIFIED_FILE = 'notified_ids.json'

    # --- FILTRY ---
    BLACKLIST = ['bez stk', 'na díly', 'na nahradni dily', 'exekuce', 
                 'ťuklé', 'klepe', 'žere olej', 'nutný přepis', 'závada', 'projekt']

    PLUSOVE_BODY = {
        r'po dědovi|pozůstalost|dědictví': 100,
        r'garážováno|v zimě nejeto|v zimě neježděno': 50,
        r'servisní knížka|pravidelný servis': 30,
        r'nevyužité|stojí v garáži': 40,
        r'bez koroze|bez rzi': 40
    }

    MINUSOVE_BODY = {
        r'čip|chip|chiptuning|chiptunning': -50,
        r'zavařeno|zavařený diferenciál': -100,
        r'žere olej|klepe': -200
    }

    def __init__(self, *args, **kwargs):
        super(SautoSpider, self).__init__(*args, **kwargs)
        self.notified_ids = set()
        if os.path.exists(self.NOTIFIED_FILE):
            with open(self.NOTIFIED_FILE, 'r') as f:
                self.notified_ids = set(json.load(f))
                
        self.items_scraped = 0
        self.scored_cars = []

    def _save_notified(self):
        with open(self.NOTIFIED_FILE, 'w') as f:
            json.dump(list(self.notified_ids), f)

    def _send_discord(self, msg):
        payload = {"content": msg}
        try:
            requests.post(self.DISCORD_WEBHOOK_URL, json=payload, timeout=5)
        except Exception as e:
            self.logger.error(f"Discord error: {e}")

    def _evaluate_and_store(self, item):
        ad_id = str(item.get("id"))
        detail_raw = item.get("detail_raw", {})
        
        # Sauto vrací data pod klíčem "result"
        result = detail_raw.get("result")
        if not result:
            return

# 1. Extrakce přesných dat s ochranou proti hodnotám "null" (None)
        popis = (result.get("description") or "").lower()
        nazev = (result.get("name") or "").lower()
        vykon_kw = result.get("engine_power") or 0
        cena = result.get("price") or 0
        
        tachometr = result.get("tachometer")
        tachometr = tachometr if tachometr is not None else 999999
        
        prevodovka_dict = result.get("gearbox_cb") or {}
        prevodovka = (prevodovka_dict.get("name") or "").lower()
        
        prvni_majitel = result.get("first_owner", False)
        nebourano = result.get("crashed_in_past") is False
        
        rok_vyroby_str = result.get("manufacturing_date") or "2000"
        rok_vyroby = int(rok_vyroby_str.split("-")[0]) if rok_vyroby_str else 2000
        
        vybava_seznam = [eq.get("name", "").lower() for eq in (result.get("equipment_cb") or []) if eq.get("name")]
        vybava_str = " ".join(vybava_seznam)

        # 2. Tvrdé filtry (Vyhazovač)
        if "automat" in prevodovka:
            return
            
        for bad_word in self.BLACKLIST:
            if bad_word in popis or bad_word in nazev:
                return

        # 3. Skórování
        skore = 0
        duvody = []

        # A) Klíčová slova v popisu
        for vzor, body in self.PLUSOVE_BODY.items():
            if re.search(vzor, popis):
                skore += body
                duvody.append(f"+{body} ({vzor.split('|')[0]})")
                
        for vzor, body in self.MINUSOVE_BODY.items():
            if re.search(vzor, popis) or re.search(vzor, vybava_str):
                skore += body
                duvody.append(f"{body} (Tuning/úpravy)")

        # B) Bodování metadat z databáze
        if prvni_majitel:
            skore += 100
            duvody.append("+100 (1. majitel dle dat)")
            
        if nebourano:
            skore += 20
            duvody.append("+20 (Nebouráno)")
            
        if tachometr < 50000:
            skore += 150
            duvody.append("+150 (Nájezd pod 50k)")
        elif tachometr < 100000:
            skore += 80
            duvody.append("+80 (Nájezd pod 100k)")
            
        if (2026 - rok_vyroby) >= 15 and tachometr < 120000:
            skore += 150
            duvody.append("+150 (Youngtimer uloženka)")

        # C) Výkonové anomálie (Hot Hatche & Ztracené specifikace)
        if vykon_kw >= 140 and cena > 0 and cena <= 150000:
            skore += 200
            duvody.append(f"+200 (Anomálie: {vykon_kw}kW za {cena}Kč)")
            
        if any(x in nazev for x in ['gti', 'rs', 'st', 'type r', 'mps', 'cupra']) and cena <= 200000:
            skore += 100
            duvody.append("+100 (Dostupný Hot Hatch)")

        # Uložíme výsledek pro závěrečný report
        url = item.get("url", "Odkaz chybí")
        nazev_k_zobrazeni = result.get("name", item.get("manufacturer_name", ""))
        
        self.scored_cars.append({
            'ad_id': ad_id,
            'nazev': nazev_k_zobrazeni,
            'cena': cena,
            'vykon': vykon_kw,
            'skore': skore,
            'duvody': duvody,
            'url': url
        })

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
        self.strict_manufacturer_seo = self._norm_str(params.get("manufacturer_seo_name"))
        self.strict_model_seo = self._norm_str(params.get("model_seo_name"))
        self.strict_seller_type = self._norm_str(params.get("seller_type")) 

    def _passes_strict_filter(self, item: dict) -> bool:
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
        self._load_strict_filters(params)
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

    def parse_search(self, response):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON response (search)")
            return

        results = data.get("results", []) or []

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

        params = (response.meta.get("params") or {}).copy()
        limit = int(params.get("limit", 35))
        offset = int(params.get("offset", 0))
        total = self._extract_total(data)

        if total == -1:
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
        
        self._evaluate_and_store(base_item)
        self.items_scraped += 1
        
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

    def closed(self, reason):
        znacka = self.strict_manufacturer_seo or "Vše"
        model = self.strict_model_seo or "Vše"
        typ_prodejce = self.strict_seller_type or "Všichni"

        zprava = (
            f"🏁 **SCRAPE DOKONČEN** (`{reason}`)\n"
            f"⚙️ **Filtry:** Značka: {znacka} | Model: {model} | Prodejce: {typ_prodejce}\n"
            f"📊 **Zkontrolováno inzerátů:** {self.items_scraped}\n"
            "───────────────────\n"
        )

        top_kousky = sorted(self.scored_cars, key=lambda x: x['skore'], reverse=True)
        top_kousky = [auto for auto in top_kousky if auto['skore'] > 0][:5]

        if not top_kousky:
            zprava += "Nenašlo se vůbec nic zajímavého. Žádné auto nezískalo plusové body."
        else:
            zprava += "🏆 **TOP NALEZENÉ KOUSKY:**\n\n"
            for i, auto in enumerate(top_kousky, 1):
                if auto['ad_id'] not in self.notified_ids:
                    self.notified_ids.add(auto['ad_id'])
                    novinka = "🆕 "
                else:
                    novinka = ""

                # Vypíšeme si všechny důvody, díky kterým to nasbíralo body
                duvody_text = ", ".join(auto['duvody']) if auto['duvody'] else "Bez důvodu"
                
                zprava += (
                    f"{i}. {novinka}**{auto['nazev']}** — {auto['cena']} Kč\n"
                    f"   🐎 {auto['vykon']} kW | 📊 Skóre: **{auto['skore']}**\n"
                    f"   📌 {duvody_text}\n"
                    f"   🔗 {auto['url']}\n\n"
                )
            
            self._save_notified()

        self._send_discord(zprava)