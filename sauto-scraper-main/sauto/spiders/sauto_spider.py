import datetime
import json
import logging
import os
import re
from collections import Counter, defaultdict
from urllib.parse import urlencode

import requests
import scrapy


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


class CarEvaluator:
    HARD_REJECT_PATTERNS = (
        (r"na\s*(nahradni\s*)?d[ií]ly", "for parts"),
        (r"bez\s*stk|propadl[ae]\s*stk", "invalid STK"),
        (r"v[aá]da\s*motoru|motor\s*(klepe|zadreny)|z[eě]re\s*olej", "engine issue"),
        (r"exekuc", "legal issue"),
        (r"tot[aá]ln[ií]\s*skoda|po\s*tot[aá]ln[ií]", "total loss"),
    )

    BONUS_PATTERNS = (
        (r"servisn[ií]\s*kni[zž]ka|kompletni\s*servis|faktur", 35, "service history"),
        (r"gar[aá][zž]ovan", 20, "garaged"),
        (r"po\s*rozvodech|rozvody\s*(d[eě]lan[ey]|vym[eě]n[ae]ny)", 25, "timing service"),
        (r"nov[aé]\s*brzdy|nov[aá]\s*baterie|nov[eé]\s*pneu", 12, "recent maintenance"),
        (r"nehavarovan[eé]|nebouran[eé]|bez\s*koroze", 20, "clean history"),
        (r"prvn[ií]\s*majitel|1\.\s*majitel", 25, "first owner mention"),
        (r"dolo[zž]eno|servis\s*dolo[zž]en", 15, "proof available"),
    )

    PENALTY_PATTERNS = (
        (r"koroze|rez", -40, "rust mention"),
        (r"investic|nutn[ýá]\s*servis|vym[eě]nit", -35, "needs investment"),
        (r"n[eě]funk[nč][ií]|nefunguje", -30, "non-functional parts"),
        (r"tuning|chip|na[cč]ipov[aá]no|upraven[eo]", -30, "tuning mention"),
        (r"klepe|hu[cč][ií]|[řr]acht[aá]", -45, "suspicious sounds"),
    )

    EQUIPMENT_BONUS = (
        (r"tempomat|adaptivn[ií]\s*tempomat", 8, "cruise control"),
        (r"parkovac[ií]\s*senzory|parkovac[ií]\s*kamera", 8, "parking assist"),
        (r"apple\s*car\s*play|android\s*auto|navigace", 8, "connectivity/nav"),
        (r"vyh[rř]ivan[aá]\s*sedadla", 6, "heated seats"),
        (r"led|xenon", 6, "lighting package"),
    )

    SCORING_PRESETS = {
        "value": {
            "name": "Cena / výkon",
            "description": "Hledá nejvíc muziky za peníze: nízká cena za kW, levný provoz, rozumný nájezd.",
            "weights": {
                "age": 0.75,
                "mileage": 1.10,
                "price": 1.40,
                "consumption": 1.15,
                "cost": 1.45,
                "price_power": 1.85,
                "power": 0.85,
                "equipment": 0.45,
                "flags": 1.15,
                "sport": 0.25,
                "luxury": 0.15,
            },
        },
        "balanced": {
            "name": "Balanced",
            "description": "Nejuniverzálnější hodnocení: stav, nájezd, provozní náklady, výkon i výbava mají podobnou váhu.",
            "weights": {
                "age": 1.00,
                "mileage": 1.00,
                "price": 1.00,
                "consumption": 1.00,
                "cost": 1.00,
                "price_power": 1.00,
                "power": 0.75,
                "equipment": 0.85,
                "flags": 1.00,
                "sport": 0.35,
                "luxury": 0.35,
            },
        },
        "sport": {
            "name": "Sport",
            "description": "Priorita: výkon, dynamika, cena za kW, pohon a mladší kusy. Spotřeba a luxus jsou méně důležité.",
            "weights": {
                "age": 1.05,
                "mileage": 0.75,
                "price": 0.55,
                "consumption": 0.35,
                "cost": 0.55,
                "price_power": 1.30,
                "power": 2.10,
                "equipment": 0.45,
                "flags": 0.80,
                "sport": 1.45,
                "luxury": 0.20,
            },
        },
        "luxury": {
            "name": "Luxury",
            "description": "Priorita: prémiová značka, výbava, komfort, mladší vůz a kultivovaný výkon. Cena/provoz má menší váhu.",
            "weights": {
                "age": 1.35,
                "mileage": 0.90,
                "price": 0.25,
                "consumption": 0.25,
                "cost": 0.35,
                "price_power": 0.45,
                "power": 0.80,
                "equipment": 2.10,
                "flags": 0.90,
                "sport": 0.25,
                "luxury": 1.90,
            },
        },
    }

    PREMIUM_BRANDS = {
        "alfa-romeo",
        "audi",
        "bmw",
        "ds",
        "infiniti",
        "jaguar",
        "land-rover",
        "lexus",
        "maserati",
        "mercedes-benz",
        "mini",
        "porsche",
        "tesla",
        "volvo",
    }
    BUDGET_BRANDS = {
        "citroen",
        "dacia",
        "fiat",
        "hyundai",
        "kia",
        "opel",
        "peugeot",
        "renault",
        "seat",
        "skoda",
        "suzuki",
        "toyota",
    }

    @classmethod
    def _get_preset_config(cls, preset_name):
        """Get scoring preset configuration by name. Defaults to 'balanced' if not found."""
        return cls.SCORING_PRESETS.get(preset_name, cls.SCORING_PRESETS["balanced"])

    @staticmethod
    def _safe_int(value, default=0):
        try:
            if value is None:
                return default
            return int(float(value))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _safe_float(value, default=0.0):
        try:
            if value is None:
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _normalize_choice(value, allowed, default):
        if value is None:
            return default
        normalized = str(value).strip().lower()
        return normalized if normalized in allowed else default

    @classmethod
    def _brand_tier(cls, manufacturer_seo):
        brand = (manufacturer_seo or "").strip().lower()
        if brand in cls.PREMIUM_BRANDS:
            return "premium"
        if brand in cls.BUDGET_BRANDS:
            return "budget"
        return "mainstream"

    @staticmethod
    def _brand_market_weight(brand_tier):
        if brand_tier == "premium":
            return 1.15
        if brand_tier == "budget":
            return 0.92
        return 1.0

    @staticmethod
    def _parse_iso_datetime(value):
        if value is None:
            return None

        raw = str(value).strip()
        if not raw:
            return None

        normalized = raw.replace("Z", "+00:00")
        try:
            dt = datetime.datetime.fromisoformat(normalized)
        except ValueError:
            return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)

    @classmethod
    def _parse_date(cls, value):
        if value is None:
            return None

        text = str(value).strip()
        if not text:
            return None

        dt = cls._parse_iso_datetime(text)
        if dt is not None:
            return dt.date()

        for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
            try:
                return datetime.datetime.strptime(text, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _months_until(target_date):
        if target_date is None:
            return None
        today = datetime.date.today()
        months = (target_date.year - today.year) * 12 + (target_date.month - today.month)
        if target_date.day < today.day:
            months -= 1
        return months

    @staticmethod
    def _infer_drive_type(drive_name):
        value = (drive_name or "").lower()
        if any(x in value for x in ("4x4", "4wd", "awd", "quattro", "xdrive", "allrad")):
            return "awd"
        if any(x in value for x in ("zad", "rear", "rwd")):
            return "rwd"
        if any(x in value for x in ("pred", "před", "front", "fwd")):
            return "fwd"
        return "unknown"

    @staticmethod
    def _infer_gearbox_type(gearbox_name):
        value = (gearbox_name or "").lower()
        if any(x in value for x in ("automat", "dsg", "tiptronic")):
            return "automatic"
        if "manu" in value:
            return "manual"
        return "unknown"

    @staticmethod
    def _estimate_fuel_price_per_unit(fuel_seo):
        prices = {
            "benzin": 39.5,
            "nafta": 38.0,
            "lpg-benzin": 18.5,
            "cng-benzin": 35.0,
            "hybrid": 38.5,
            "elektro": 6.0,
        }
        return prices.get(fuel_seo, 38.5)

    @classmethod
    def _estimate_consumption_per_100km(cls, reported, fuel_seo, power_kw, gearbox_type, drive_type):
        reported_value = cls._safe_float(reported, 0.0)
        if 1.5 <= reported_value <= 30:
            return round(reported_value, 2)

        base_map = {
            "benzin": 6.3,
            "nafta": 5.3,
            "lpg-benzin": 8.7,
            "cng-benzin": 5.0,
            "hybrid": 5.0,
            "elektro": 17.0,
        }
        value = base_map.get(fuel_seo, 6.0)

        if fuel_seo != "elektro":
            value += max(0, power_kw - 70) / 65.0

        if gearbox_type == "automatic":
            value += 0.35
        if drive_type == "awd":
            value += 0.55
        elif drive_type == "rwd":
            value += 0.2

        return round(min(max(value, 2.3), 30.0), 2)

    @staticmethod
    def _estimate_annual_insurance(
        price,
        power_kw,
        fuel_seo,
        drive_type,
        gearbox_type,
        age_years,
        brand_tier,
        engine_volume,
    ):
        estimate = 2300.0

        # Engine power in buckets reflects insurer risk tables better than a pure linear term.
        if power_kw <= 55:
            estimate += 250.0
        elif power_kw <= 75:
            estimate += 700.0
        elif power_kw <= 95:
            estimate += 1250.0
        elif power_kw <= 120:
            estimate += 1950.0
        elif power_kw <= 150:
            estimate += 3000.0
        else:
            estimate += 4200.0

        estimate += min(2600.0, price * 0.0050)

        if fuel_seo == "nafta":
            estimate += 300.0
        elif fuel_seo == "lpg-benzin":
            estimate += 180.0
        elif fuel_seo == "elektro":
            estimate += 950.0

        if drive_type == "awd":
            estimate += 520.0
        elif drive_type == "rwd":
            estimate += 240.0

        if gearbox_type == "automatic":
            estimate += 240.0

        if engine_volume >= 3000:
            estimate += 420.0
        elif engine_volume >= 2200:
            estimate += 220.0

        if age_years <= 2:
            estimate += 650.0
        elif age_years <= 5:
            estimate += 420.0
        elif age_years >= 18:
            estimate -= 420.0
        elif age_years >= 12:
            estimate -= 220.0

        if brand_tier == "premium":
            estimate += 1100.0
        elif brand_tier == "budget":
            estimate -= 320.0

        if power_kw >= 140 and brand_tier == "premium":
            estimate += 380.0

        return int(max(2500, min(22000, round(estimate))))

    @staticmethod
    def _estimate_annual_maintenance(
        price,
        age_years,
        tachometer,
        drive_type,
        gearbox_type,
        service_book,
        first_owner,
        tuning,
        brand_tier,
        fuel_seo,
        engine_volume,
        power_kw,
    ):
        estimate = 4000.0

        estimate += age_years * 290.0
        estimate += max(0, tachometer - 80000) / 1000.0 * 42.0
        estimate += price * 0.0105

        if drive_type == "awd":
            estimate += 1300.0
        if gearbox_type == "automatic":
            estimate += 1050.0
        if service_book:
            estimate -= 950.0
        if first_owner:
            estimate -= 300.0
        if tuning:
            estimate += 1200.0

        if fuel_seo == "nafta":
            estimate += 500.0
        elif fuel_seo == "lpg-benzin":
            estimate += 420.0
        elif fuel_seo == "elektro":
            estimate += 650.0

        if engine_volume >= 3000:
            estimate += 650.0
        elif engine_volume >= 2200:
            estimate += 300.0

        if power_kw >= 170:
            estimate += 450.0
        elif power_kw <= 60:
            estimate -= 120.0

        if brand_tier == "premium":
            estimate += 1900.0
        elif brand_tier == "budget":
            estimate -= 550.0

        if age_years >= 14 and gearbox_type == "automatic":
            estimate += 700.0
        if age_years >= 13 and drive_type == "awd":
            estimate += 600.0
        if tachometer >= 220000 and brand_tier == "premium":
            estimate += 900.0
        if tachometer >= 250000 and gearbox_type == "automatic":
            estimate += 700.0

        return int(max(3000, min(38000, round(estimate))))

    @classmethod
    def _equipment_depth_score(cls, equipment_names):
        if not equipment_names:
            return 0, []

        equipment_text = " ".join(equipment_names)
        rules = (
            (r"adaptivn[ií]\s*tempomat", 8, "eq:adaptive cruise"),
            (r"nouzov[eé]\s*brzd|front assist|syst[eé]m nouzov[eé]ho zastaven[ií]", 8, "eq:active safety"),
            (r"mrtv[eé]ho\s*uhlu|jizdniho\s*pruhu|rcta", 6, "eq:lane/blind assist"),
            (r"parkovac[ií]\s*kamera", 5, "eq:camera"),
            (r"parkovac[ií]\s*senzory", 4, "eq:sensors"),
            (r"apple\s*car\s*play|android\s*auto|navigace", 4, "eq:multimedia"),
            (r"vyh[rř]ivan[aá]\s*sedadla|vyh[rř]ivan[eé]\s*celn[ií]\s*sklo", 3, "eq:winter package"),
            (r"kozen[aá]\s*sedadla|kozen[eé]\s*calouneni", 3, "eq:leather"),
            (r"xenon|led\s*svetl", 3, "eq:lights"),
            (r"panoramaticka\s*strecha|stresni\s*okno", 2, "eq:panorama"),
        )

        points = 0
        reasons = []
        for pattern, value, reason in rules:
            if re.search(pattern, equipment_text):
                points += value
                reasons.append(f"+{value} ({reason})")

        safety_tokens = ("airbag", "abs", "esp", "asistent", "front assist", "mrtveho", "pruhu", "nouz")
        safety_count = sum(1 for name in equipment_names if any(token in name for token in safety_tokens))
        if safety_count >= 10:
            points += 10
            reasons.append("+10 (equipment safety depth)")
        elif safety_count >= 6:
            points += 6
            reasons.append("+6 (equipment safety depth)")

        if len(equipment_names) >= 40:
            points += 8
            reasons.append("+8 (very rich equipment)")
        elif len(equipment_names) >= 25:
            points += 4
            reasons.append("+4 (good equipment breadth)")

        if points > 55:
            points = 55

        return points, reasons

    @staticmethod
    def _age_bucket(age_years):
        if age_years <= 5:
            return "0-5"
        if age_years <= 10:
            return "6-10"
        if age_years <= 15:
            return "11-15"
        return "16+"

    @classmethod
    def _apply_pattern_score(cls, text, rules):
        score = 0
        reasons = []
        for pattern, points, reason in rules:
            if re.search(pattern, text):
                score += points
                reasons.append(f"{points:+d} ({reason})")
        return score, reasons

    @classmethod
    def evaluate(
        cls,
        item,
        current_year=None,
        allow_automatic=False,
        min_price=20000,
        target_annual_km=15000,
    ):
        """
        Extract and normalize raw car metrics. NO SCORING.
        Varianta A: Frontend handles all scoring and preset application.
        """
        current_year = current_year or datetime.datetime.now().year

        detail_raw = item.get("detail_raw", {})
        result = detail_raw.get("result")
        if not result:
            return None

        ad_id = str(item.get("id") or result.get("id") or "")
        if not ad_id:
            return None

        manufacturer_cb = result.get("manufacturer_cb") or item.get("manufacturer_cb") or {}
        model_cb = result.get("model_cb") or item.get("model_cb") or {}
        fuel_cb = result.get("fuel_cb") or item.get("fuel_cb") or {}
        body_cb = result.get("vehicle_body_cb") or item.get("vehicle_body_cb") or {}

        manufacturer_seo = manufacturer_cb.get("seo_name") or "unknown-brand"
        model_seo = model_cb.get("seo_name") or "unknown-model"
        fuel_seo = fuel_cb.get("seo_name") or "unknown-fuel"
        body_seo = body_cb.get("seo_name") or "unknown-body"
        brand_tier = cls._brand_tier(manufacturer_seo)
        brand_market_weight = cls._brand_market_weight(brand_tier)

        name = result.get("name") or item.get("name") or "Unknown"
        description = (result.get("description") or "").lower()
        title = name.lower()
        full_text = f"{title}\n{description}"

        gearbox_name = ((result.get("gearbox_cb") or {}).get("name") or "").lower()
        gearbox_type = cls._infer_gearbox_type(gearbox_name)
        if not allow_automatic and gearbox_type == "automatic":
            return None

        drive_name = ((result.get("drive_cb") or {}).get("name") or "").lower()
        drive_type = cls._infer_drive_type(drive_name)

        for pattern, _reason in cls.HARD_REJECT_PATTERNS:
            if re.search(pattern, full_text):
                return None

        price = cls._safe_int(result.get("price") or item.get("price"), 0)
        if price < min_price:
            return None

        power_kw = cls._safe_int(result.get("engine_power"), 0)
        engine_volume = cls._safe_int(result.get("engine_volume"), 0)
        tachometer = cls._safe_int(result.get("tachometer"), 0)

        manufacturing_date = str(result.get("manufacturing_date") or item.get("manufacturing_date") or "")
        year_match = re.search(r"(19|20)\d{2}", manufacturing_date)
        year = int(year_match.group(0)) if year_match else current_year - 15
        age_years = max(1, current_year - year)

        create_date = result.get("create_date") or item.get("create_date")
        create_dt = cls._parse_iso_datetime(create_date)
        listing_age_days = None
        if create_dt is not None:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            listing_age_days = max(0, (now_utc - create_dt).days)

        stk_date = cls._parse_date(result.get("stk_date"))
        months_to_stk = cls._months_until(stk_date)

        price_per_kw = round(price / power_kw, 2) if price > 0 and power_kw > 0 else None
        price_per_km = round(price / tachometer, 4) if price > 0 and tachometer > 0 else None
        km_per_year = round(tachometer / age_years) if tachometer > 0 else None

        annual_km_for_cost = km_per_year if km_per_year and km_per_year > 0 else max(6000, int(target_annual_km))
        average_gas_mileage = result.get("average_gas_mileage")
        estimated_consumption = cls._estimate_consumption_per_100km(
            average_gas_mileage,
            fuel_seo,
            power_kw,
            gearbox_type,
            drive_type,
        )
        fuel_price_per_unit = cls._estimate_fuel_price_per_unit(fuel_seo)
        annual_fuel_cost = int(round(annual_km_for_cost * estimated_consumption / 100.0 * fuel_price_per_unit))

        equipment_list = [
            eq.get("name", "").lower()
            for eq in (result.get("equipment_cb") or [])
            if eq.get("name")
        ]
        images_count = len(result.get("images") or [])

        first_owner = bool(result.get("first_owner"))
        service_book = bool(result.get("service_book"))
        tuning = bool(result.get("tuning"))

        euro_value = cls._safe_int((result.get("euro_level_cb") or {}).get("value"), 0)
        vin = str(result.get("vin") or "").strip()
        airbags = cls._safe_int(result.get("airbags"), 0)

        annual_insurance = cls._estimate_annual_insurance(
            price=price,
            power_kw=power_kw,
            fuel_seo=fuel_seo,
            drive_type=drive_type,
            gearbox_type=gearbox_type,
            age_years=age_years,
            brand_tier=brand_tier,
            engine_volume=engine_volume,
        )
        annual_maintenance = cls._estimate_annual_maintenance(
            price=price,
            age_years=age_years,
            tachometer=tachometer,
            drive_type=drive_type,
            gearbox_type=gearbox_type,
            service_book=service_book,
            first_owner=first_owner,
            tuning=tuning,
            brand_tier=brand_tier,
            fuel_seo=fuel_seo,
            engine_volume=engine_volume,
            power_kw=power_kw,
        )
        annual_total_cost = annual_fuel_cost + annual_insurance + annual_maintenance

        age_bucket = cls._age_bucket(age_years)
        cohort_key = f"{manufacturer_seo}:{model_seo}:{fuel_seo}:{age_bucket}:{gearbox_type}:{drive_type}"
        model_family_key = f"{manufacturer_seo}:{model_seo}"
        model_key = f"{manufacturer_seo}:{model_seo}:{body_seo}"

        return {
            "ad_id": ad_id,
            "name": name,
            "price": price,
            "power_kw": power_kw,
            "tachometer": tachometer,
            "age_years": age_years,
            "url": item.get("url", "URL missing"),
            "price_per_kw": price_per_kw,
            "price_per_km": price_per_km,
            "km_per_year": km_per_year,
            "seller_type": item.get("seller_type"),
            "manufacturer_seo": manufacturer_seo,
            "brand_tier": brand_tier,
            "brand_market_weight": brand_market_weight,
            "model_seo": model_seo,
            "fuel_seo": fuel_seo,
            "body_seo": body_seo,
            "gearbox_type": gearbox_type,
            "drive_type": drive_type,
            "cohort_key": cohort_key,
            "model_family_key": model_family_key,
            "model_key": model_key,
            "listing_age_days": listing_age_days,
            "months_to_stk": months_to_stk,
            "euro_value": euro_value,
            "airbags": airbags,
            "annual_km_for_cost": annual_km_for_cost,
            "estimated_consumption_per_100km": estimated_consumption,
            "fuel_price_per_unit": fuel_price_per_unit,
            "annual_fuel_cost": annual_fuel_cost,
            "annual_insurance": annual_insurance,
            "annual_maintenance": annual_maintenance,
            "annual_total_cost": annual_total_cost,
            "first_owner": first_owner,
            "service_book": service_book,
            "tuning": tuning,
            "equipment_list": equipment_list,
            "images_count": images_count,
        }


class SautoSpider(scrapy.Spider):
    name = "sauto"
    BASE_URL = "https://www.sauto.cz/api/v1/items/search?"
    DETAIL_API_URL = "https://www.sauto.cz/api/v1/items/{}"

    NOTIFIED_FILE = "notified_ids.json"
    INTERESTING_OFFERS_FILE = "data/sauto_interesting.json"
    CATALOG_CACHE_FILE = "data/sauto_catalog_cache.json"

    def __init__(self, *args, **kwargs):
        super(SautoSpider, self).__init__(*args, **kwargs)

        self.notified_ids = set()
        if os.path.exists(self.NOTIFIED_FILE):
            try:
                with open(self.NOTIFIED_FILE, "r", encoding="utf-8") as f:
                    self.notified_ids = set(str(x) for x in json.load(f))
            except (json.JSONDecodeError, OSError) as exc:
                self.logger.warning(f"Unable to read {self.NOTIFIED_FILE}: {exc}")

        self.items_scraped = 0
        self.scored_cars = []
        self.all_items = []
        self.seen_ad_ids = set()

        self.strict_manufacturer_seo = None
        self.strict_model_seo = None
        self.strict_manufacturer_set = set()
        self.strict_model_set = set()
        self.strict_seller_type = None

        self.filter_year_from = None
        self.filter_year_to = None
        self.filter_tachometer_from = None
        self.filter_tachometer_to = None
        self.filter_power_from = None
        self.filter_power_to = None
        self.filter_fuel_set = set()
        self.filter_body_set = set()
        self.filter_gearbox = None
        self.filter_drive = None
        self.required_equipment_terms = []

        self.discord_webhook_url = os.getenv("SAUTO_DISCORD_WEBHOOK_URL", "").strip()
        self.min_interesting_score = 90
        self.top_n = 10
        self.min_price = 0
        self.allow_automatic = False
        self.discord_notify_only_new = True

        self.market_min_cohort_size = 6
        self.market_expected_km_per_year = 16000

        self.target_annual_km = 15000
        self.prefer_gearbox = "any"
        self.prefer_drive = "any"

        self.model_price_min_samples = 5
        self.undervalue_ratio_threshold = 0.88
        self.deep_undervalue_ratio_threshold = 0.75
        self.overprice_ratio_threshold = 1.18

    def _save_notified(self):
        with open(self.NOTIFIED_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(self.notified_ids), f, ensure_ascii=False, indent=2)

    def _save_sorted_offers(self, sorted_offers):
        output_dir = os.path.dirname(self.INTERESTING_OFFERS_FILE)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        with open(self.INTERESTING_OFFERS_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted_offers, f, ensure_ascii=False, indent=2)

    def _send_discord(self, msg):
        if not self.discord_webhook_url:
            self.logger.info("Discord webhook URL not set, skipping Discord notification.")
            return

        chunks = [msg[i:i + 1900] for i in range(0, len(msg), 1900)]

        for chunk in chunks:
            payload = {"content": chunk}
            try:
                response = requests.post(self.discord_webhook_url, json=payload, timeout=5)
                if response.status_code >= 400:
                    self.logger.error(
                        f"Discord rejected message (Error {response.status_code}): {response.text}"
                    )
            except requests.RequestException as exc:
                self.logger.error(f"Failed to send Discord webhook: {exc}")

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

    @staticmethod
    def _to_int(value, default):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _to_float(value, default):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _to_bool(value, default=False):
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        normalized = str(value).strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
        return default

    @staticmethod
    def _to_choice(value, allowed, default):
        if value is None:
            return default
        normalized = str(value).strip().lower()
        return normalized if normalized in allowed else default

    def _to_optional_int(self, value):
        normalized = self._norm_str(value)
        if normalized is None:
            return None
        try:
            return int(float(normalized))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _median(values):
        seq = sorted(v for v in values if v is not None)
        if not seq:
            return None

        middle = len(seq) // 2
        if len(seq) % 2 == 1:
            return float(seq[middle])
        return (seq[middle - 1] + seq[middle]) / 2.0

    @staticmethod
    def _clamp_int(value, low, high):
        return max(low, min(high, int(round(value))))

    def _load_runtime_options(self, params: dict):
        webhook_from_params = self._norm_str(params.pop("discord_webhook_url", None))
        if webhook_from_params:
            self.discord_webhook_url = webhook_from_params

        self.min_interesting_score = self._to_int(
            params.pop("interesting_min_score", self.min_interesting_score),
            self.min_interesting_score,
        )
        self.top_n = max(1, self._to_int(params.pop("interesting_top_n", self.top_n), self.top_n))
        self.min_price = max(0, self._to_int(params.pop("interesting_min_price", self.min_price), self.min_price))
        self.allow_automatic = self._to_bool(
            params.pop("allow_automatic", self.allow_automatic),
            self.allow_automatic,
        )
        self.discord_notify_only_new = self._to_bool(
            params.pop("discord_notify_only_new", self.discord_notify_only_new),
            self.discord_notify_only_new,
        )

        self.market_min_cohort_size = max(
            2,
            self._to_int(
                params.pop("market_min_cohort_size", self.market_min_cohort_size),
                self.market_min_cohort_size,
            ),
        )
        self.market_expected_km_per_year = max(
            8000,
            self._to_int(
                params.pop("market_expected_km_per_year", self.market_expected_km_per_year),
                self.market_expected_km_per_year,
            ),
        )

        self.target_annual_km = max(
            6000,
            self._to_int(
                params.pop("target_annual_km", self.target_annual_km),
                self.target_annual_km,
            ),
        )
        self.prefer_gearbox = self._to_choice(
            params.pop("prefer_gearbox", self.prefer_gearbox),
            {"any", "manual", "automatic"},
            self.prefer_gearbox,
        )
        self.prefer_drive = self._to_choice(
            params.pop("prefer_drive", self.prefer_drive),
            {"any", "fwd", "rwd", "awd"},
            self.prefer_drive,
        )

        self.filter_year_from = self._to_optional_int(params.pop("year_from", None))
        self.filter_year_to = self._to_optional_int(params.pop("year_to", None))
        self.filter_tachometer_from = self._to_optional_int(params.pop("tachometer_from", None))
        self.filter_tachometer_to = self._to_optional_int(params.pop("tachometer_to", None))
        self.filter_power_from = self._to_optional_int(params.pop("power_from", None))
        self.filter_power_to = self._to_optional_int(params.pop("power_to", None))
        self.filter_fuel_set = {x.lower() for x in self._split_csv(params.pop("fuel_seo", ""))}
        self.filter_body_set = {x.lower() for x in self._split_csv(params.pop("body_seo", ""))}
        self.filter_gearbox = self._to_choice(
            params.pop("gearbox_filter", None),
            {"manual", "automatic"},
            None,
        )
        self.filter_drive = self._to_choice(
            params.pop("drive_filter", None),
            {"fwd", "rwd", "awd"},
            None,
        )
        self.required_equipment_terms = [
            x.lower() for x in self._split_csv(params.pop("required_equipment", ""))
        ]

        self.model_price_min_samples = max(
            2,
            self._to_int(
                params.pop("model_price_min_samples", self.model_price_min_samples),
                self.model_price_min_samples,
            ),
        )
        self.undervalue_ratio_threshold = min(
            0.98,
            max(
                0.55,
                self._to_float(
                    params.pop("undervalue_ratio_threshold", self.undervalue_ratio_threshold),
                    self.undervalue_ratio_threshold,
                ),
            ),
        )
        self.deep_undervalue_ratio_threshold = min(
            self.undervalue_ratio_threshold - 0.01,
            max(
                0.45,
                self._to_float(
                    params.pop("deep_undervalue_ratio_threshold", self.deep_undervalue_ratio_threshold),
                    self.deep_undervalue_ratio_threshold,
                ),
            ),
        )
        if self.deep_undervalue_ratio_threshold >= self.undervalue_ratio_threshold:
            self.deep_undervalue_ratio_threshold = max(0.45, self.undervalue_ratio_threshold - 0.08)

        self.overprice_ratio_threshold = max(
            1.02,
            self._to_float(
                params.pop("overprice_ratio_threshold", self.overprice_ratio_threshold),
                self.overprice_ratio_threshold,
            ),
        )

    def _load_strict_filters(self, params: dict):
        self.strict_manufacturer_seo = self._norm_str(params.get("manufacturer_seo_name"))
        self.strict_model_seo = self._norm_str(params.get("model_seo_name"))
        self.strict_manufacturer_set = {
            x.strip() for x in (self.strict_manufacturer_seo or "").split(",") if x.strip()
        }
        self.strict_model_set = {
            x.strip() for x in (self.strict_model_seo or "").split(",") if x.strip()
        }
        self.strict_seller_type = self._norm_str(params.get("seller_type"))

    def _passes_strict_filter(self, item: dict) -> bool:
        m_cb = item.get("manufacturer_cb") or {}
        mo_cb = item.get("model_cb") or {}

        m_seo = m_cb.get("seo_name")
        mo_seo = mo_cb.get("seo_name")

        if self.strict_manufacturer_set and m_seo not in self.strict_manufacturer_set:
            return False
        if self.strict_model_set and mo_seo not in self.strict_model_set:
            return False

        if self.strict_seller_type:
            is_bazar = bool(item.get("premise"))
            if self.strict_seller_type == "bazar" and not is_bazar:
                return False
            if self.strict_seller_type == "soukromy" and is_bazar:
                return False

        return True

    def _passes_detail_filters(self, offer: dict) -> bool:
        year = datetime.datetime.now().year - int(offer.get("age_years") or 0)
        tachometer = offer.get("tachometer") or 0
        power_kw = offer.get("power_kw") or 0

        if self.filter_year_from is not None and year < self.filter_year_from:
            return False
        if self.filter_year_to is not None and year > self.filter_year_to:
            return False
        if self.filter_tachometer_from is not None and tachometer < self.filter_tachometer_from:
            return False
        if self.filter_tachometer_to is not None and tachometer > self.filter_tachometer_to:
            return False
        if self.filter_power_from is not None and power_kw < self.filter_power_from:
            return False
        if self.filter_power_to is not None and power_kw > self.filter_power_to:
            return False

        if self.filter_fuel_set and offer.get("fuel_seo") not in self.filter_fuel_set:
            return False
        if self.filter_body_set and offer.get("body_seo") not in self.filter_body_set:
            return False
        if self.filter_gearbox and offer.get("gearbox_type") != self.filter_gearbox:
            return False
        if self.filter_drive and offer.get("drive_type") != self.filter_drive:
            return False

        if self.required_equipment_terms:
            equipment_text = " ".join(offer.get("equipment_list") or []).lower()
            if any(term not in equipment_text for term in self.required_equipment_terms):
                return False

        return True

    @staticmethod
    def _split_csv(value):
        return [x.strip() for x in str(value or "").split(",") if x.strip()]

    def _load_cached_model_map(self):
        try:
            with open(self.CATALOG_CACHE_FILE, "r", encoding="utf-8") as f:
                cache = json.load(f)
        except (OSError, json.JSONDecodeError):
            return {}

        model_map = {}
        models_by_brand = cache.get("models") if isinstance(cache, dict) else None
        if not isinstance(models_by_brand, dict):
            return model_map

        for manufacturer, payload in models_by_brand.items():
            if not isinstance(payload, dict):
                continue
            items = payload.get("items") or []
            model_map[manufacturer] = {
                str(item.get("value") or "").strip()
                for item in items
                if isinstance(item, dict) and str(item.get("value") or "").strip()
            }
        return model_map

    def _build_manufacturer_model_seo(self, params: dict):
        existing = self._norm_str(params.get("manufacturer_model_seo"))
        if existing:
            return existing

        manufacturers = self._split_csv(params.get("manufacturer_seo_name"))
        models = self._split_csv(params.get("model_seo_name"))
        if not manufacturers:
            return None

        if not models:
            return "|".join(manufacturers)

        model_map = self._load_cached_model_map()
        pairs = []
        for manufacturer in manufacturers:
            brand_models = model_map.get(manufacturer)
            for model in models:
                if brand_models is not None and model not in brand_models:
                    continue
                pairs.append(f"{manufacturer}:{model}")

        if pairs:
            return "|".join(pairs)

        # The Sauto API expects brand/model filters as `manufacturer_model_seo`
        # with `brand:model` pairs joined by `|`. The UI still stores our older
        # structure (`manufacturer_seo_name` + `model_seo_name`), so keep that
        # public structure and translate it only for the upstream API request.
        # If the local catalog cache is missing/stale, fall back to the old
        # broad behavior plus strict local filtering rather than returning zero.
        return "|".join(
            f"{manufacturer}:{model}"
            for manufacturer in manufacturers
            for model in models
        )

    def _build_search_params(self, params: dict):
        search_params = params.copy()
        manufacturer_model_seo = self._build_manufacturer_model_seo(search_params)

        # These keys are local/UI compatibility filters. Sauto ignores them on
        # /items/search, so sending them makes the request look filtered while it
        # is actually broad. The real Sauto filter is manufacturer_model_seo.
        search_params.pop("manufacturer_seo_name", None)
        search_params.pop("model_seo_name", None)
        if manufacturer_model_seo:
            search_params["manufacturer_model_seo"] = manufacturer_model_seo

        for key in list(search_params.keys()):
            if self._norm_str(search_params.get(key)) is None:
                search_params.pop(key, None)

        price_to = self._to_int(search_params.get("price_to"), 0)
        if price_to <= 0:
            search_params.pop("price_to", None)
        price_from = self._to_int(search_params.get("price_from"), 0)
        if price_from <= 0:
            search_params.pop("price_from", None)

        search_params["offset"] = str(search_params.get("offset", "0"))
        return search_params

    def _make_search_request(self, params: dict):
        url = f"{self.BASE_URL}{urlencode(params)}"
        _url_logger.info(f"Date: {datetime.datetime.now()}, scraping url: {url}")
        return scrapy.Request(
            url=url,
            method="GET",
            callback=self.parse_search,
            errback=self.handle_error,
            meta={"params": params},
            dont_filter=True,
        )

    def _extract_total(self, data: dict) -> int:
        for path in (("pagination", "total"), ("meta", "total"), ("data", "total"), ("total",)):
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

    def _build_market_context(self, offers):
        metric_names = (
            "price_per_kw",
            "price_per_km",
            "km_per_year",
            "annual_total_cost",
            "annual_insurance",
            "annual_fuel_cost",
        )
        global_metrics = {name: [] for name in metric_names}

        cohort_metrics = defaultdict(lambda: {name: [] for name in metric_names})
        cohort_counts = Counter()
        model_counts = Counter()
        model_price_values = defaultdict(list)

        for offer in offers:
            model_counts[offer.get("model_key") or "unknown:unknown"] += 1

            model_family_key = offer.get("model_family_key") or offer.get("model_key") or "unknown:unknown"
            price = offer.get("price") or 0
            if price > 0:
                model_price_values[model_family_key].append(price)

            for metric in metric_names:
                value = offer.get(metric)
                if value is not None:
                    global_metrics[metric].append(value)

            cohort_key = offer.get("cohort_key")
            if cohort_key:
                cohort_counts[cohort_key] += 1
                for metric in metric_names:
                    value = offer.get(metric)
                    if value is not None:
                        cohort_metrics[cohort_key][metric].append(value)

        cohort_refs = {}
        for cohort_key, metrics in cohort_metrics.items():
            cohort_refs[cohort_key] = {
                "count": cohort_counts[cohort_key],
                "price_per_kw": self._median(metrics["price_per_kw"]),
                "price_per_km": self._median(metrics["price_per_km"]),
                "km_per_year": self._median(metrics["km_per_year"]),
                "annual_total_cost": self._median(metrics["annual_total_cost"]),
                "annual_insurance": self._median(metrics["annual_insurance"]),
                "annual_fuel_cost": self._median(metrics["annual_fuel_cost"]),
            }

        model_price_refs = {}
        for model_family_key, prices in model_price_values.items():
            if not prices:
                continue
            model_price_refs[model_family_key] = {
                "count": len(prices),
                "avg_price": float(sum(prices) / len(prices)),
                "median_price": self._median(prices),
            }

        return {
            "global": {
                "price_per_kw": self._median(global_metrics["price_per_kw"]),
                "price_per_km": self._median(global_metrics["price_per_km"]),
                "km_per_year": self._median(global_metrics["km_per_year"]),
                "annual_total_cost": self._median(global_metrics["annual_total_cost"]),
                "annual_insurance": self._median(global_metrics["annual_insurance"]),
                "annual_fuel_cost": self._median(global_metrics["annual_fuel_cost"]),
            },
            "cohorts": cohort_refs,
            "model_counts": model_counts,
            "model_price_refs": model_price_refs,
        }

    def _ratio_score(self, value, reference, weight, cap):
        if value is None or reference is None or reference <= 0:
            return 0
        ratio = value / reference
        raw = (1.0 - ratio) * weight
        return self._clamp_int(raw, -cap, cap)

    def _market_adjustment_for_offer(self, offer, context):
        return 0, [], {
            "value": 0,
            "ownership": 0,
            "model_price": 0,
            "usage": 0,
            "confidence": 0,
            "freshness": 0,
            "rarity": 0,
            "regulatory": 0,
            "model_avg_price": None,
            "model_price_ratio": None,
            "model_price_sample": 0,
            "valuation_label": "disabled",
            "used_cohort_reference": False,
            "cohort_size": 0,
        }

    def _apply_advanced_sorting(self, offers):
        if not offers:
            return []

        context = self._build_market_context(offers)

        for offer in offers:
            adjustment, market_reasons, components = self._market_adjustment_for_offer(offer, context)

            offer["market_adjustment"] = adjustment
            offer["market_components"] = components
            offer["model_avg_price"] = components.get("model_avg_price")
            offer["model_price_ratio"] = components.get("model_price_ratio")
            offer["model_price_sample"] = components.get("model_price_sample")
            offer["valuation_label"] = components.get("valuation_label")
            offer["is_undervalued"] = components.get("valuation_label") in {"undervalued", "deep_undervalued"}
            if market_reasons:
                offer["market_reasons"] = market_reasons

        return sorted(
            offers,
            key=lambda x: (
                x.get("price_per_kw") is None,
                x.get("price_per_kw") or 999999999,
                x.get("annual_total_cost") or 999999999,
                x.get("price") or 999999999,
            ),
        )

    def start_requests(self):
        params = self.read_params_from_json("params.json")
        self._load_runtime_options(params)
        self._load_strict_filters(params)
        yield self._make_search_request(self._build_search_params(params))

    def parse_search(self, response):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            return

        results = data.get("results", []) or []

        for r in results:
            if not self._passes_strict_filter(r):
                continue

            manufacturer = (r.get("manufacturer_cb") or {}).get("seo_name")
            model = (r.get("model_cb") or {}).get("seo_name")
            ad_id = r.get("id")

            if ad_id and ad_id in self.seen_ad_ids:
                continue
            if ad_id:
                self.seen_ad_ids.add(ad_id)

            r["manufacturer_name"] = (r.get("manufacturer_cb") or {}).get("name")
            r["model_name"] = (r.get("model_cb") or {}).get("name")
            r["seller_type"] = "bazar" if r.get("premise") else "soukromy"
            r["url"] = (
                f"https://www.sauto.cz/osobni/detail/{manufacturer}/{model}/{ad_id}"
                if manufacturer and model and ad_id
                else None
            )

            if ad_id:
                yield scrapy.Request(
                    url=self.DETAIL_API_URL.format(ad_id),
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
        limit = max(1, self._to_int(params.get("limit", 35), 35))
        offset = max(0, self._to_int(params.get("offset", 0), 0))
        total = self._extract_total(data)

        if total == -1:
            if len(results) == limit and limit > 0:
                params["offset"] = str(offset + limit)
                yield self._make_search_request(params)
            return

        next_offset = offset + limit
        if next_offset < total:
            params["offset"] = str(next_offset)
            yield self._make_search_request(params)

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

        raw_offer = CarEvaluator.evaluate(
            base_item,
            allow_automatic=self.allow_automatic,
            min_price=self.min_price,
            target_annual_km=self.target_annual_km,
        )
        if raw_offer and self._passes_detail_filters(raw_offer):
            self.scored_cars.append(raw_offer)
            base_item["offer_metrics"] = {
                "price_per_kw": raw_offer["price_per_kw"],
                "price_per_km": raw_offer["price_per_km"],
                "km_per_year": raw_offer["km_per_year"],
                "age_years": raw_offer["age_years"],
                "gearbox_type": raw_offer["gearbox_type"],
                "drive_type": raw_offer["drive_type"],
                "brand_tier": raw_offer["brand_tier"],
                "listing_age_days": raw_offer["listing_age_days"],
                "months_to_stk": raw_offer["months_to_stk"],
                "euro_value": raw_offer["euro_value"],
                "annual_fuel_cost": raw_offer["annual_fuel_cost"],
                "annual_insurance": raw_offer["annual_insurance"],
                "annual_maintenance": raw_offer["annual_maintenance"],
                "annual_total_cost": raw_offer["annual_total_cost"],
                "estimated_consumption_per_100km": raw_offer["estimated_consumption_per_100km"],
                "model_family_key": raw_offer["model_family_key"],
                "first_owner": raw_offer["first_owner"],
                "service_book": raw_offer["service_book"],
                "tuning": raw_offer["tuning"],
                "equipment_list": raw_offer["equipment_list"],
                "images_count": raw_offer["images_count"],
            }
        else:
            base_item["filtered_out"] = raw_offer is not None
            pass

        self.items_scraped += 1
        self.all_items.append(base_item)
        yield base_item

    def handle_detail_error(self, failure):
        base_item = failure.request.meta.get("base_item") or {}
        base_item["detail_fetch_ok"] = False
        base_item["detail_raw"] = None
        base_item["detail_error"] = str(failure.value)
        self.items_scraped += 1
        self.all_items.append(base_item)
        yield base_item

    def handle_error(self, failure):
        self.logger.error(f"Request failed: {failure.request.url}, Error: {failure.value}")

    def _format_discord_message(self, reason, offers, total_interesting):
        brand = self.strict_manufacturer_seo or "all"
        model = self.strict_model_seo or "all"
        seller_type = self.strict_seller_type or "all"

        lines = [
            f"SAUTO scrape finished ({reason})",
            f"Filters: brand={brand} | model={model} | seller={seller_type}",
            f"Checked ads: {self.items_scraped}",
            f"Raw matched ads: {len(self.scored_cars)}",
            f"Output: {self.INTERESTING_OFFERS_FILE}",
            (
                f"Market tuning: cohort>={self.market_min_cohort_size}, "
                f"expected km/year={self.market_expected_km_per_year}"
            ),
            (
                f"Ownership tuning: target annual km={self.target_annual_km}, "
                f"prefer gearbox={self.prefer_gearbox}, prefer drive={self.prefer_drive}"
            ),
            (
                f"Note: Scoring and preset selection moved to frontend (Varianta A)"
            ),
            (
                f"Model valuation: min samples={self.model_price_min_samples}, "
                f"undervalued<= {self.undervalue_ratio_threshold:.2f}x, "
                f"deep<= {self.deep_undervalue_ratio_threshold:.2f}x, "
                f"overpriced>= {self.overprice_ratio_threshold:.2f}x"
            ),
            "",
        ]

        if not offers:
            lines.append("No matching offers to notify on Discord in this run.")
            return "\n".join(lines)

        lines.append(f"Top {len(offers)} raw offers:")
        lines.append("")
        for index, offer in enumerate(offers, 1):
            new_prefix = "NEW " if offer.get("is_new") else ""
            lines.extend(
                [
                    (
                        f"{index}. {new_prefix}{offer['name']} | {offer['price']} CZK"
                    ),
                    (
                        f"   {offer.get('power_kw', 0)} kW | {offer.get('tachometer', 0)} km "
                        f"| age {offer.get('age_years', 0)}y | {offer.get('gearbox_type', 'unknown')}/"
                        f"{offer.get('drive_type', 'unknown')}"
                    ),
                    (
                        f"   price/kW: {offer.get('price_per_kw')} | price/km: {offer.get('price_per_km')} "
                        f"| km/year: {offer.get('km_per_year')}"
                    ),
                    (
                        f"   annual cost: {offer.get('annual_total_cost')} (fuel {offer.get('annual_fuel_cost')}, "
                        f"insurance {offer.get('annual_insurance')}, service {offer.get('annual_maintenance')})"
                    ),
                    (
                        f"   flags: first_owner={offer.get('first_owner')}, service_book={offer.get('service_book')}, "
                        f"tuning={offer.get('tuning')} | brand: {offer.get('brand_tier')}"
                    ),
                    f"   (scoring applied on frontend)",
                    f"   {offer.get('url') or 'URL missing'}",
                    "",
                ]
            )
        return "\n".join(lines)

    def closed(self, reason):
        all_offers = self._apply_advanced_sorting(list(self.scored_cars))
        top_offers = all_offers[: self.top_n]

        for offer in top_offers:
            is_new = offer["ad_id"] not in self.notified_ids
            offer["is_new"] = is_new
            if is_new:
                self.notified_ids.add(offer["ad_id"])

        if self.discord_notify_only_new:
            offers_for_discord = [offer for offer in top_offers if offer.get("is_new")]
        else:
            offers_for_discord = top_offers

        self._save_sorted_offers(all_offers)
        if top_offers:
            self._save_notified()

        message = self._format_discord_message(reason, offers_for_discord, len(all_offers))
        self._send_discord(message)