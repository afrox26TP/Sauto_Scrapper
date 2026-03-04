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
    def _estimate_annual_insurance(price, power_kw, fuel_seo, drive_type, gearbox_type, age_years, brand_tier):
        estimate = 2400.0
        estimate += power_kw * 24.0
        estimate += min(2200.0, price * 0.0045)

        if fuel_seo == "nafta":
            estimate += 250.0
        if drive_type == "awd":
            estimate += 450.0
        elif drive_type == "rwd":
            estimate += 200.0
        if gearbox_type == "automatic":
            estimate += 200.0

        if age_years <= 5:
            estimate += 450.0
        elif age_years >= 15:
            estimate -= 250.0

        if brand_tier == "premium":
            estimate += 1000.0
        elif brand_tier == "budget":
            estimate -= 300.0

        return int(max(2500, min(18000, round(estimate))))

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
    ):
        estimate = 4200.0
        estimate += age_years * 260.0
        estimate += max(0, tachometer - 90000) / 1000.0 * 38.0
        estimate += price * 0.010

        if drive_type == "awd":
            estimate += 1200.0
        if gearbox_type == "automatic":
            estimate += 900.0
        if service_book:
            estimate -= 800.0
        if first_owner:
            estimate -= 200.0
        if tuning:
            estimate += 900.0

        if brand_tier == "premium":
            estimate += 1800.0
        elif brand_tier == "budget":
            estimate -= 500.0

        return int(max(3000, min(32000, round(estimate))))

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
        min_score=90,
        min_price=20000,
        target_annual_km=15000,
        prefer_gearbox="any",
        prefer_drive="any",
    ):
        current_year = current_year or datetime.datetime.now().year
        prefer_gearbox = cls._normalize_choice(prefer_gearbox, {"any", "manual", "automatic"}, "any")
        prefer_drive = cls._normalize_choice(prefer_drive, {"any", "fwd", "rwd", "awd"}, "any")

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
        equipment_text = " ".join(equipment_list)
        images_count = len(result.get("images") or [])

        score = 0
        reasons = []

        text_bonus, text_bonus_reasons = cls._apply_pattern_score(full_text, cls.BONUS_PATTERNS)
        text_penalty, text_penalty_reasons = cls._apply_pattern_score(full_text, cls.PENALTY_PATTERNS)
        score += text_bonus + text_penalty
        reasons.extend(text_bonus_reasons)
        reasons.extend(text_penalty_reasons)

        first_owner = bool(result.get("first_owner"))
        crash_status = result.get("crashed_in_past")
        service_book = bool(result.get("service_book"))
        tuning = bool(result.get("tuning"))

        if first_owner:
            score += 20
            reasons.append("+20 (first owner)")
        if crash_status is False:
            score += 18
            reasons.append("+18 (no crash history)")
        elif crash_status is True:
            score -= 45
            reasons.append("-45 (crashed in past)")
        if service_book:
            score += 20
            reasons.append("+20 (service book)")
        if tuning:
            score -= 35
            reasons.append("-35 (tuning flag)")

        if brand_tier == "premium":
            score += 4
            reasons.append("+4 (premium brand desirability)")
            if age_years >= 12 or tachometer >= 220000:
                score -= 10
                reasons.append("-10 (older/high-km premium maintenance risk)")
        elif brand_tier == "budget":
            score += 3
            reasons.append("+3 (budget brand ownership simplicity)")
        else:
            score += 1
            reasons.append("+1 (mainstream brand liquidity)")

        if gearbox_type == "manual":
            score += 5
            reasons.append("+5 (manual gearbox)")
        elif gearbox_type == "automatic":
            score += 3
            reasons.append("+3 (automatic comfort)")
            if age_years >= 14 or tachometer >= 220000:
                score -= 6
                reasons.append("-6 (older/high-km automatic risk)")
        else:
            score -= 2
            reasons.append("-2 (unknown gearbox)")

        if prefer_gearbox != "any" and gearbox_type in {"manual", "automatic"}:
            if gearbox_type == prefer_gearbox:
                score += 8
                reasons.append(f"+8 (preferred gearbox: {prefer_gearbox})")
            else:
                score -= 8
                reasons.append(f"-8 (non-preferred gearbox: {gearbox_type})")

        if drive_type == "awd":
            score += 6
            reasons.append("+6 (AWD capability)")
            if age_years >= 15 or tachometer >= 250000:
                score -= 7
                reasons.append("-7 (AWD complexity on older/high-km car)")
        elif drive_type == "fwd":
            score += 3
            reasons.append("+3 (FWD lower running complexity)")
        elif drive_type == "rwd":
            score += 3
            reasons.append("+3 (RWD dynamics)")
            if power_kw >= 140:
                score += 2
                reasons.append("+2 (RWD + strong power)")

        if prefer_drive != "any" and drive_type in {"fwd", "rwd", "awd"}:
            if drive_type == prefer_drive:
                score += 6
                reasons.append(f"+6 (preferred drive: {prefer_drive})")
            else:
                score -= 6
                reasons.append(f"-6 (non-preferred drive: {drive_type})")

        if price_per_kw is not None:
            if price_per_kw <= 1300:
                score += 40
                reasons.append(f"+40 (price/kW {price_per_kw})")
            elif price_per_kw <= 1600:
                score += 25
                reasons.append(f"+25 (price/kW {price_per_kw})")
            elif price_per_kw <= 2200:
                score += 8
                reasons.append(f"+8 (price/kW {price_per_kw})")
            else:
                score -= 18
                reasons.append(f"-18 (high price/kW {price_per_kw})")

        if price_per_km is not None:
            if price_per_km <= 0.9:
                score += 30
                reasons.append(f"+30 (price/km {price_per_km})")
            elif price_per_km <= 1.3:
                score += 15
                reasons.append(f"+15 (price/km {price_per_km})")
            elif price_per_km >= 2.0:
                score -= 20
                reasons.append(f"-20 (high price/km {price_per_km})")

        if km_per_year is not None:
            if km_per_year <= 10000:
                score += 30
                reasons.append(f"+30 (low usage {km_per_year} km/year)")
            elif km_per_year <= 15000:
                score += 15
                reasons.append(f"+15 (normal usage {km_per_year} km/year)")
            elif km_per_year >= 25000:
                score -= 25
                reasons.append(f"-25 (high usage {km_per_year} km/year)")

        if age_years <= 8:
            score += 20
            reasons.append(f"+20 (younger car: {age_years}y)")
        elif age_years <= 12:
            score += 12
            reasons.append(f"+12 (mid age: {age_years}y)")
        elif age_years >= 20:
            score -= 10
            reasons.append(f"-10 (older car: {age_years}y)")

        if tachometer > 0:
            if tachometer <= 120000:
                score += 20
                reasons.append("+20 (low mileage)")
            elif tachometer <= 180000:
                score += 10
                reasons.append("+10 (reasonable mileage)")
            elif tachometer >= 280000:
                score -= 18
                reasons.append("-18 (high mileage)")

        if power_kw >= 110:
            score += 8
            reasons.append("+8 (good power)")
        elif power_kw > 0 and power_kw < 55:
            score -= 8
            reasons.append("-8 (low power)")

        if engine_volume >= 3000:
            score -= 4
            reasons.append("-4 (large engine running cost)")

        equipment_score, equipment_reasons = cls._apply_pattern_score(equipment_text, cls.EQUIPMENT_BONUS)
        score += equipment_score
        reasons.extend(equipment_reasons)

        depth_score, depth_reasons = cls._equipment_depth_score(equipment_list)
        score += depth_score
        reasons.extend(depth_reasons)

        if images_count >= 10:
            score += 6
            reasons.append("+6 (many photos)")

        euro_value = cls._safe_int((result.get("euro_level_cb") or {}).get("value"), 0)
        if euro_value >= 6:
            score += 6
            reasons.append("+6 (EURO 6+)")
        elif euro_value == 5:
            score += 3
            reasons.append("+3 (EURO 5)")
        elif 0 < euro_value <= 3:
            score -= 6
            reasons.append("-6 (low EURO class)")

        if months_to_stk is not None:
            if months_to_stk < 0:
                score -= 16
                reasons.append("-16 (STK expired)")
            elif months_to_stk <= 3:
                score -= 10
                reasons.append("-10 (STK expires soon)")
            elif months_to_stk <= 6:
                score -= 6
                reasons.append("-6 (short STK horizon)")
            elif months_to_stk >= 24:
                score += 8
                reasons.append("+8 (long STK horizon)")
            elif months_to_stk >= 12:
                score += 4
                reasons.append("+4 (solid STK horizon)")

        vin = str(result.get("vin") or "").strip()
        if len(vin) >= 17:
            score += 4
            reasons.append("+4 (VIN present)")
        else:
            score -= 2
            reasons.append("-2 (VIN missing/short)")

        cebia_verified = bool(item.get("is_cebia_smart_code_url_verified") or result.get("is_cebia_smart_code_url_verified"))
        if cebia_verified:
            score += 4
            reasons.append("+4 (Cebia verified)")

        user = result.get("user") or item.get("user") or {}
        if str(user.get("bankid_status") or "").lower() == "verified":
            score += 2
            reasons.append("+2 (verified seller)")

        airbags = cls._safe_int(result.get("airbags"), 0)
        if airbags >= 8:
            score += 4
            reasons.append("+4 (airbag count)")
        elif 0 < airbags <= 2:
            score -= 3
            reasons.append("-3 (low airbag count)")

        origin_name = ((result.get("country_of_origin_cb") or {}).get("name") or "").lower()
        if "nedohled" in origin_name:
            score -= 8
            reasons.append("-8 (unclear country of origin)")

        annual_insurance = cls._estimate_annual_insurance(
            price=price,
            power_kw=power_kw,
            fuel_seo=fuel_seo,
            drive_type=drive_type,
            gearbox_type=gearbox_type,
            age_years=age_years,
            brand_tier=brand_tier,
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
        )
        annual_total_cost = annual_fuel_cost + annual_insurance + annual_maintenance

        if annual_total_cost <= 50000:
            score += 10
            reasons.append(f"+10 (low annual ownership cost: {annual_total_cost})")
        elif annual_total_cost <= 65000:
            score += 5
            reasons.append(f"+5 (good annual ownership cost: {annual_total_cost})")
        elif annual_total_cost >= 95000:
            score -= 12
            reasons.append(f"-12 (high annual ownership cost: {annual_total_cost})")

        if annual_insurance <= 4000:
            score += 4
            reasons.append(f"+4 (low insurance est.: {annual_insurance})")
        elif annual_insurance >= 9000:
            score -= 8
            reasons.append(f"-8 (high insurance est.: {annual_insurance})")

        completeness_checks = [
            price > 0,
            power_kw > 0,
            tachometer > 0,
            year_match is not None,
            bool(description.strip()),
            images_count > 0,
            bool(item.get("url")),
            drive_type != "unknown",
            gearbox_type != "unknown",
            months_to_stk is not None,
            euro_value > 0,
            len(vin) >= 10,
        ]
        completeness_ratio = sum(1 for v in completeness_checks if v) / len(completeness_checks)
        confidence_score = int(round(completeness_ratio * 25))

        if len(description) >= 250:
            confidence_score += 6
        elif len(description) >= 120:
            confidence_score += 3

        if images_count >= 10:
            confidence_score += 5
        elif images_count >= 5:
            confidence_score += 3

        if service_book:
            confidence_score += 2

        confidence_score = min(45, confidence_score)
        confidence_impact = min(12, confidence_score // 3)
        if confidence_impact > 0:
            score += confidence_impact
            reasons.append(f"+{confidence_impact} (data confidence)")

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
            "base_score": score,
            "score": score,
            "interesting": score >= min_score,
            "reasons": reasons,
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
            "confidence_score": confidence_score,
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
        }


class SautoSpider(scrapy.Spider):
    name = "sauto"
    BASE_URL = "https://www.sauto.cz/api/v1/items/search?"
    DETAIL_API_URL = "https://www.sauto.cz/api/v1/items/{}"

    NOTIFIED_FILE = "notified_ids.json"
    INTERESTING_OFFERS_FILE = "data/sauto_interesting.json"

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

        self.strict_manufacturer_seo = None
        self.strict_model_seo = None
        self.strict_seller_type = None

        self.discord_webhook_url = os.getenv("SAUTO_DISCORD_WEBHOOK_URL", "").strip()
        self.min_interesting_score = 90
        self.top_n = 10
        self.min_price = 20000
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

        self.min_interesting_score = max(
            1,
            self._to_int(
                params.pop("interesting_min_score", self.min_interesting_score),
                self.min_interesting_score,
            ),
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
        cohort_key = offer.get("cohort_key")
        cohort_ref = context["cohorts"].get(cohort_key)

        use_cohort = bool(cohort_ref and cohort_ref.get("count", 0) >= self.market_min_cohort_size)
        ref = cohort_ref if use_cohort else context["global"]

        value_score = 0
        value_score += self._ratio_score(offer.get("price_per_kw"), ref.get("price_per_kw"), 70, 32)
        value_score += self._ratio_score(offer.get("price_per_km"), ref.get("price_per_km"), 60, 26)
        value_score += self._ratio_score(offer.get("km_per_year"), ref.get("km_per_year"), 48, 20)

        ownership_score = 0
        ownership_score += self._ratio_score(offer.get("annual_total_cost"), ref.get("annual_total_cost"), 78, 34)
        ownership_score += self._ratio_score(offer.get("annual_insurance"), ref.get("annual_insurance"), 36, 14)
        ownership_score += self._ratio_score(offer.get("annual_fuel_cost"), ref.get("annual_fuel_cost"), 26, 10)

        expected_km = max(1, (offer.get("age_years") or 1) * self.market_expected_km_per_year)
        tachometer = offer.get("tachometer") or 0
        usage_score = 0
        if tachometer > 0:
            usage_ratio = tachometer / expected_km
            usage_score = self._clamp_int((1.0 - usage_ratio) * 35, -18, 18)

        regulatory_score = 0
        months_to_stk = offer.get("months_to_stk")
        if months_to_stk is not None:
            if months_to_stk <= 3:
                regulatory_score -= 3
            elif months_to_stk >= 18:
                regulatory_score += 2

        euro_value = offer.get("euro_value") or 0
        if euro_value >= 6:
            regulatory_score += 2
        elif 0 < euro_value <= 3:
            regulatory_score -= 3

        if not use_cohort:
            value_score = int(round(value_score * 0.65))
            ownership_score = int(round(ownership_score * 0.70))
        elif cohort_ref and cohort_ref.get("count", 0) >= self.market_min_cohort_size * 2:
            value_score = int(round(value_score * 1.1))
            ownership_score = int(round(ownership_score * 1.08))

        confidence_score = offer.get("confidence_score") or 0
        confidence_adjustment = self._clamp_int((confidence_score - 16) * 0.45, -4, 10)

        listing_age_days = offer.get("listing_age_days")
        freshness_adjustment = 0
        if listing_age_days is not None:
            if listing_age_days <= 1:
                freshness_adjustment = 8
            elif listing_age_days <= 3:
                freshness_adjustment = 6
            elif listing_age_days <= 7:
                freshness_adjustment = 4
            elif listing_age_days <= 14:
                freshness_adjustment = 2
            elif listing_age_days >= 90:
                freshness_adjustment = -6
            elif listing_age_days >= 45:
                freshness_adjustment = -3

        model_key = offer.get("model_key") or "unknown:unknown"
        model_count = context["model_counts"].get(model_key, 0)
        rarity_adjustment = 0
        if model_count <= 2:
            rarity_adjustment = 4
        elif model_count <= 5:
            rarity_adjustment = 2
        elif model_count >= 70:
            rarity_adjustment = -1

        model_family_key = offer.get("model_family_key") or model_key
        model_price_ref = context["model_price_refs"].get(model_family_key)
        model_price_score = 0
        model_avg_price = None
        model_price_ratio = None
        model_price_sample = 0
        valuation_label = "unknown"

        if model_price_ref:
            model_price_sample = model_price_ref.get("count", 0)
            model_avg_price = model_price_ref.get("avg_price")

            offer_price = offer.get("price") or 0
            if model_price_sample >= self.model_price_min_samples and model_avg_price and offer_price > 0:
                model_price_ratio = offer_price / model_avg_price

                if model_price_ratio <= self.deep_undervalue_ratio_threshold:
                    model_price_score = 26
                    valuation_label = "deep_undervalued"
                elif model_price_ratio <= self.undervalue_ratio_threshold:
                    model_price_score = 15
                    valuation_label = "undervalued"
                elif model_price_ratio >= self.overprice_ratio_threshold + 0.12:
                    model_price_score = -14
                    valuation_label = "overpriced"
                elif model_price_ratio >= self.overprice_ratio_threshold:
                    model_price_score = -8
                    valuation_label = "slightly_overpriced"
                else:
                    model_price_score = 2
                    valuation_label = "fair"

                if model_price_ratio <= 0.62:
                    model_price_score -= 6

                brand_weight = offer.get("brand_market_weight") or 1.0
                model_price_score = self._clamp_int(model_price_score * brand_weight, -24, 24)

        total_adjustment = (
            value_score
            + ownership_score
            + model_price_score
            + usage_score
            + confidence_adjustment
            + freshness_adjustment
            + rarity_adjustment
            + regulatory_score
        )

        reasons = []
        if value_score != 0:
            reasons.append(f"{value_score:+d} (market value)")
        if ownership_score != 0:
            reasons.append(f"{ownership_score:+d} (ownership economics)")
        if model_price_score != 0:
            if model_price_ratio is None:
                reasons.append(f"{model_price_score:+d} (model pricing)")
            else:
                reasons.append(
                    f"{model_price_score:+d} (model avg ratio {model_price_ratio:.2f}x, {valuation_label})"
                )
        if usage_score != 0:
            reasons.append(f"{usage_score:+d} (expected mileage fit)")
        if confidence_adjustment != 0:
            reasons.append(f"{confidence_adjustment:+d} (data confidence weight)")
        if freshness_adjustment != 0:
            reasons.append(f"{freshness_adjustment:+d} (listing freshness)")
        if rarity_adjustment != 0:
            reasons.append(f"{rarity_adjustment:+d} (model rarity)")
        if regulatory_score != 0:
            reasons.append(f"{regulatory_score:+d} (STK/EURO context)")

        return total_adjustment, reasons, {
            "value": value_score,
            "ownership": ownership_score,
            "model_price": model_price_score,
            "usage": usage_score,
            "confidence": confidence_adjustment,
            "freshness": freshness_adjustment,
            "rarity": rarity_adjustment,
            "regulatory": regulatory_score,
            "model_avg_price": model_avg_price,
            "model_price_ratio": model_price_ratio,
            "model_price_sample": model_price_sample,
            "valuation_label": valuation_label,
            "used_cohort_reference": use_cohort,
            "cohort_size": (cohort_ref or {}).get("count", 0),
        }

    def _apply_advanced_sorting(self, offers):
        if not offers:
            return []

        context = self._build_market_context(offers)

        for offer in offers:
            base_score = offer.get("base_score", offer.get("score", 0))
            adjustment, market_reasons, components = self._market_adjustment_for_offer(offer, context)

            offer["base_score"] = base_score
            offer["market_adjustment"] = adjustment
            offer["market_components"] = components
            offer["score"] = base_score + adjustment
            offer["interesting"] = offer["score"] >= self.min_interesting_score
            offer["model_avg_price"] = components.get("model_avg_price")
            offer["model_price_ratio"] = components.get("model_price_ratio")
            offer["model_price_sample"] = components.get("model_price_sample")
            offer["valuation_label"] = components.get("valuation_label")
            offer["is_undervalued"] = components.get("valuation_label") in {"undervalued", "deep_undervalued"}

            merged_reasons = list(offer.get("reasons") or [])
            merged_reasons.extend(market_reasons)
            offer["reasons"] = merged_reasons

        return sorted(
            offers,
            key=lambda x: (
                x.get("score", 0),
                (x.get("market_components") or {}).get("model_price", 0),
                (x.get("market_components") or {}).get("ownership", 0),
                (x.get("market_components") or {}).get("value", 0),
                x.get("confidence_score", 0),
                -(x.get("price") or 0),
            ),
            reverse=True,
        )

    @log_url
    def start_requests(self):
        params = self.read_params_from_json("params.json")
        self._load_runtime_options(params)
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
        limit = int(params.get("limit", 35))
        offset = int(params.get("offset", 0))
        total = self._extract_total(data)

        if total == -1:
            if len(results) == limit and limit > 0:
                params["offset"] = str(offset + limit)
                yield scrapy.Request(
                    url=f"{self.BASE_URL}{urlencode(params)}",
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
            yield scrapy.Request(
                url=f"{self.BASE_URL}{urlencode(params)}",
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

        scored_offer = CarEvaluator.evaluate(
            base_item,
            allow_automatic=self.allow_automatic,
            min_score=self.min_interesting_score,
            min_price=self.min_price,
            target_annual_km=self.target_annual_km,
            prefer_gearbox=self.prefer_gearbox,
            prefer_drive=self.prefer_drive,
        )
        if scored_offer:
            self.scored_cars.append(scored_offer)
            base_item["offer_score"] = scored_offer["score"]
            base_item["offer_interesting"] = scored_offer["interesting"]
            base_item["offer_reasons"] = scored_offer["reasons"]
            base_item["offer_metrics"] = {
                "price_per_kw": scored_offer["price_per_kw"],
                "price_per_km": scored_offer["price_per_km"],
                "km_per_year": scored_offer["km_per_year"],
                "age_years": scored_offer["age_years"],
                "gearbox_type": scored_offer["gearbox_type"],
                "drive_type": scored_offer["drive_type"],
                "brand_tier": scored_offer["brand_tier"],
                "confidence_score": scored_offer["confidence_score"],
                "listing_age_days": scored_offer["listing_age_days"],
                "months_to_stk": scored_offer["months_to_stk"],
                "euro_value": scored_offer["euro_value"],
                "annual_fuel_cost": scored_offer["annual_fuel_cost"],
                "annual_insurance": scored_offer["annual_insurance"],
                "annual_maintenance": scored_offer["annual_maintenance"],
                "annual_total_cost": scored_offer["annual_total_cost"],
                "estimated_consumption_per_100km": scored_offer["estimated_consumption_per_100km"],
                "model_family_key": scored_offer["model_family_key"],
            }
        else:
            base_item["offer_score"] = None
            base_item["offer_interesting"] = False
            base_item["offer_reasons"] = []

        self.items_scraped += 1
        yield base_item

    def handle_detail_error(self, failure):
        base_item = failure.request.meta.get("base_item") or {}
        base_item["detail_fetch_ok"] = False
        base_item["detail_raw"] = None
        base_item["detail_error"] = str(failure.value)
        self.items_scraped += 1
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
            f"Scored ads: {len(self.scored_cars)}",
            f"Interesting ads (score >= {self.min_interesting_score}): {total_interesting}",
            f"Ranked output: {self.INTERESTING_OFFERS_FILE}",
            (
                f"Market tuning: cohort>={self.market_min_cohort_size}, "
                f"expected km/year={self.market_expected_km_per_year}"
            ),
            (
                f"Ownership tuning: target annual km={self.target_annual_km}, "
                f"prefer gearbox={self.prefer_gearbox}, prefer drive={self.prefer_drive}"
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

        lines.append(f"Top {len(offers)} interesting offers:")
        lines.append("")
        for index, offer in enumerate(offers, 1):
            new_prefix = "NEW " if offer.get("is_new") else ""
            reasons = ", ".join(offer.get("reasons", [])[:4]) or "no specific reasons"
            lines.extend(
                [
                    (
                        f"{index}. {new_prefix}{offer['name']} | {offer['price']} CZK | "
                        f"score {offer['score']} (base {offer.get('base_score', offer['score'])}, "
                        f"adj {offer.get('market_adjustment', 0):+d})"
                    ),
                    (
                        f"   {offer.get('power_kw', 0)} kW | {offer.get('tachometer', 0)} km "
                        f"| age {offer.get('age_years', 0)}y | {offer.get('gearbox_type', 'unknown')}/"
                        f"{offer.get('drive_type', 'unknown')} | conf {offer.get('confidence_score', 0)}"
                    ),
                    (
                        f"   price/kW: {offer.get('price_per_kw')} | price/km: {offer.get('price_per_km')} "
                        f"| km/year: {offer.get('km_per_year')}"
                    ),
                    (
                        f"   annual cost: {offer.get('annual_total_cost')} (fuel {offer.get('annual_fuel_cost')}, "
                        f"insurance {offer.get('annual_insurance')}, service {offer.get('annual_maintenance')}) "
                        f"| STK m: {offer.get('months_to_stk')} | EURO: {offer.get('euro_value')}"
                    ),
                    (
                        f"   model avg: {offer.get('model_avg_price')} | ratio: {offer.get('model_price_ratio')} "
                        f"| valuation: {offer.get('valuation_label')} | brand: {offer.get('brand_tier')}"
                    ),
                    f"   reasons: {reasons}",
                    f"   {offer.get('url') or 'URL missing'}",
                    "",
                ]
            )
        return "\n".join(lines)

    def closed(self, reason):
        sorted_offers = self._apply_advanced_sorting(list(self.scored_cars))
        interesting_offers = [offer for offer in sorted_offers if offer["interesting"]]
        top_offers = interesting_offers[: self.top_n]

        for offer in top_offers:
            is_new = offer["ad_id"] not in self.notified_ids
            offer["is_new"] = is_new
            if is_new:
                self.notified_ids.add(offer["ad_id"])

        if self.discord_notify_only_new:
            offers_for_discord = [offer for offer in top_offers if offer.get("is_new")]
        else:
            offers_for_discord = top_offers

        self._save_sorted_offers(sorted_offers)
        if top_offers:
            self._save_notified()

        message = self._format_discord_message(reason, offers_for_discord, len(interesting_offers))
        self._send_discord(message)