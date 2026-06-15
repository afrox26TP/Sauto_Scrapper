"""
Offline VIN decoder for EU market vehicles.
Decodes WMI → make/country, VDS → body/engine/drive, VIS → year/plant.
Completely offline, no API calls needed.
"""

try:
    from .wmi_db import lookup_wmi
except ImportError:
    from wmi_db import lookup_wmi  # fallback for standalone usage

# ============================================================
# Model Year decoder (VIN position 10)
# ISO 3779 standard - this is UNIVERSAL
# ============================================================
# Model year decoder (VIN position 10, ISO 3779)
# Letters I, O, Q, U, Z and digit 0 are not used as year codes
_MODEL_YEAR_PRE2010 = {
    "A": 1980, "B": 1981, "C": 1982, "D": 1983, "E": 1984,
    "F": 1985, "G": 1986, "H": 1987, "J": 1988, "K": 1989,
    "L": 1990, "M": 1991, "N": 1992, "P": 1993, "R": 1994,
    "S": 1995, "T": 1996, "V": 1997, "W": 1998, "X": 1999,
    "Y": 2000,
}
_MODEL_YEAR_NUMERIC = {
    "1": 2001, "2": 2002, "3": 2003, "4": 2004,
    "5": 2005, "6": 2006, "7": 2007, "8": 2008, "9": 2009,
}
_MODEL_YEAR_POST2010 = {
    "A": 2010, "B": 2011, "C": 2012, "D": 2013, "E": 2014,
    "F": 2015, "G": 2016, "H": 2017, "J": 2018, "K": 2019,
    "L": 2020, "M": 2021, "N": 2022, "P": 2023, "R": 2024,
    "S": 2025, "T": 2026,
}

def decode_model_year(code):
    """
    Decode VIN position 10 to model year.
    Handles the 30-year repeat cycle by preferring 2010+ for letters (A-P)
    and falling back to 1980+ for older cars.
    """
    if not code:
        return None
    c = code.upper()
    
    # Digits 1-9 are unambiguous (2001-2009)
    if c in _MODEL_YEAR_NUMERIC:
        return _MODEL_YEAR_NUMERIC[c]
    
    # Letters: default to 2010+ era (most cars on Sauto are 2010+)
    if c in _MODEL_YEAR_POST2010:
        return _MODEL_YEAR_POST2010[c]
    
    # Fallback to 1980-2000
    if c in _MODEL_YEAR_PRE2010:
        return _MODEL_YEAR_PRE2010[c]
    
    return None

# ============================================================
# Plant codes (VIN position 11) - manufacturer-specific
# ============================================================
PLANT_CODES = {
    # VW Group
    "VW": {
        "A": "Ingolstadt", "B": "Brussels", "C": "Taubaté", "D": "Bratislava",
        "E": "Emden", "F": "Ipiranga", "G": "Graz", "H": "Hanover",
        "J": "Brescia", "K": "Osnabrück", "L": "Leipzig", "M": "Puebla",
        "N": "Neckarsulm", "P": "Mosel", "R": "Resende", "S": "Salzgitter",
        "T": "Sarajevo", "U": "Uitenhage", "V": "Westmoreland", "W": "Wolfsburg",
        "X": "Poznan", "Y": "Pamplona", "0": "Pune", "1": "Skoda",
        "2": "Mladá Boleslav", "3": "Vrchlabí", "4": "Kvasiny",
        "5": "Aurangabad", "6": "Changchun", "7": "Anting",
        "8": "Dresden", "9": "Bratislava",
    },
    # BMW
    "BMW": {
        "A": "Munich", "B": "Dingolfing", "C": "Dingolfing", "D": "Dingolfing",
        "E": "Regensburg", "F": "Regensburg", "G": "Spartanburg",
        "H": "Rosslyn", "J": "Regensburg", "K": "Munich", "L": "Spartanburg",
        "M": "Spartanburg", "N": "Rosslyn", "P": "Regensburg", "R": "Berlin",
        "S": "Shenyang", "T": "Oxford", "U": "Born", "V": "Leipzig",
        "W": "Graz", "X": "Araquari", "Y": "Chennai", "Z": "Rayong",
        "1": "Dingolfing", "2": "Dingolfing", "3": "Steyr",
    },
    # Toyota (EU-focused)
    "Toyota": {
        "0": "Japan", "1": "Japan", "2": "Japan", "3": "Japan", "4": "Japan",
        "5": "Japan", "6": "Japan", "7": "Japan", "8": "Japan", "9": "Japan",
        "A": "Japan", "B": "Japan", "C": "Canada", "D": "USA",
        "E": "USA", "F": "USA", "G": "Mexico", "H": "Mexico",
        "J": "Japan", "K": "UK", "L": "Turkey", "M": "France",
        "N": "South Africa", "P": "Portugal", "R": "Russia",
        "S": "Czech Republic", "T": "India", "U": "China",
        "V": "Argentina", "W": "Brazil", "X": "Poland",
        "Y": "Thailand", "Z": "Malaysia",
    },
    # PSA (Peugeot/Citroen)
    "PSA": {
        "A": "Mulhouse", "B": "Sochaux", "C": "Rennes",
        "D": "Hordain", "E": "Madrid", "F": "Mangualde",
        "G": "Vigo", "H": "Trnava", "J": "Kaluga",
        "K": "Wuhan", "L": "Shenzhen", "M": "Kenitra",
        "N": "Porto Real", "P": "El Palomar",
        "R": "Kaduna", "S": "Sevel Nord", "T": "Kostanay",
        "U": "Bursa", "V": "Luton", "W": "Ellesmere Port",
        "X": "Madagascar", "Y": "Milano", "Z": "Pune",
    },
    # Renault
    "Renault": {
        "A": "Flins", "B": "Sandouville", "C": "Maubeuge",
        "D": "Douai", "E": "Batilly", "F": "Blainville",
        "G": "Valladolid", "H": "Palencia", "J": "Seville",
        "K": "Bursa", "L": "Curitiba", "M": "Santa Isabel",
        "N": "Mioveni", "P": "Tanger", "R": "Moscow",
        "S": "Chennai", "T": "Bushehr", "U": "Novo Mesto",
        "V": "Casablanca", "W": "Córdoba", "X": "Envigado",
        "Y": "Slovenia", "Z": "Aguascalientes",
    },
    # Hyundai/Kia (EU-focused)
    "Hyundai": {
        "A": "Asan", "B": "Ulsan", "C": "Jeonju", "D": "Sohari",
        "E": "Namyang", "F": "Beijing", "G": "Guangzhou",
        "H": "Alabama", "J": "Nošovice", "K": "Saint Petersburg",
        "L": "Izmit", "M": "Chennai", "N": "Ankara",
        "P": "Piracicaba", "R": "Monterrey", "S": "Žilina",
        "T": "Bamberg", "U": "Georgia", "V": "Vietnam",
        "W": "Indonesia", "Z": "Indonesia",
    },
    "Kia": {
        "A": "Hwasung", "B": "Sohari", "C": "Gwangju", "D": "Seosan",
        "E": "Gwangmyeong", "F": "Yancheng", "G": "Georgia",
        "H": "Zilina", "J": "Monterrey", "K": "Kaliningrad",
        "L": "Chennai", "M": "Izmit", "N": "Saint Petersburg",
        "P": "Anantapur", "R": "Batna", "S": "Zilina",
        "T": "Quang Nam", "U": "Karawang", "V": "Chu Lai",
    },
    # Volvo
    "Volvo": {
        "0": "Torslanda", "1": "Torslanda", "2": "Ghent",
        "3": "Samut Prakan", "4": "Kuala Lumpur", "5": "Halifax",
        "6": "Kaluga", "7": "Bangalore", "8": "Luqiao",
        "9": "Chengdu", "A": "Uddevalla", "B": "Ghent",
        "C": "Chongqing", "D": "Daqing", "E": "Charleston",
        "F": "Rockleigh", "G": "Gothenburg", "H": "Shah Alam",
        "J": "Gothenburg", "K": "Markham", "L": "Curitiba",
    },
    # Mercedes-Benz
    "Mercedes-Benz": {
        "A": "Sindelfingen", "B": "Sindelfingen", "C": "Sindelfingen",
        "D": "Bremen", "E": "Rastatt", "F": "Bremen",
        "G": "East London", "H": "Kecskemét", "J": "Rastatt",
        "K": "Pune", "L": "Pekan", "M": "Toluca",
        "N": "Kecskemét", "P": "Graz", "R": "East London",
        "S": "Sindelfingen", "T": "Kecskemét", "U": "Sindelfingen",
        "V": "Vitoria", "W": "Ludwigsfelde", "X": "Bremen",
        "Y": "East London", "Z": "Hambach",
    },
    # Ford (EU)
    "Ford": {
        "A": "Cologne", "B": "Genk", "C": "Saarlouis",
        "D": "Valencia", "E": "Kansas City", "F": "Dearborn",
        "G": "Chicago", "H": "Lorain", "J": "Monterrey",
        "K": "Oakville", "L": "Wayne", "M": "Cuautitlan",
        "N": "Flat Rock", "P": "Tatamy", "R": "Hermosillo",
        "S": "Kocaeli", "T": "Yelabuga", "U": "Louisville",
        "V": "Vsevolozhsk", "W": "Romeo", "X": "St. Thomas",
        "Y": "Wixom", "Z": "St. Petersburg",
    },
    # Suzuki
    "Suzuki": {
        "0": "Hamamatsu", "1": "Hamamatsu", "2": "Kosai",
        "3": "Iwata", "4": "Toyokawa", "5": "Sagara",
        "6": "Magyar", "7": "Gurgaon", "8": "Manesar",
        "9": "Rayong", "A": "Changan", "B": "Karachi",
        "C": "Cikarang", "D": "Yangon", "E": "Bien Hoa",
        "F": "Lahore", "G": "Gurgaon", "H": "Changzhou",
        "J": "Karawang", "K": "Changshu", "L": "Lahore",
    },
    # Subaru
    "Subaru": {
        "0": "Gunma", "1": "Gunma", "2": "Gunma", "3": "Gunma",
        "4": "Lafayette", "5": "Lafayette", "6": "Gunma",
        "7": "Gunma", "8": "Gunma", "9": "Gunma",
    },
    # Mazda
    "Mazda": {
        "0": "Hiroshima", "1": "Hofu", "2": "Hofu",
        "3": "Hiroshima", "4": "Flat Rock", "5": "Rayong",
        "6": "Hofu", "7": "Hiroshima", "8": "Hiroshima",
        "9": "Changan", "A": "Nanjing", "B": "Hiroshima",
        "C": "Salamanca", "D": "Kulim", "E": "Ho Chi Minh",
        "F": "Bogota", "G": "Kuala Lumpur",
    },
    # Tesla
    "Tesla": {
        "F": "Fremont", "G": "Berlin", "H": "Austin",
        "N": "Reno", "R": "Fremont", "A": "Austin",
        "B": "Berlin", "C": "Shanghai", "P": "Sparks",
        "1": "Menlo Park", "3": "Hethel", "S": "Fremont",
    },
}

# ============================================================
# VDS body type codes - manufacturer-specific
# Note: These are the most common patterns for EU market vehicles.
# Position 4-8 encode vehicle attributes varying by manufacturer.
# ============================================================

# VAG Group: Positions 4-6 = vehicle type/market
VAG_BODY = {
    "ZZZ": "EU market (filler)",  # Very common - used for EU vehicles
    "KE2": "Crafter", "KZ2": "Crafter",
    "1ZZ": "Multivan", "2ZZ": "Caravelle",
    "3ZZ": "Transporter", "7ZZ": "Transporter",
}

# VAG engine codes - position 7-8 (partial - too many to list all)
VAG_ENGINE = {
    "A": "Petrol", "B": "Petrol",
    "C": "Diesel", "D": "Diesel",
    "E": "Petrol", "F": "Petrol",
    "G": "Diesel", "H": "Diesel",
    "J": "Electric/BEV", "K": "Hybrid",
    "L": "Diesel", "M": "Petrol",
    "N": "Hybrid", "P": "Plug-in Hybrid",
    "R": "Petrol", "S": "Petrol",
    "T": "Diesel", "U": "Diesel",
    "V": "Petrol", "W": "Petrol",
    "X": "Diesel", "Y": "Petrol",
    "Z": "EV/BEV",
}


def _extract_vin_fields(vin):
    """Break VIN into standard fields."""
    vin = vin.upper().strip()
    if len(vin) != 17:
        return None
    return {
        "wmi": vin[0:3],     # World Manufacturer Identifier
        "vds": vin[3:8],     # Vehicle Descriptor Section
        "check": vin[8],     # Check digit
        "year": vin[9],      # Model year code
        "plant": vin[10],    # Plant code
        "serial": vin[11:17],# Serial number
        "vin": vin,
    }


def _normalize_make(make_name):
    """Normalize make name to match plant code keys."""
    if not make_name:
        return None
    name = make_name.lower()
    # Map to plant code keys
    for key in PLANT_CODES:
        if key.lower() == name:
            return key
        if key.lower() in name or name in key.lower():
            return key
    return None


def decode(vin):
    """
    Decode a 17-character VIN.
    Returns dict with all decoded fields.
    
    Fields decoded:
    - make: manufacturer
    - country: manufacturing country
    - model_year: model year
    - plant: factory name
    - plant_location: country of factory
    - vin: original VIN
    """
    fields = _extract_vin_fields(vin)
    if not fields:
        return {"vin": vin, "error": "Invalid VIN length (must be 17 chars)"}
    
    result = {"vin": vin}
    
    # --- WMI decode ---
    wmi_info = lookup_wmi(fields["wmi"])
    if wmi_info:
        result["make"] = wmi_info["Make"]
        result["country"] = wmi_info["Country"]
    else:
        result["make"] = "Unknown"
        result["country"] = "Unknown"
    
    # --- Model year ---
    my = decode_model_year(fields["year"])
    if my:
        result["model_year"] = my
    
    # --- Plant decode ---
    plant_key = _normalize_make(result.get("make"))
    if plant_key and plant_key in PLANT_CODES:
        pc = PLANT_CODES[plant_key]
        if fields["plant"] in pc:
            result["plant"] = pc[fields["plant"]]
        else:
            result["plant"] = f"Plant code: {fields['plant']}"
    
    # --- VDS hints for VAG group ---
    vds = fields["vds"]
    if result.get("make") in ("Volkswagen", "Audi", "Skoda", "Seat", "Porsche"):
        # Check body marker (positions 4-6 of VIN = vds[0:3])
        body_marker = vds[:3]
        if body_marker in VAG_BODY:
            result["body_type_note"] = VAG_BODY[body_marker]
        # Engine type: For VAG, if positions 4-6 are ZZZ (EU filler),
        # the engine is encoded at position 4 of VDS = vds[3] (7th VIN char)
        if body_marker == "ZZZ":
            engine_code = vds[3] if len(vds) >= 4 else None
        else:
            engine_code = vds[2] if len(vds) >= 3 else None
        if engine_code and engine_code in VAG_ENGINE:
            result["fuel_type_hint"] = VAG_ENGINE[engine_code]
    
    # --- VDS hints for PSA ---
    if result.get("make") in ("Peugeot", "Citroen", "DS", "Opel"):
        body_code = vds[:2]
        if body_code == "30":
            result["body_class"] = "Sedan (4-door)"
        elif body_code == "36":
            result["body_class"] = "Break/Combi/Wagon"
        elif body_code == "31":
            result["body_class"] = "Cabriolet/Coupe"
        elif body_code == "32":
            result["body_class"] = "Minivan/MPV"
        elif body_code == "7R":
            result["body_class"] = "SUV/Crossover"
        elif body_code == "RW":
            result["body_class"] = "SUV/Crossover"
        elif body_code == "SX":
            result["body_class"] = "SUV/Crossover"
    
    # --- VDS for Renault ---
    if result.get("make") == "Renault":
        body_code = vds[:2]
        renault_body = {
            "1F": "Clio", "1K": "Megane", "1M": "Scenic",
            "1R": "Laguna", "1T": "Espace", "1V": "Kangoo",
            "2F": "Twingo", "2K": "Megane II", "5F": "Captur",
            "5V": "Kadjar", "6F": "Zoe", "7R": "Arkana",
        }
        if body_code in renault_body:
            result["model_hint"] = renault_body[body_code]
    
    # --- VDS for Volvo ---
    if result.get("make") == "Volvo":
        # Position 4 = vehicle line (older system)
        chassis_code = vds[0] if vds else None
        if chassis_code == "G":
            result["model_hint"] = "V40/V40 Cross Country"
        elif chassis_code == "M":
            result["model_hint"] = "V60"
        elif chassis_code == "S":
            result["model_hint"] = "S60"
        elif chassis_code == "X":
            result["model_hint"] = "XC60/XC90"
        elif chassis_code == "Y":
            result["model_hint"] = "V90 Cross Country"
    
    # --- VDS for Toyota ---
    if result.get("make") == "Toyota":
        body_prefix = vds[:2]
        toyota_bodies = {
            "KZ": "C-HR", "ZZ": "Yaris/Corolla (EU)",
            "BK": "Auris/Corolla", "BH": "RAV4",
            "BR": "Avensis", "AD": "Corolla Sedan",
            "GK": "Camry", "GS": "Auris",
            "HG": "Hilux", "JT": "Land Cruiser",
            "KN": "Proace", "PG": "Prius",
        }
        for prefix, model in toyota_bodies.items():
            if body_prefix.startswith(prefix[:2]):
                result["model_hint"] = model
                break
    
    return result


# Convenience function
def decode_batch(vins):
    """Decode a list of VINs, return list of dicts."""
    return [decode(v) for v in vins]


if __name__ == "__main__":
    # Quick test
    test_vins = [
        "NMTKZ3BX70R251846",  # Toyota C-HR, Turkey
        "WP1ZZZ9YZJDA61879",  # Porsche Cayenne, 2018
        "TMBLC75L8G6016681",  # Skoda Octavia, 2016, CZ
        "TMBLK7NS5K8057311",  # Skoda Kodiaq, 2019, CZ
        "VF30U5FV8ES215652",  # Peugeot 508, 2014, France
        "WV2ZZZ7HZHH009192",  # VW Transporter, 2017
        "VF7RW5FV8BL534913",  # Citroen C3, 2011
        "VF30E5FS0AS144642",  # Peugeot 308, 2010
        "VF7SXHNZ6HT508593",  # Citroen C4 Cactus, 2017
        "YV1GWDCH0G1311269",  # Volvo XC90, 2016
    ]
    
    print("=" * 70)
    print("OFFLINE VIN DECODER - Test run")
    print("=" * 70)
    
    for vin in test_vins:
        info = decode(vin)
        print(f"\nVIN: {info['vin']}")
        print(f"  Výrobce: {info.get('make', '?')}")
        print(f"  Země: {info.get('country', '?')}")
        if 'model_year' in info:
            print(f"  Modelový rok: {info['model_year']}")
        if 'model_hint' in info:
            print(f"  Odhad modelu: {info['model_hint']}")
        if 'body_class' in info:
            print(f"  Karoserie: {info['body_class']}")
        if 'body_type_note' in info:
            print(f"  Typ VIN: {info['body_type_note']}")
        if 'fuel_type_hint' in info:
            print(f"  Palivo (odhad): {info['fuel_type_hint']}")
        if 'plant' in info:
            print(f"  Továrna: {info['plant']}")