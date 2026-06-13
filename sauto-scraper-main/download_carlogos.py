import urllib.request
import os
import time

LOGOS = {
    "skoda": "https://www.carlogos.org/car-logos/skoda-logo.png",
    "volkswagen": "https://www.carlogos.org/car-logos/volkswagen-logo.png",
    "audi": "https://www.carlogos.org/car-logos/audi-logo.png",
    "bmw": "https://www.carlogos.org/car-logos/bmw-logo.png",
    "mercedes-benz": "https://www.carlogos.org/car-logos/mercedes-benz-logo.png",
    "ford": "https://www.carlogos.org/car-logos/ford-logo.png",
    "toyota": "https://www.carlogos.org/car-logos/toyota-logo.png",
    "honda": "https://www.carlogos.org/car-logos/honda-logo.png",
    "hyundai": "https://www.carlogos.org/car-logos/hyundai-logo.png",
    "kia": "https://www.carlogos.org/car-logos/kia-logo.png",
    "mazda": "https://www.carlogos.org/car-logos/mazda-logo.png",
    "nissan": "https://www.carlogos.org/car-logos/nissan-logo.png",
    "opel": "https://www.carlogos.org/car-logos/opel-logo.png",
    "peugeot": "https://www.carlogos.org/car-logos/peugeot-logo.png",
    "renault": "https://www.carlogos.org/car-logos/renault-logo.png",
    "seat": "https://www.carlogos.org/car-logos/seat-logo.png",
    "citroen": "https://www.carlogos.org/car-logos/citroen-logo.png",
    "fiat": "https://www.carlogos.org/car-logos/fiat-logo.png",
    "volvo": "https://www.carlogos.org/car-logos/volvo-logo.png",
    "dacia": "https://www.carlogos.org/car-logos/dacia-logo.png",
    "suzuki": "https://www.carlogos.org/car-logos/suzuki-logo.png",
    "mitsubishi": "https://www.carlogos.org/car-logos/mitsubishi-logo.png",
    "subaru": "https://www.carlogos.org/car-logos/subaru-logo.png",
    "porsche": "https://www.carlogos.org/car-logos/porsche-logo.png",
    "land-rover": "https://www.carlogos.org/car-logos/land-rover-logo.png",
    "jaguar": "https://www.carlogos.org/car-logos/jaguar-logo.png",
    "tesla": "https://www.carlogos.org/car-logos/tesla-logo.png",
    "mini": "https://www.carlogos.org/car-logos/mini-logo.png",
    "ferrari": "https://www.carlogos.org/car-logos/ferrari-logo.png",
    "lamborghini": "https://www.carlogos.org/car-logos/lamborghini-logo.png",
    "maserati": "https://www.carlogos.org/car-logos/maserati-logo.png",
    "alfa-romeo": "https://www.carlogos.org/car-logos/alfa-romeo-logo.png",
    "chevrolet": "https://www.carlogos.org/car-logos/chevrolet-logo.png",
    "lexus": "https://www.carlogos.org/car-logos/lexus-logo.png",
    "infiniti": "https://www.carlogos.org/car-logos/infiniti-logo.png",
    "acura": "https://www.carlogos.org/car-logos/acura-logo.png",
    "cadillac": "https://www.carlogos.org/car-logos/cadillac-logo.png",
    "chrysler": "https://www.carlogos.org/car-logos/chrysler-logo.png",
    "dodge": "https://www.carlogos.org/car-logos/dodge-logo.png",
    "jeep": "https://www.carlogos.org/car-logos/jeep-logo.png",
    "bentley": "https://www.carlogos.org/car-logos/bentley-logo.png",
    "aston-martin": "https://www.carlogos.org/car-logos/aston-martin-logo.png",
    "mclaren": "https://www.carlogos.org/car-logos/mclaren-logo.png",
    "saab": "https://www.carlogos.org/car-logos/saab-logo.png",
    "genesis": "https://www.carlogos.org/car-logos/genesis-logo.png",
    "smart": "https://www.carlogos.org/car-logos/smart-logo.png",
    "lada": "https://www.carlogos.org/car-logos/lada-logo.png",
    "alpina": "https://www.carlogos.org/car-logos/alpina-logo.png",
    "byd": "https://www.carlogos.org/car-logos/byd-logo.png",
    "cupra": "https://www.carlogos.org/car-logos/cupra-logo.png",
    "daewoo": "https://www.carlogos.org/car-logos/daewoo-logo.png",
    "daihatsu": "https://www.carlogos.org/car-logos/daihatsu-logo.png",
    "ds": "https://www.carlogos.org/car-logos/ds-logo.png",
    "hummer": "https://www.carlogos.org/car-logos/hummer-logo.png",
    "jaecoo": "https://www.carlogos.org/car-logos/jaecoo-logo.png",
    "lancia": "https://www.carlogos.org/car-logos/lancia-logo.png",
    "polestar": "https://www.carlogos.org/car-logos/polestar-logo.png",
    "ram": "https://www.carlogos.org/car-logos/ram-logo.png",
    "ssangyong": "https://www.carlogos.org/car-logos/ssangyong-logo.png",
}

OUT_DIR = os.path.join(os.path.dirname(__file__), "web-ui", "public", "logos")
os.makedirs(OUT_DIR, exist_ok=True)

for name, url in LOGOS.items():
    out_path = os.path.join(OUT_DIR, f"{name}.png")
    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        print(f"SKIP {name}.png (already exists)")
        continue
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120"})
    try:
        data = urllib.request.urlopen(req, timeout=15).read()
        with open(out_path, "wb") as f:
            f.write(data)
        print(f"OK  {name}.png  ({len(data)} bytes)")
    except Exception as e:
        print(f"ERR {name}: {e}")
    time.sleep(1)

print("\nDone!")