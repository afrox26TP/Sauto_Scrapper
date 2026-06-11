import urllib.request
import os
import time

LOGOS = {
    "skoda": "https://upload.wikimedia.org/wikipedia/commons/thumb/4/49/Skoda_Auto_Logo_%282022%29.svg/120px-Skoda_Auto_Logo_%282022%29.svg.png",
    "vw": "https://upload.wikimedia.org/wikipedia/commons/thumb/6/6d/Volkswagen_logo_2019.svg/120px-Volkswagen_logo_2019.svg.png",
    "audi": "https://upload.wikimedia.org/wikipedia/commons/thumb/9/92/Audi-Logo_2016.svg/120px-Audi-Logo_2016.svg.png",
    "bmw": "https://upload.wikimedia.org/wikipedia/commons/thumb/4/44/BMW.svg/120px-BMW.svg.png",
    "mercedes": "https://upload.wikimedia.org/wikipedia/commons/thumb/9/90/Mercedes-Logo.svg/120px-Mercedes-Logo.svg.png",
    "ford": "https://upload.wikimedia.org/wikipedia/commons/thumb/3/3e/Ford_logo_flat.svg/120px-Ford_logo_flat.svg.png",
    "toyota": "https://upload.wikimedia.org/wikipedia/commons/thumb/9/9d/Toyota_carlogo.svg/120px-Toyota_carlogo.svg.png",
    "honda": "https://upload.wikimedia.org/wikipedia/commons/thumb/3/38/Honda.svg/120px-Honda.svg.png",
    "hyundai": "https://upload.wikimedia.org/wikipedia/commons/thumb/4/44/Hyundai_Motor_Company_logo.svg/120px-Hyundai_Motor_Company_logo.svg.png",
    "kia": "https://upload.wikimedia.org/wikipedia/commons/thumb/9/9f/Kia-logo-2021.svg/120px-Kia-logo-2021.svg.png",
    "mazda": "https://upload.wikimedia.org/wikipedia/commons/thumb/e/e5/Mazda_Motor_Corporation_Logo_2018.svg/120px-Mazda_Motor_Corporation_Logo_2018.svg.png",
    "nissan": "https://upload.wikimedia.org/wikipedia/commons/thumb/2/23/Nissan_2020_Logo.svg/120px-Nissan_2020_Logo.svg.png",
    "opel": "https://upload.wikimedia.org/wikipedia/commons/thumb/a/ad/Opel_logo_2020.svg/120px-Opel_logo_2020.svg.png",
    "peugeot": "https://upload.wikimedia.org/wikipedia/commons/thumb/0/03/Peugeot_2021_Logo.svg/120px-Peugeot_2021_Logo.svg.png",
    "renault": "https://upload.wikimedia.org/wikipedia/commons/thumb/7/75/Renault_2021_Logo.svg/120px-Renault_2021_Logo.svg.png",
    "seat": "https://upload.wikimedia.org/wikipedia/commons/thumb/f/f7/SEAT_Logo_from_2017.svg/120px-SEAT_Logo_from_2017.svg.png",
    "citroen": "https://upload.wikimedia.org/wikipedia/commons/thumb/9/9b/Citroen_Logo_2022.svg/120px-Citroen_Logo_2022.svg.png",
    "fiat": "https://upload.wikimedia.org/wikipedia/commons/thumb/9/99/Fiat_Logo_2020.svg/120px-Fiat_Logo_2020.svg.png",
    "volvo": "https://upload.wikimedia.org/wikipedia/commons/thumb/a/a2/Volvo_logo_2021.svg/120px-Volvo_logo_2021.svg.png",
    "dacia": "https://upload.wikimedia.org/wikipedia/commons/thumb/d/d2/Dacia_Logo_2021.svg/120px-Dacia_Logo_2021.svg.png",
    "suzuki": "https://upload.wikimedia.org/wikipedia/commons/thumb/1/1b/Suzuki_logo_2.svg/120px-Suzuki_logo_2.svg.png",
    "mitsubishi": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/5a/Mitsubishi_logo.svg/120px-Mitsubishi_logo.svg.png",
    "subaru": "https://upload.wikimedia.org/wikipedia/commons/thumb/8/82/Subaru_Logo_2019.svg/120px-Subaru_Logo_2019.svg.png",
    "porsche": "https://upload.wikimedia.org/wikipedia/commons/thumb/5/59/Porsche_Logo_2023.svg/120px-Porsche_Logo_2023.svg.png",
    "landrover": "https://upload.wikimedia.org/wikipedia/commons/thumb/e/ec/Land_Rover_Logo_2021.svg/120px-Land_Rover_Logo_2021.svg.png",
    "jaguar": "https://upload.wikimedia.org/wikipedia/commons/thumb/1/1e/Jaguar_Jumping_2021.svg/120px-Jaguar_Jumping_2021.svg.png",
    "tesla": "https://upload.wikimedia.org/wikipedia/commons/thumb/b/bd/Tesla_Motors.svg/120px-Tesla_Motors.svg.png",
    "mini": "https://upload.wikimedia.org/wikipedia/commons/thumb/e/e9/MINI_Logo_2018.svg/120px-MINI_Logo_2018.svg.png",
    "ferrari": "https://upload.wikimedia.org/wikipedia/commons/thumb/d/dc/Ferrari_logo_2024.svg/120px-Ferrari_logo_2024.svg.png",
    "lamborghini": "https://upload.wikimedia.org/wikipedia/commons/thumb/7/75/Lamborghini_Logo_2024.svg/120px-Lamborghini_Logo_2024.svg.png",
    "maserati": "https://upload.wikimedia.org/wikipedia/commons/thumb/1/1b/Maserati_logo_2020.svg/120px-Maserati_logo_2020.svg.png",
    "alfaromeo": "https://upload.wikimedia.org/wikipedia/commons/thumb/0/00/Alfa_Romeo_logo_2015.svg/120px-Alfa_Romeo_logo_2015.svg.png",
}

OUT_DIR = os.path.join(os.path.dirname(__file__), "web-ui", "public", "logos")
os.makedirs(OUT_DIR, exist_ok=True)

for name, url in LOGOS.items():
    time.sleep(2)
    out_path = os.path.join(OUT_DIR, f"{name}.png")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        data = urllib.request.urlopen(req, timeout=15).read()
        with open(out_path, "wb") as f:
            f.write(data)
        print(f"OK  {name}.png  ({len(data)} bytes)")
    except Exception as e:
        print(f"ERR {name}: {e}")

print("\nDone!")