"""
Complete WMI (World Manufacturer Identifier) database.
First 3 characters of VIN identify manufacturer and country.
Based on ISO 3780 / SAE J1044 standards.
"""

# Format: WMI -> {Make, Country, Parent (optional)}
WMI_DB = {
    # === VW Group ===
    # Audi
    "WAU": {"Make": "Audi", "Country": "Germany"},
    "WUA": {"Make": "Audi Sport", "Country": "Germany"},
    "TRU": {"Make": "Audi", "Country": "Hungary"},
    # Volkswagen
    "WVW": {"Make": "Volkswagen", "Country": "Germany"},
    "WV1": {"Make": "Volkswagen", "Country": "Germany"},
    "WV2": {"Make": "Volkswagen", "Country": "Germany"},
    "WV3": {"Make": "Volkswagen", "Country": "Germany"},
    "VWV": {"Make": "Volkswagen", "Country": "Spain"},
    "VW2": {"Make": "Volkswagen", "Country": "Brazil"},
    "1VW": {"Make": "Volkswagen", "Country": "USA"},
    "3VW": {"Make": "Volkswagen", "Country": "Mexico"},
    "AAV": {"Make": "Volkswagen", "Country": "South Africa"},
    "XW8": {"Make": "Volkswagen", "Country": "Russia"},
    # Skoda
    "TMB": {"Make": "Skoda", "Country": "Czech Republic"},
    "TMP": {"Make": "Skoda", "Country": "Czech Republic"},
    "TMC": {"Make": "Skoda", "Country": "Czech Republic"},
    # Seat
    "VSS": {"Make": "Seat", "Country": "Spain"},
    "VS1": {"Make": "Seat", "Country": "Spain"},
    # Porsche
    "WP0": {"Make": "Porsche", "Country": "Germany"},
    "WP1": {"Make": "Porsche", "Country": "Germany"},
    "WP2": {"Make": "Porsche", "Country": "Germany"},
    # Bentley
    "SCB": {"Make": "Bentley", "Country": "UK"},
    "SJA": {"Make": "Bentley", "Country": "UK"},
    # Lamborghini
    "ZHW": {"Make": "Lamborghini", "Country": "Italy"},
    "ZPB": {"Make": "Lamborghini", "Country": "Italy"},

    # === BMW Group ===
    "WBA": {"Make": "BMW", "Country": "Germany"},
    "WBS": {"Make": "BMW M", "Country": "Germany"},
    "WBY": {"Make": "BMW i", "Country": "Germany"},
    "WBX": {"Make": "BMW SUV", "Country": "USA"},
    "WMW": {"Make": "MINI", "Country": "UK/Germany"},
    "WDS": {"Make": "MINI", "Country": "Austria"},

    # === Mercedes-Benz Group ===
    "WDD": {"Make": "Mercedes-Benz", "Country": "Germany"},
    "WDB": {"Make": "Mercedes-Benz", "Country": "Germany"},
    "WDC": {"Make": "Mercedes-Benz SUV", "Country": "Germany"},
    "WDF": {"Make": "Mercedes-Benz Vans", "Country": "Germany"},
    "W1N": {"Make": "Mercedes-Benz SUV", "Country": "USA"},
    "W1K": {"Make": "Mercedes-Benz", "Country": "Germany"},
    "W1V": {"Make": "Mercedes-Benz Vans", "Country": "Spain"},
    "W1T": {"Make": "Mercedes-Benz Vans", "Country": "Germany"},
    "W1W": {"Make": "Mercedes-Benz", "Country": "South Africa"},
    "W1Z": {"Make": "Mercedes-Benz", "Country": "India"},
    "WKK": {"Make": "Mercedes-Benz", "Country": "Germany"},
    "WME": {"Make": "smart", "Country": "Germany"},
    "W5A": {"Make": "smart", "Country": "China"},

    # === Stellantis (PSA + FCA) ===
    # Peugeot
    "VF3": {"Make": "Peugeot", "Country": "France"},
    "VR3": {"Make": "Peugeot", "Country": "France"},
    "VGA": {"Make": "Peugeot", "Country": "Argentina"},
    "8AD": {"Make": "Peugeot", "Country": "Argentina"},
    # Citroen
    "VF7": {"Make": "Citroen", "Country": "France"},
    "VR7": {"Make": "Citroen", "Country": "France"},
    "935": {"Make": "Citroen", "Country": "Brazil"},
    # DS
    "VYF": {"Make": "DS", "Country": "France"},
    # Opel/Vauxhall
    "W0L": {"Make": "Opel", "Country": "Germany"},
    "W0V": {"Make": "Opel", "Country": "Germany"},
    "W0P": {"Make": "Opel", "Country": "Germany"},
    "VSX": {"Make": "Opel", "Country": "UK"},
    "XUF": {"Make": "Opel", "Country": "Russia"},
    "SCC": {"Make": "Vauxhall", "Country": "UK"},
    # Fiat
    "ZFA": {"Make": "Fiat", "Country": "Italy"},
    "ZFB": {"Make": "Fiat", "Country": "Italy"},
    "ZFC": {"Make": "Fiat", "Country": "Italy"},
    "ZDF": {"Make": "Fiat", "Country": "Italy"},
    "9BD": {"Make": "Fiat", "Country": "Brazil"},
    "8AP": {"Make": "Fiat", "Country": "Argentina"},
    "SU9": {"Make": "Fiat", "Country": "Poland"},
    # Alfa Romeo
    "ZAR": {"Make": "Alfa Romeo", "Country": "Italy"},
    "ZAS": {"Make": "Alfa Romeo", "Country": "Italy"},
    # Lancia
    "ZLA": {"Make": "Lancia", "Country": "Italy"},
    # Jeep
    "1C4": {"Make": "Jeep", "Country": "USA"},
    "1J4": {"Make": "Jeep", "Country": "USA"},
    "3C4": {"Make": "Jeep", "Country": "Mexico"},
    "MC4": {"Make": "Jeep", "Country": "Italy"},
    "ZAC": {"Make": "Jeep", "Country": "Italy"},
    # Dodge
    "1B3": {"Make": "Dodge", "Country": "USA"},
    "2B3": {"Make": "Dodge", "Country": "Canada"},
    "3B3": {"Make": "Dodge", "Country": "Mexico"},
    # Chrysler
    "1C3": {"Make": "Chrysler", "Country": "USA"},
    "2C3": {"Make": "Chrysler", "Country": "Canada"},
    "3C3": {"Make": "Chrysler", "Country": "Mexico"},
    # Maserati
    "ZAM": {"Make": "Maserati", "Country": "Italy"},
    "ZN6": {"Make": "Maserati", "Country": "Italy"},

    # === Renault-Nissan-Mitsubishi ===
    # Renault
    "VF1": {"Make": "Renault", "Country": "France"},
    "VFA": {"Make": "Renault", "Country": "France"},
    "VF2": {"Make": "Renault Van", "Country": "France"},
    "VF6": {"Make": "Renault Trucks", "Country": "France"},
    "VF8": {"Make": "Renault", "Country": "France"},
    "VNE": {"Make": "Renault Trucks", "Country": "France"},
    "VVY": {"Make": "Renault", "Country": "Turkey"},
    "VKR": {"Make": "Renault", "Country": "South Korea"},
    "8A1": {"Make": "Renault", "Country": "Argentina"},
    "93Y": {"Make": "Renault", "Country": "Brazil"},
    "VF9": {"Make": "Renault Sport", "Country": "France"},
    # Dacia
    "UU1": {"Make": "Dacia", "Country": "Romania"},
    "UUV": {"Make": "Dacia", "Country": "Romania"},
    # Alpine
    "VJA": {"Make": "Alpine", "Country": "France"},
    # Nissan
    "JN1": {"Make": "Nissan", "Country": "Japan"},
    "JN3": {"Make": "Nissan", "Country": "Japan"},
    "JN6": {"Make": "Nissan", "Country": "Japan"},
    "1N4": {"Make": "Nissan", "Country": "USA"},
    "1N6": {"Make": "Nissan", "Country": "USA"},
    "3N1": {"Make": "Nissan", "Country": "Mexico"},
    "VSK": {"Make": "Nissan", "Country": "Spain"},
    "SJN": {"Make": "Nissan", "Country": "UK"},
    "MNT": {"Make": "Nissan", "Country": "UK/Spain"},
    "94D": {"Make": "Nissan", "Country": "Brazil"},
    # Mitsubishi
    "JMB": {"Make": "Mitsubishi", "Country": "Japan"},
    "JMF": {"Make": "Mitsubishi", "Country": "Japan"},
    "JMP": {"Make": "Mitsubishi", "Country": "Japan"},
    "JMR": {"Make": "Mitsubishi", "Country": "Japan"},
    "JA3": {"Make": "Mitsubishi", "Country": "Japan"},
    "4A3": {"Make": "Mitsubishi", "Country": "USA"},
    "MMC": {"Make": "Mitsubishi", "Country": "Thailand"},
    "MMB": {"Make": "Mitsubishi", "Country": "Thailand"},

    # === Toyota Group ===
    "JTD": {"Make": "Toyota", "Country": "Japan"},
    "JTE": {"Make": "Toyota SUV", "Country": "Japan"},
    "JT1": {"Make": "Toyota", "Country": "Japan"},
    "JT2": {"Make": "Toyota", "Country": "Japan"},
    "JT3": {"Make": "Toyota SUV", "Country": "Japan"},
    "JT4": {"Make": "Toyota", "Country": "Japan"},
    "JTM": {"Make": "Toyota", "Country": "Japan"},
    "JTN": {"Make": "Toyota", "Country": "Japan"},
    "JTA": {"Make": "Toyota", "Country": "Japan"},
    "1NX": {"Make": "Toyota", "Country": "USA"},
    "2T1": {"Make": "Toyota", "Country": "Canada"},
    "4T1": {"Make": "Toyota", "Country": "USA"},
    "5TD": {"Make": "Toyota SUV", "Country": "USA"},
    "5TF": {"Make": "Toyota Truck", "Country": "USA"},
    "8AJ": {"Make": "Toyota", "Country": "Argentina"},
    "9BR": {"Make": "Toyota", "Country": "Brazil"},
    "MBJ": {"Make": "Toyota", "Country": "India"},
    "NMT": {"Make": "Toyota", "Country": "Turkey"},
    "SB1": {"Make": "Toyota", "Country": "UK"},
    "TMK": {"Make": "Toyota", "Country": "Czech Republic"},
    "TW1": {"Make": "Toyota", "Country": "Portugal"},
    "VNK": {"Make": "Toyota", "Country": "France"},
    "PN1": {"Make": "Toyota", "Country": "Malaysia"},
    "MR0": {"Make": "Toyota", "Country": "Thailand"},
    "LFM": {"Make": "Toyota", "Country": "China"},
    # Lexus
    "JTH": {"Make": "Lexus", "Country": "Japan"},
    "JTJ": {"Make": "Lexus SUV", "Country": "Japan"},
    "JT8": {"Make": "Lexus", "Country": "Japan"},
    "2T2": {"Make": "Lexus SUV", "Country": "Canada"},
    "58A": {"Make": "Lexus", "Country": "USA"},

    # === Honda ===
    "JHM": {"Make": "Honda", "Country": "Japan"},
    "JH1": {"Make": "Honda", "Country": "Japan"},
    "JH2": {"Make": "Honda Motorcycle", "Country": "Japan"},
    "JH3": {"Make": "Honda ATV", "Country": "Japan"},
    "JH4": {"Make": "Honda (Acura)", "Country": "Japan"},
    "JH5": {"Make": "Honda", "Country": "Japan"},
    "1HG": {"Make": "Honda", "Country": "USA"},
    "2HG": {"Make": "Honda", "Country": "Canada"},
    "3HG": {"Make": "Honda", "Country": "Mexico"},
    "5FN": {"Make": "Honda SUV", "Country": "USA"},
    "LUC": {"Make": "Honda", "Country": "China"},
    "MAK": {"Make": "Honda", "Country": "India"},
    "MHR": {"Make": "Honda", "Country": "Indonesia"},
    "MLH": {"Make": "Honda", "Country": "Thailand"},
    "MRH": {"Make": "Honda", "Country": "Thailand"},
    "NLA": {"Make": "Honda", "Country": "Turkey"},
    "PAD": {"Make": "Honda", "Country": "Philippines"},
    "PMH": {"Make": "Honda", "Country": "Malaysia"},
    "SHS": {"Make": "Honda", "Country": "UK"},
    "VTM": {"Make": "Honda Motorcycle", "Country": "Spain"},
    "ZDC": {"Make": "Honda Motorcycle", "Country": "Italy"},

    # === Hyundai-Kia ===
    # Hyundai
    "KMH": {"Make": "Hyundai", "Country": "South Korea"},
    "KMF": {"Make": "Hyundai", "Country": "South Korea"},
    "KMJ": {"Make": "Hyundai", "Country": "South Korea"},
    "KMT": {"Make": "Hyundai", "Country": "South Korea"},
    "KM8": {"Make": "Hyundai", "Country": "South Korea"},
    "5NP": {"Make": "Hyundai", "Country": "USA"},
    "5NM": {"Make": "Hyundai", "Country": "USA"},
    "1F3": {"Make": "Hyundai", "Country": "USA"},
    "MAL": {"Make": "Hyundai", "Country": "India"},
    "MB2": {"Make": "Hyundai", "Country": "India"},
    "NLH": {"Make": "Hyundai", "Country": "Turkey"},
    "TMC": {"Make": "Hyundai", "Country": "Czech Republic"},
    "TMH": {"Make": "Hyundai", "Country": "Czech Republic"},
    "94G": {"Make": "Hyundai", "Country": "Brazil"},
    "LBE": {"Make": "Hyundai", "Country": "China"},
    "LBH": {"Make": "Hyundai", "Country": "China"},
    "X7M": {"Make": "Hyundai", "Country": "Russia"},
    "XWE": {"Make": "Hyundai", "Country": "Russia"},
    # Kia
    "KNA": {"Make": "Kia", "Country": "South Korea"},
    "KNC": {"Make": "Kia", "Country": "South Korea"},
    "KND": {"Make": "Kia SUV", "Country": "South Korea"},
    "KNE": {"Make": "Kia", "Country": "South Korea"},
    "KNF": {"Make": "Kia", "Country": "South Korea"},
    "KNG": {"Make": "Kia", "Country": "South Korea"},
    "3KP": {"Make": "Kia", "Country": "Mexico"},
    "5XX": {"Make": "Kia", "Country": "USA"},
    "5XY": {"Make": "Kia SUV", "Country": "USA"},
    "U5Y": {"Make": "Kia", "Country": "Slovakia"},
    "U6Y": {"Make": "Kia", "Country": "Slovakia"},
    "LJD": {"Make": "Kia", "Country": "China"},
    "MZB": {"Make": "Kia", "Country": "India"},
    "PNA": {"Make": "Kia", "Country": "Malaysia"},
    "X7W": {"Make": "Kia", "Country": "Russia"},
    "8LG": {"Make": "Kia", "Country": "Argentina"},
    # Genesis
    "KMT": {"Make": "Genesis", "Country": "South Korea"},
    "KMU": {"Make": "Genesis", "Country": "South Korea"},

    # === Ford ===
    "WF0": {"Make": "Ford", "Country": "Germany"},
    "WF1": {"Make": "Ford", "Country": "Germany"},
    "1FA": {"Make": "Ford", "Country": "USA"},
    "1FB": {"Make": "Ford", "Country": "USA"},
    "1FC": {"Make": "Ford", "Country": "USA"},
    "1FD": {"Make": "Ford Truck", "Country": "USA"},
    "1FM": {"Make": "Ford SUV", "Country": "USA"},
    "2FD": {"Make": "Ford", "Country": "Canada"},
    "2FM": {"Make": "Ford SUV", "Country": "Canada"},
    "3FA": {"Make": "Ford", "Country": "Mexico"},
    "1ZV": {"Make": "Ford", "Country": "USA"},
    "MAJ": {"Make": "Ford", "Country": "India"},
    "MNB": {"Make": "Ford", "Country": "South Africa"},
    "NM0": {"Make": "Ford", "Country": "Turkey"},
    "NM1": {"Make": "Ford", "Country": "Turkey"},
    "PE1": {"Make": "Ford", "Country": "Philippines"},
    "PE3": {"Make": "Ford", "Country": "Philippines"},
    "RLM": {"Make": "Ford", "Country": "Vietnam"},
    "SYY": {"Make": "Ford", "Country": "Spain"},
    "TW2": {"Make": "Ford", "Country": "Taiwan"},
    "VS6": {"Make": "Ford", "Country": "Spain"},
    "WF0": {"Make": "Ford", "Country": "Germany"},
    "WFO": {"Make": "Ford", "Country": "Germany"},
    "X9F": {"Make": "Ford", "Country": "Russia"},
    "Z6F": {"Make": "Ford", "Country": "Russia"},
    # Lincoln
    "1LN": {"Make": "Lincoln", "Country": "USA"},
    "2LN": {"Make": "Lincoln", "Country": "Canada"},
    "3LN": {"Make": "Lincoln", "Country": "Mexico"},
    "5LM": {"Make": "Lincoln SUV", "Country": "USA"},
    "5L1": {"Make": "Lincoln SUV", "Country": "USA"},

    # === General Motors ===
    # Chevrolet
    "1G1": {"Make": "Chevrolet", "Country": "USA"},
    "1G2": {"Make": "Chevrolet", "Country": "USA"},
    "1GC": {"Make": "Chevrolet Truck", "Country": "USA"},
    "3G1": {"Make": "Chevrolet", "Country": "Mexico"},
    "KL1": {"Make": "Chevrolet", "Country": "South Korea"},
    "KL7": {"Make": "Chevrolet", "Country": "South Korea"},
    "9BG": {"Make": "Chevrolet", "Country": "Brazil"},
    "8AG": {"Make": "Chevrolet", "Country": "Argentina"},
    "XUF": {"Make": "Chevrolet", "Country": "Russia"},
    "XWF": {"Make": "Chevrolet", "Country": "Uzbekistan"},
    "LSG": {"Make": "Chevrolet", "Country": "China"},
    # Opel (now Stellantis, but some older GM Opels)
    "W0L": {"Make": "Opel", "Country": "Germany"},
    # Cadillac
    "1G6": {"Make": "Cadillac", "Country": "USA"},
    "1GY": {"Make": "Cadillac", "Country": "USA"},
    "2G6": {"Make": "Cadillac", "Country": "Canada"},
    "3G6": {"Make": "Cadillac", "Country": "Mexico"},
    "LGE": {"Make": "Cadillac", "Country": "China"},
    # GMC
    "1GT": {"Make": "GMC Truck", "Country": "USA"},
    "1GK": {"Make": "GMC SUV", "Country": "USA"},
    "2GT": {"Make": "GMC Truck", "Country": "Canada"},

    # === Volvo ===
    "YV1": {"Make": "Volvo", "Country": "Sweden"},
    "YV2": {"Make": "Volvo Trucks", "Country": "Sweden"},
    "YV3": {"Make": "Volvo Buses", "Country": "Sweden"},
    "YV4": {"Make": "Volvo", "Country": "Sweden"},
    "4V1": {"Make": "Volvo", "Country": "USA"},
    "4V4": {"Make": "Volvo Trucks", "Country": "USA"},
    "LYV": {"Make": "Volvo", "Country": "China"},
    "XLB": {"Make": "Volvo", "Country": "Netherlands"},
    "PNV": {"Make": "Volvo", "Country": "Malaysia"},
    "7JD": {"Make": "Volvo", "Country": "Belgium"},

    # === Jaguar Land Rover ===
    "SAL": {"Make": "Land Rover", "Country": "UK"},
    "SALL": {"Make": "Land Rover", "Country": "UK"},
    "SAJ": {"Make": "Jaguar", "Country": "UK"},
    "SAD": {"Make": "Jaguar", "Country": "UK"},
    "L2C": {"Make": "Jaguar Land Rover", "Country": "China"},
    "LZC": {"Make": "Jaguar Land Rover", "Country": "China"},

    # === Subaru ===
    "JF1": {"Make": "Subaru", "Country": "Japan"},
    "JF2": {"Make": "Subaru SUV", "Country": "Japan"},
    "JF3": {"Make": "Subaru", "Country": "Japan"},
    "4S3": {"Make": "Subaru", "Country": "USA"},
    "4S4": {"Make": "Subaru", "Country": "USA"},

    # === Mazda ===
    "JMZ": {"Make": "Mazda", "Country": "Japan"},
    "JM0": {"Make": "Mazda", "Country": "Japan"},
    "JM1": {"Make": "Mazda", "Country": "Japan"},
    "JM3": {"Make": "Mazda SUV", "Country": "Japan"},
    "JM6": {"Make": "Mazda", "Country": "Japan"},
    "JM7": {"Make": "Mazda", "Country": "Japan"},
    "1YV": {"Make": "Mazda", "Country": "USA"},
    "4FZ": {"Make": "Mazda", "Country": "USA"},
    "MMZ": {"Make": "Mazda", "Country": "Thailand"},
    "PMZ": {"Make": "Mazda", "Country": "Malaysia"},
    "VSP": {"Make": "Mazda", "Country": "Spain"},
    "LDM": {"Make": "Mazda", "Country": "China"},

    # === Suzuki ===
    "JSA": {"Make": "Suzuki", "Country": "Japan"},
    "JS1": {"Make": "Suzuki Motorcycle", "Country": "Japan"},
    "JS2": {"Make": "Suzuki", "Country": "Japan"},
    "JS3": {"Make": "Suzuki SUV", "Country": "Japan"},
    "MA3": {"Make": "Suzuki", "Country": "India"},
    "MBH": {"Make": "Suzuki (Maruti)", "Country": "India"},
    "MMS": {"Make": "Suzuki", "Country": "Thailand"},
    "TSM": {"Make": "Suzuki", "Country": "Hungary"},
    "VSE": {"Make": "Suzuki", "Country": "Spain"},

    # === Tesla ===
    "5YJ": {"Make": "Tesla", "Country": "USA"},
    "7SA": {"Make": "Tesla", "Country": "USA"},
    "SFZ": {"Make": "Tesla", "Country": "UK"},
    "XP7": {"Make": "Tesla", "Country": "Netherlands"},
    "LRW": {"Make": "Tesla", "Country": "China"},

    # === Ferrari ===
    "ZFF": {"Make": "Ferrari", "Country": "Italy"},
    "ZFR": {"Make": "Ferrari", "Country": "Italy"},

    # === Aston Martin ===
    "SCF": {"Make": "Aston Martin", "Country": "UK"},
    "SDP": {"Make": "Aston Martin", "Country": "UK"},

    # === Rolls-Royce ===
    "SCA": {"Make": "Rolls-Royce", "Country": "UK"},
    "SAA": {"Make": "Rolls-Royce", "Country": "UK"},

    # === McLaren ===
    "SBM": {"Make": "McLaren", "Country": "UK"},

    # === Lotus ===
    "SCC": {"Make": "Lotus", "Country": "UK"},

    # === Polestar ===
    "YSM": {"Make": "Polestar", "Country": "Sweden"},
    "LPS": {"Make": "Polestar", "Country": "China"},
    "YSR": {"Make": "Polestar", "Country": "Sweden"},

    # === Other European ===
    "XTA": {"Make": "Lada", "Country": "Russia"},
    "XTB": {"Make": "Lada", "Country": "Russia"},
    "XWK": {"Make": "Lada", "Country": "Russia"},
    "UU1": {"Make": "Dacia", "Country": "Romania"},
    "UUV": {"Make": "Dacia", "Country": "Romania"},
    "ZAP": {"Make": "Piaggio", "Country": "Italy"},
    
    # === Additional European (VW Group) ===
    "WVG": {"Make": "Volkswagen", "Country": "Germany"},
    "VW1": {"Make": "Volkswagen", "Country": "Portugal"},
    
    # === Honda UK ===
    "SHH": {"Make": "Honda", "Country": "UK"},
    
    # === Stellantis (RAM/Chrysler/Dodge) ===
    "1C6": {"Make": "RAM", "Country": "USA"},
    "2C4": {"Make": "Chrysler", "Country": "Canada"},
    
    # === GM ===
    "1GN": {"Make": "GMC SUV", "Country": "USA"},
    
    # === Toyota (additional) ===
    "YAR": {"Make": "Toyota", "Country": "South Africa"},
    
    # === Various Chinese / EU market ===
    "LVY": {"Make": "DFSK", "Country": "China"},
    "VXK": {"Make": "Opel", "Country": "Spain"},
    "VXE": {"Make": "Opel", "Country": "France"},
    "KPT": {"Make": "SsangYong", "Country": "South Korea"},
    "SGT": {"Make": "Setra (Daimler)", "Country": "Germany"},
    
    # === Ford Australia / Others ===
    "6FP": {"Make": "Ford", "Country": "Australia"},
    "AHT": {"Make": "Toyota", "Country": "South Africa"},
    
    # === Tesla / Others (TMA is not Tesla) ===
    "TMA": {"Make": "Hyundai", "Country": "Czech Republic"},

    # === Chinese Brands ===
    "LGW": {"Make": "Great Wall", "Country": "China"},
    "LGX": {"Make": "BYD", "Country": "China"},
    "LNB": {"Make": "BAIC", "Country": "China"},
    "LMB": {"Make": "Brilliance", "Country": "China"},
    "LS6": {"Make": "Changan", "Country": "China"},
    "LTV": {"Make": "Chery", "Country": "China"},
    "LVV": {"Make": "Chery", "Country": "China"},
    "LZE": {"Make": "SAIC", "Country": "China"},
    "MXV": {"Make": "Geely", "Country": "China"},
    "L6T": {"Make": "Geely", "Country": "China"},
    "LPR": {"Make": "Haval", "Country": "China"},
    "LZM": {"Make": "Hongqi", "Country": "China"},
    "LVH": {"Make": "Dongfeng", "Country": "China"},
    "LL3": {"Make": "Xpeng", "Country": "China"},
    "L1N": {"Make": "NIO", "Country": "China"},
    "HLM": {"Make": "Li Auto", "Country": "China"},
    "LRU": {"Make": "Leapmotor", "Country": "China"},
    "LN1": {"Make": "Neta", "Country": "China"},
    "MA2": {"Make": "Mahindra", "Country": "India"},
}

# Build reverse lookup: (Make name) -> list of WMIs
MAKE_TO_WMI = {}
for wmi_code, data in WMI_DB.items():
    make = data["Make"].lower().replace(" ", "-")
    if make not in MAKE_TO_WMI:
        MAKE_TO_WMI[make] = []
    MAKE_TO_WMI[make].append(wmi_code)

def lookup_wmi(wmi):
    """Look up WMI; returns dict or None."""
    return WMI_DB.get(wmi.upper())