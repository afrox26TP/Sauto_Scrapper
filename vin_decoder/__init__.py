"""
vin_decoder - Offline VIN decoder for EU market vehicles.
No external API calls needed. Pure Python.

Usage:
    from vin_decoder import decode, decode_batch
    
    info = decode("TMBLK7NS5K8057311")
    print(info["make"])      # Skoda
    print(info["model_year"]) # 2019
    print(info["country"])   # Czech Republic

Supports 50+ manufacturers including:
    VW Group (Audi, VW, Skoda, Seat, Porsche, Bentley, Lamborghini),
    BMW Group (BMW, MINI), Mercedes-Benz, Stellantis (Peugeot, Citroen,
    Fiat, Alfa Romeo, Jeep, Opel), Renault-Nissan, Toyota, Honda,
    Hyundai-Kia, Ford, Volvo, JLR, Subaru, Mazda, Suzuki, Tesla, + more.
"""

from .decoder import decode, decode_batch, decode_model_year
from .wmi_db import lookup_wmi, WMI_DB

__version__ = "1.0.0"
__all__ = ["decode", "decode_batch", "decode_model_year", "lookup_wmi", "WMI_DB"]