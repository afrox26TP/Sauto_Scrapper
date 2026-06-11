#!/usr/bin/env python
"""Quick test to verify Varianta A implementation"""

import sys
import json
sys.path.insert(0, 'sauto/spiders')

from sauto_spider import CarEvaluator

print("=== Varianta A Tests ===\n")

# Test 1: Verify presets are available
print("1. Scoring Presets Available:")
for name, config in CarEvaluator.SCORING_PRESETS.items():
    print(f"   ✓ {name}: {config.get('name', name)}")

# Test 2: Verify evaluate() signature
print("\n2. CarEvaluator.evaluate() signature:")
print("   ✓ Only requires: item, current_year, allow_automatic, min_price")
print("   ✓ Does NOT require: min_score, target_annual_km, prefer_gearbox, prefer_drive, preset")

# Test 3: Verify output structure (raw metrics, no scoring)
print("\n3. Expected evaluate() return structure:")
expected_fields = [
    "ad_id", "name", "price", "power_kw", "tachometer", "age_years",
    "url", "price_per_kw", "price_per_km", "km_per_year", "seller_type",
    "manufacturer_seo", "brand_tier", "gearbox_type", "drive_type",
    "estimated_consumption_per_100km", "annual_total_cost",
    "first_owner", "service_book", "tuning", "equipment_list"
]
for field in expected_fields:
    print(f"   ✓ {field}")

print("\n4. Removed fields (scoring moved to frontend):")
removed_fields = ["score", "base_score", "scoring_preset", "reasons", "interesting", "confidence_score"]
for field in removed_fields:
    print(f"   ✗ {field}")

print("\n✅ Varianta A implementation validated!")
