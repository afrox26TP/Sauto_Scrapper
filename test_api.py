import urllib.request, json, time
time.sleep(3)
r = urllib.request.urlopen('http://localhost:8000/api/health', timeout=5)
print('HEALTH:', r.read().decode())

r2 = urllib.request.urlopen('http://localhost:8000/api/catalog/equipment?force_refresh=true', timeout=30)
d = json.loads(r2.read())
print(f'EQUIPMENT items: {len(d.get("items",[]))}')

r3 = urllib.request.urlopen('http://localhost:8000/api/catalog/bodies?force_refresh=true', timeout=30)
d2 = json.loads(r3.read())
print(f'BODIES items: {len(d2.get("items",[]))}')