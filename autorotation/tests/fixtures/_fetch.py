"""One-off helper used to generate C2132.json fixture. Not part of the test suite."""
import json
import os
import urllib.request

url = "https://easyeda.com/api/products/C2132/components"
req = urllib.request.Request(url, headers={
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://easyeda.com/",
})
with urllib.request.urlopen(req, timeout=15) as resp:
    data = resp.read()

out = os.path.join(os.path.dirname(__file__), "C2132.json")
with open(out, "wb") as f:
    f.write(data)

d = json.loads(data)
print("top keys:", list(d.keys()))
print("success:", d.get("success"))
r = d.get("result", {})
print("result keys:", list(r.keys()))
pd = r.get("packageDetail", {})
print("packageDetail keys:", list(pd.keys()))
print("packageDetail.title:", pd.get("title"))
print("packageDetail.uuid:", pd.get("uuid"))
ds = pd.get("dataStr", {}) if isinstance(pd, dict) else {}
print("dataStr keys:", list(ds.keys()) if isinstance(ds, dict) else type(ds))
head = ds.get("head") if isinstance(ds, dict) else None
print("head:", head)
shape = ds.get("shape") if isinstance(ds, dict) else None
print("shape count:", len(shape) if shape else 0)
if shape:
    for s in shape:
        if s.startswith("PAD"):
            print("PAD:", s)
