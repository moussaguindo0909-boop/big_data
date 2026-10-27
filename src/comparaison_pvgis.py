"""
Compare le productible du projet (modèle NOCT + Open-Meteo) avec PVGIS.

Usage (depuis la racine du projet, environnement actif) :
    pip install requests pandas
    python comparaison_pvgis.py

Résultat : pvgis_comparaison.csv + un résumé affiché dans le terminal.

Réglages PVGIS, choisis pour être comparables à ton modèle :
  - 1 kWc, silicium cristallin
  - pertes système = 14 %  (équivalent de ton rendement de 86 %)
  - inclinaison 0° (plan horizontal), comme ton rayonnement Open-Meteo
PVGIS calcule lui-même l'effet de la température, ce qui permet de tester ton coefficient.
"""
import time
import requests
import pandas as pd

# gouvernorat, latitude, longitude, ton rayonnement (kWh/m²/an), ton productible (kWh/kWc/an)
DATA = [
    ("Tataouine", 32.9297, 10.4518, 2000, 1574),
    ("Tozeur", 33.9197, 8.1335, 1992, 1557),
    ("Kébili", 33.7050, 8.9690, 1982, 1551),
    ("Médenine", 33.3549, 10.5055, 1968, 1548),
    ("Gabès", 33.8815, 10.0982, 1957, 1548),
    ("Gafsa", 34.4250, 8.7842, 1963, 1547),
    ("Sfax", 34.7406, 10.7603, 1922, 1527),
    ("Kasserine", 35.1676, 8.8365, 1881, 1495),
    ("Sidi Bouzid", 35.0382, 9.4849, 1889, 1492),
    ("Sousse", 35.8256, 10.6369, 1861, 1477),
    ("Mahdia", 35.5047, 11.0622, 1853, 1475),
    ("Monastir", 35.7643, 10.8113, 1842, 1469),
    ("Kairouan", 35.6781, 10.0963, 1836, 1445),
    ("Nabeul", 36.4513, 10.7357, 1800, 1444),
    ("Le Kef", 36.1826, 8.7148, 1794, 1440),
    ("Siliana", 36.0850, 9.3708, 1808, 1438),
    ("Bizerte", 37.2744, 9.8739, 1787, 1436),
    ("Ariana", 36.8625, 10.1956, 1792, 1431),
    ("Ben Arous", 36.7533, 10.2282, 1791, 1427),
    ("Zaghouan", 36.4029, 10.1429, 1791, 1424),
    ("Béja", 36.7256, 9.1817, 1774, 1414),
    ("Tunis", 36.8065, 10.1815, 1774, 1413),
    ("Manouba", 36.8101, 10.0863, 1778, 1413),
    ("Jendouba", 36.5011, 8.7803, 1769, 1400),
]

URLS = [
    "https://re.jrc.ec.europa.eu/api/v5_3/PVcalc",
    "https://re.jrc.ec.europa.eu/api/v5_2/PVcalc",
]


def pvgis(lat, lon):
    params = dict(
        lat=lat, lon=lon, peakpower=1, loss=14, pvtechchoice="crystSi",
        mountingplace="free", angle=0, aspect=0, usehorizon=1, outputformat="json",
    )
    last_err = None
    for url in URLS:
        for _ in range(3):
            try:
                r = requests.get(url, params=params, timeout=60)
                r.raise_for_status()
                tot = r.json()["outputs"]["totals"]["fixed"]
                return tot["H(i)_y"], tot["E_y"]
            except Exception as e:  # on réessaie, puis on passe à l'URL suivante
                last_err = e
                time.sleep(2)
    raise RuntimeError(f"PVGIS indisponible pour ({lat}, {lon}) : {last_err}")


rows = []
for name, lat, lon, my_irr, my_yield in DATA:
    h, e = pvgis(lat, lon)
    rows.append(dict(
        gouvernorat=name,
        mon_irradiation=my_irr, pvgis_irradiation=round(h),
        ecart_irradiation_pct=round((my_irr / h - 1) * 100, 1),
        mon_productible=my_yield, pvgis_productible=round(e),
        ecart_productible_pct=round((my_yield / e - 1) * 100, 1),
    ))
    print(f"{name:12s} irradiation {my_irr} vs {h:.0f} | productible {my_yield} vs {e:.0f}")
    time.sleep(0.5)  # PVGIS limite le nombre d'appels par seconde

df = pd.DataFrame(rows)
df.to_csv("pvgis_comparaison.csv", index=False, encoding="utf-8-sig")

rho_irr = df["mon_irradiation"].corr(df["pvgis_irradiation"], method="spearman")
rho_y = df["mon_productible"].corr(df["pvgis_productible"], method="spearman")
print("\n=== RÉSUMÉ (24 gouvernorats) ===")
print(f"Irradiation : écart moyen {df.ecart_irradiation_pct.mean():+.1f} %, "
      f"écart absolu moyen {df.ecart_irradiation_pct.abs().mean():.1f} %, Spearman {rho_irr:.2f}")
print(f"Productible : écart moyen {df.ecart_productible_pct.mean():+.1f} %, "
      f"écart absolu moyen {df.ecart_productible_pct.abs().mean():.1f} %, Spearman {rho_y:.2f}")
print("Fichier écrit : pvgis_comparaison.csv")
