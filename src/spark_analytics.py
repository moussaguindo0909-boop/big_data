import glob
import os
import shutil
import sys

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("solaire-tunisie-analytics")
    .master("local[*]")
    .config("spark.driver.memory", "2g")   
    .getOrCreate()
)

clean = spark.read.parquet("data/clean/meteo_horaire")

OUT = "data/analytics"
os.makedirs(OUT, exist_ok=True)


def export_parquet(sdf, name):
    """Spark écrit un dossier de fichiers 'part-...'. Pour de petites tables,
    on force un seul fichier puis on le copie sous un nom propre."""
    tmp = f"{OUT}/_tmp_{name}"
    sdf.coalesce(1).write.mode("overwrite").parquet(tmp)
    part = glob.glob(f"{tmp}/part-*.parquet")[0]
    shutil.copy(part, f"{OUT}/{name}.parquet")
    shutil.rmtree(tmp)
    print(f"{name} : {sdf.count()} lignes")



agg_mensuel = (
    clean.groupBy("gouvernorat", "annee", "mois")
    .agg(
        F.round(F.avg("rayonnement_wm2"), 1).alias("rayonnement_moyen_wm2"),
        F.round(F.sum("rayonnement_wm2") / 1000, 1).alias("energie_kwh_m2"),
        F.round(F.avg("temperature_c"), 1).alias("temperature_moyenne_c"),
        F.round(F.max("temperature_c"), 1).alias("temperature_max_c"),
        F.round(F.avg("nebulosite_pct"), 1).alias("nebulosite_moyenne_pct"),
        F.round(F.avg("vent_kmh"), 1).alias("vent_moyen_kmh"),
    )
    .withColumn("date_mois", F.make_date("annee", "mois", F.lit(1)))
)
export_parquet(agg_mensuel, "agg_mensuel")


profil_horaire = (
    clean.groupBy("gouvernorat", "saison", "heure")
    .agg(
        F.round(F.avg("rayonnement_wm2"), 1).alias("rayonnement_moyen_wm2"),
        F.round(F.avg("temperature_c"), 1).alias("temperature_moyenne_c"),
    )
)
export_parquet(profil_horaire, "profil_horaire")


regions = [
    ("Tunis", "Tunis", "Nord-Est"), ("Ariana", "Ariana", "Nord-Est"),
    ("Ben_Arous", "Ben Arous", "Nord-Est"), ("Manouba", "Manouba", "Nord-Est"),
    ("Nabeul", "Nabeul", "Nord-Est"), ("Zaghouan", "Zaghouan", "Nord-Est"),
    ("Bizerte", "Bizerte", "Nord-Est"),
    ("Beja", "Béja", "Nord-Ouest"), ("Jendouba", "Jendouba", "Nord-Ouest"),
    ("Le_Kef", "Le Kef", "Nord-Ouest"), ("Siliana", "Siliana", "Nord-Ouest"),
    ("Sousse", "Sousse", "Centre-Est"), ("Monastir", "Monastir", "Centre-Est"),
    ("Mahdia", "Mahdia", "Centre-Est"), ("Sfax", "Sfax", "Centre-Est"),
    ("Kairouan", "Kairouan", "Centre-Ouest"), ("Kasserine", "Kasserine", "Centre-Ouest"),
    ("Sidi_Bouzid", "Sidi Bouzid", "Centre-Ouest"),
    ("Gabes", "Gabès", "Sud-Est"), ("Medenine", "Médenine", "Sud-Est"),
    ("Tataouine", "Tataouine", "Sud-Est"),
    ("Gafsa", "Gafsa", "Sud-Ouest"), ("Tozeur", "Tozeur", "Sud-Ouest"),
    ("Kebili", "Kébili", "Sud-Ouest"),
]
dim_regions = spark.createDataFrame(regions, ["gouvernorat", "nom_affichage", "region"])

dim_gouvernorat = (
    clean.select("gouvernorat", "latitude", "longitude").distinct()
    .join(dim_regions, "gouvernorat", "inner")
)
export_parquet(dim_gouvernorat, "dim_gouvernorat")

spark.stop()