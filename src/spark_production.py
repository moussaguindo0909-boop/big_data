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

# --- Hypothèses du modèle (à savoir justifier à l'oral) ---
NOCT = 45.0      # température nominale de fonctionnement de la cellule (°C)
GAMMA = -0.004   # perte de puissance par °C au-dessus de 25 °C (silicium cristallin)

spark = (
    SparkSession.builder
    .appName("solaire-tunisie-production")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)

clean = spark.read.parquet("data/clean/meteo_horaire")

prod = (
    clean
    .withColumn(
        "temp_cellule_c",
        F.col("temperature_c") + (NOCT - 20.0) / 800.0 * F.col("rayonnement_wm2"),
    )
    .withColumn("facteur_temperature", 1.0 + GAMMA * (F.col("temp_cellule_c") - 25.0))
    .withColumn("prod_sans_chaleur", F.col("rayonnement_wm2") / 1000.0)   # kWh par kWc, par heure
    .withColumn("prod_brute", F.col("prod_sans_chaleur") * F.col("facteur_temperature"))
)

agg_production = (
    prod.groupBy("gouvernorat", "annee", "mois")
    .agg(
        F.round(F.sum("prod_sans_chaleur"), 2).alias("prod_sans_chaleur_kwh_kwc"),
        F.round(F.sum("prod_brute"), 2).alias("prod_brute_kwh_kwc"),
        F.round(F.max("temp_cellule_c"), 1).alias("temp_cellule_max_c"),
    )
    .withColumn("date_mois", F.make_date("annee", "mois", F.lit(1)))
)

# --- Contrôle : production annuelle et perte due à la chaleur, par gouvernorat ---
resume = (
    agg_production.groupBy("gouvernorat")
    .agg(
        (F.sum("prod_brute_kwh_kwc") / F.countDistinct("annee")).alias("brut_annuel"),
        (F.sum("prod_sans_chaleur_kwh_kwc") / F.countDistinct("annee")).alias("ideal_annuel"),
    )
    .withColumn("perte_chaleur_pct", F.round((1 - F.col("brut_annuel") / F.col("ideal_annuel")) * 100, 1))
    .withColumn("productible_kwh_kwc", F.round(F.col("brut_annuel") * 0.86, 0))  # avec 86 % de rendement système
    .select("gouvernorat", "perte_chaleur_pct", "productible_kwh_kwc")
    .orderBy(F.desc("productible_kwh_kwc"))
)
resume.show(24, truncate=False)

# --- Export Parquet (un seul fichier) ---
OUT = "data/analytics"
os.makedirs(OUT, exist_ok=True)
tmp = f"{OUT}/_tmp_agg_production"
agg_production.coalesce(1).write.mode("overwrite").parquet(tmp)
shutil.copy(glob.glob(f"{tmp}/part-*.parquet")[0], f"{OUT}/agg_production.parquet")
shutil.rmtree(tmp)
print("agg_production :", agg_production.count(), "lignes")

spark.stop()