import os
import sys

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("solaire-tunisie-nettoyage")
    .master("local[*]")
    .getOrCreate()
)

raw = spark.read.parquet("data/raw/open_meteo")

# 1. Noms de colonnes clairs, avec l'unité quand c'est utile
df = (
    raw
    .withColumnRenamed("time", "horodatage")
    .withColumnRenamed("shortwave_radiation", "rayonnement_wm2")
    .withColumnRenamed("temperature_2m", "temperature_c")
    .withColumnRenamed("cloud_cover", "nebulosite_pct")
    .withColumnRenamed("wind_speed_10m", "vent_kmh")
)

# 2. Colonnes calculées
df = (
    df
    .withColumn("annee", F.year("horodatage"))
    .withColumn("mois", F.month("horodatage"))
    .withColumn("heure", F.hour("horodatage"))
    .withColumn(
        "saison",
        F.when(F.col("mois").isin(12, 1, 2), "Hiver")
         .when(F.col("mois").isin(3, 4, 5), "Printemps")
         .when(F.col("mois").isin(6, 7, 8), "Ete")
         .otherwise("Automne"),
    )
    .withColumn("est_jour", F.col("rayonnement_wm2") > 0)
)

# 3. Règles de validité : on écarte les valeurs physiquement impossibles
valide = (
    F.col("rayonnement_wm2").between(0, 1400)
    & F.col("temperature_c").between(-30, 60)
    & F.col("nebulosite_pct").between(0, 100)
    & F.col("vent_kmh").between(0, 200)
)
clean = df.filter(valide)

n_raw = raw.count()
n_clean = clean.count()
print(f"Lignes brutes   : {n_raw}")
print(f"Lignes valides  : {n_clean}")
print(f"Lignes écartées : {n_raw - n_clean}")

# 4. Écriture en Parquet, un dossier par année
(
    clean.write
    .mode("overwrite")
    .partitionBy("annee")
    .parquet("data/clean/meteo_horaire")
)

# 5. Relecture pour vérifier
check = spark.read.parquet("data/clean/meteo_horaire")
check.printSchema()
print("Lignes relues :", check.count())
check.show(5)

spark.stop()