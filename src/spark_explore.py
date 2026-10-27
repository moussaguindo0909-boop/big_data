import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Spark doit utiliser le Python de notre environnement virtuel
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = (
    SparkSession.builder
    .appName("solaire-tunisie")
    .master("local[*]")      # utilise tous les cœurs de la machine
    .getOrCreate()
)

# Lecture de tout le dossier : Spark le traite comme une seule table
df = spark.read.parquet("data/raw/open_meteo")

df.printSchema()
print("Nombre de lignes :", df.count())

# Même calcul qu'avec pandas : rayonnement moyen par gouvernorat
result = (
    df.groupBy("gouvernorat")
      .agg(F.round(F.avg("shortwave_radiation"), 1).alias("rayonnement_moyen"))
      .orderBy(F.desc("rayonnement_moyen"))
)
result.show(24, truncate=False)
result.explain()

spark.stop()