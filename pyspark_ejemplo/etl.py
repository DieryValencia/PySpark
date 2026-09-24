import os
import shutil
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, avg

# Windows necesita HADOOP_HOME (winutils.exe) para escribir archivos
os.environ.setdefault("HADOOP_HOME", os.path.join(os.path.dirname(os.path.abspath(__file__)), ".hadoop"))
os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ["PATH"]

# 1. Crear sesión de Spark
spark = SparkSession.builder \
    .appName("ETL_Ventas") \
    .master("local[*]") \
    .getOrCreate()

# 2. Extraer datos (leer CSV)
df = spark.read.csv("ventas.csv", header=True, inferSchema=True)
print("Datos originales:")
df.show()

# 3. Transformar datos
df2 = df.withColumn("monto", col("monto").cast("double")) \
        .withColumn("fecha", to_date(col("fecha"), "yyyy-MM-dd"))

ventas_por_cat = df2.groupBy("categoria").agg(_sum("monto").alias("ventas_totales"))
ticket_prom = df2.groupBy("cliente").agg(avg("monto").alias("ticket_promedio"))

print("Ventas por categoría:")
ventas_por_cat.show()

print("Ticket promedio por cliente:")
ticket_prom.show()

# 4. Cargar resultados (guardar en un único archivo CSV)
def guardar_csv(df, ruta):
    """Escribe el DataFrame como un solo archivo .csv (sin parteos ni _SUCCESS)."""
    carpeta_tmp = ruta + "_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(carpeta_tmp)

    parteo = glob.glob(os.path.join(carpeta_tmp, "part-*.csv"))[0]
    if os.path.exists(ruta):
        os.remove(ruta)
    os.makedirs(os.path.dirname(ruta) or ".", exist_ok=True)
    shutil.move(parteo, ruta)
    shutil.rmtree(carpeta_tmp, ignore_errors=True)

guardar_csv(ventas_por_cat, "out/ventas_por_categoria.csv")
guardar_csv(ticket_prom, "out/ticket_promedio.csv")

# 5. Finalizar
spark.stop()
