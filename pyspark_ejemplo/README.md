
# Proyecto ETL con PySpark

Este proyecto implementa un flujo **ETL (Extract, Transform, Load)** utilizando **PySpark**, con el objetivo de procesar un archivo CSV de ventas y generar reportes agregados.

---

##  Requisitos previos

Antes de ejecutar el proyecto, asegúrate de tener instalado lo siguiente:

- **Java** (versión 8, 11 o 17)  
- **Python 3.8+**  
- **Apache Spark**  
- **PySpark**  
  ```bash
  pip install pyspark
  ```

---

## 🚀 Ejecución

Puedes ejecutar el script de dos maneras:

### 1. Con Python directamente
```bash
python etl_ventas.py
```

### 2. Con Spark Submit
```bash
spark-submit --master local[*] etl_ventas.py
```

---

##  Ejemplo de datos (`ventas.csv`)

El archivo de entrada debe estar en la raíz del proyecto con el nombre **ventas.csv**:

```csv
id,fecha,cliente,categoria,monto
1,2025-08-01,Ana,Granos,120000
2,2025-08-02,Carlos,Accesorios,45000
3,2025-08-02,Ana,Granos,80000
4,2025-08-03,María,Procesados,150000
5,2025-08-04,Carlos,Granos,30000
```

---

## Flujo ETL implementado

### 1. **Extract**
Se leen los datos desde un archivo CSV usando Spark:
```python
df = spark.read.csv("ventas.csv", header=True, inferSchema=True)
```

### 2. **Transform**
- Conversión de columnas:
  - `monto` → tipo `double`  
  - `fecha` → tipo `date`  
- Agregaciones:
  - Ventas totales por categoría  
  - Ticket promedio por cliente  

```python
df2 = df.withColumn("monto", col("monto").cast("double"))         .withColumn("fecha", to_date(col("fecha"), "yyyy-MM-dd"))

ventas_por_cat = df2.groupBy("categoria").agg(_sum("monto").alias("ventas_totales"))
ticket_prom = df2.groupBy("cliente").agg(avg("monto").alias("ticket_promedio"))
```

### 3. **Load**
Los resultados se guardan en la carpeta `out/` en formato CSV:

```python
ventas_por_cat.coalesce(1).write.mode("overwrite").option("header", True).csv("out/ventas_por_categoria")
ticket_prom.coalesce(1).write.mode("overwrite").option("header", True).csv("out/ticket_promedio")
```

---

##  Resultados esperados

### Ventas totales por categoría
| categoria   | ventas_totales |
|-------------|----------------|
| Granos      | 230000         |
| Accesorios  | 45000          |
| Procesados  | 150000         |

### Ticket promedio por cliente
| cliente | ticket_promedio |
|---------|-----------------|
| Ana     | 100000          |
| Carlos  | 37500           |
| María   | 150000          |

---

## Diagrama del flujo ETL

```mermaid
flowchart LR
    A[CSV Ventas] --> B[Extract: Lectura con Spark]
    B --> C[Transform: Limpieza y Conversión]
    C --> D[Transform: Agregaciones]
    D --> E[Load: Guardar en CSV]
    E --> F[Resultados Finales]
```

---

##  Conclusión

Este proyecto muestra cómo usar **PySpark** para:
- Cargar datos desde CSV
- Transformarlos aplicando conversiones y agregaciones
- Guardar resultados procesados en formato CSV  

Es una base que puede extenderse para proyectos más complejos de **Big Data y ETL**.
