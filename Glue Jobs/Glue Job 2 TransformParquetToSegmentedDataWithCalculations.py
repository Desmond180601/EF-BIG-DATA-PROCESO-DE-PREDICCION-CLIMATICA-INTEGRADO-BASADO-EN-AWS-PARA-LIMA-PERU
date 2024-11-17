
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, split, to_date, substring, udf
from pyspark.sql.types import DoubleType
import math

# Asignar valores de entrada directamente
JOB_NAME = 'TransformParquetToSegmentedDataWithCalculationsRounded'
S3_INPUT_PATH = 's3://bucket-2-building/weather_data_lima/'
S3_OUTPUT_PATH = 's3://bucket-3-segmented-data/segmented_weather_data_lima_with_calculations/'

# Crear un contexto de Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, {})

# Leer el archivo Parquet directamente en un DataFrame
dataframe = spark.read.parquet(S3_INPUT_PATH)

# Transformar 'startTime' en dos nuevas columnas: 'date' y 'time'
dataframe = dataframe.withColumn("date", to_date(split(col("startTime"), "T")[0], "yyyy-MM-dd"))
dataframe = dataframe.withColumn("time", split(split(col("startTime"), "T")[1], "Z")[0])

# Eliminar los segundos de la columna 'time', conservando solo hora y minuto
dataframe = dataframe.withColumn("time", substring(col("time"), 1, 5))

# Eliminar la columna original 'startTime' si ya no es necesaria
dataframe = dataframe.drop("startTime")

# Función para calcular Wind Chill (Sensación térmica por viento) con redondeo
def calcular_sensacion_termica(temp_c, velocidad_viento_kmh):
    return round(13.12 + 0.6215 * temp_c - 11.37 * (velocidad_viento_kmh ** 0.16) + 0.3965 * temp_c * (velocidad_viento_kmh ** 0.16), 2)

# Función para calcular Heat Index (Índice de calor) con redondeo
def calcular_indice_calor(temp_c, humedad_relativa):
    return round(temp_c + 0.33 * humedad_relativa - 0.7 - 4.00, 2)

# Función para calcular Dew Point (Punto de rocío) con redondeo
def calcular_punto_rocio(temp_c, humedad_relativa):
    return round(temp_c - ((100 - humedad_relativa) / 5), 2)

# Función para calcular Vapor Pressure (Presión de vapor) con redondeo
def calcular_presion_vapor(temp_c, humedad_relativa):
    return round(humedad_relativa / 100 * 6.112 * math.exp(17.67 * temp_c / (temp_c + 243.5)), 2)

# Registrar las funciones UDF en Spark
udf_sensacion_termica = udf(calcular_sensacion_termica, DoubleType())
udf_indice_calor = udf(calcular_indice_calor, DoubleType())
udf_punto_rocio = udf(calcular_punto_rocio, DoubleType())
udf_presion_vapor = udf(calcular_presion_vapor, DoubleType())

# Añadir las columnas calculadas al DataFrame
dataframe = dataframe.withColumn("wind_chill", udf_sensacion_termica(col("temperature"), col("windSpeed") * 3.6))  # Convertimos windSpeed de m/s a km/h
dataframe = dataframe.withColumn("heat_index", udf_indice_calor(col("temperature"), col("humidity")))
dataframe = dataframe.withColumn("dew_point", udf_punto_rocio(col("temperature"), col("humidity")))
dataframe = dataframe.withColumn("vapor_pressure", udf_presion_vapor(col("temperature"), col("humidity")))

# Crear un DynamicFrame a partir del DataFrame de Spark
dynamic_frame = DynamicFrame.fromDF(dataframe, glueContext, "dynamic_frame")

# Guardar el DynamicFrame en formato Parquet en el bucket de salida
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={"path": S3_OUTPUT_PATH},
    format="parquet"
)

# Finalizar el Job
job.commit()