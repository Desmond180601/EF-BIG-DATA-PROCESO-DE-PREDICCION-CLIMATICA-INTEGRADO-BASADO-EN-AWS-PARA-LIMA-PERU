
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_timestamp, round
from awsglue.utils import getResolvedOptions

# Capturar los argumentos de entrada
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
JOB_NAME = 'Segmentar-Data-Predicha'

# Rutas S3 definitivas
input_data = "s3://temp-predicha/predicciones_temperatura_completas-prueba3meses.csv"
output_path = "s3://data-predecida-del-sagemaker/"

# Crear un contexto de Spark y Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

# Leer el archivo CSV ignorando encabezado y limitando a los datos de predicciones
dataframe = spark.read.option("header", "false").csv(input_data)

# Asignar nombres de columnas si es necesario para mayor claridad
dataframe = dataframe.withColumnRenamed("_c0", "raw_datetime").withColumnRenamed("_c1", "temperature")

# Convertir la columna raw_datetime a tipo datetime y temperature a tipo double
# raw_datetime debe estar en formato 'yyyy-MM-dd HH:mm:ss', ajusta si el formato es distinto
dataframe = dataframe.withColumn("datetime", to_timestamp(col("raw_datetime"), "yyyy-MM-dd HH:mm:ss")) \
                     .withColumn("temperature", round(col("temperature").cast("double"), 2))

# Filtrar solo filas donde temperature no sea nula
dataframe = dataframe.filter(col("temperature").isNotNull())

# Seleccionar las columnas necesarias
dataframe = dataframe.select("datetime", "temperature")

# Convertir el DataFrame de Spark a un DynamicFrame para Glue
dynamic_frame = DynamicFrame.fromDF(dataframe, glueContext, "dynamic_frame")

# Guardar el DynamicFrame en formato Parquet en el bucket de salida
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet"
)

# Finalizar el Job
job.commit()