
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Asignar valores de entrada directamente
JOB_NAME = 'TransformJSONtoParquet'
S3_INPUT_PATH = 's3://bucket-1-landing/weather_data_lima_tomorrow_io.json'
S3_OUTPUT_PATH = 's3://building-2-building/weather_data_lima/'

# Crear un contexto de Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, {})

# Leer el archivo JSON directamente en un DataFrame
input_path = S3_INPUT_PATH  # El path del archivo JSON en S3
output_path = S3_OUTPUT_PATH  # El path de salida en S3

# Cargar el archivo JSON como DataFrame
dataframe = spark.read.json(input_path)

# Estructurar los datos
structured_data = []

# Extraer y transformar los datos de interés
timelines = dataframe.select("data.timelines").collect()
for timeline in timelines:
    for entry in timeline["timelines"]:
        for interval in entry["intervals"]:
            structured_data.append({
                "startTime": interval["startTime"],
                "humidity": interval["values"]["humidity"],
                "temperature": interval["values"]["temperature"],
                "windSpeed": interval["values"]["windSpeed"]
            })

# Crear un DataFrame de Spark con los datos estructurados
final_df = spark.createDataFrame(structured_data)

# Convertir el DataFrame de Spark a un DynamicFrame
dynamic_frame = DynamicFrame.fromDF(final_df, glueContext, "dynamic_frame")

# Guardar el DynamicFrame en formato Parquet en el bucket 'building-an'
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet"
)

# Finalizar el Job
job.commit()