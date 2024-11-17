
import boto3
import time
import csv

# Inicializar clientes para Athena y S3
athena_client = boto3.client('athena')
s3_client = boto3.client('s3')

def run_athena_query(query, database, output_location):
    """Ejecuta una consulta en Athena y devuelve el ID de ejecución."""
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}
    )
    return response['QueryExecutionId']

def get_query_results(query_execution_id):
    """Verifica el estado de la consulta de Athena y retorna la ubicación del archivo de resultados en S3."""
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        
        if status == 'SUCCEEDED':
            break
        elif status == 'FAILED':
            raise Exception(f"La consulta falló: {response['QueryExecution']['Status']['StateChangeReason']}")
        
        time.sleep(2)  # Espera antes de volver a verificar el estado de la consulta

    # Obtener la ubicación del archivo de resultados
    result_location = response['QueryExecution']['ResultConfiguration']['OutputLocation']
    return result_location

def download_csv_from_s3(s3_path):
    """Descargar el archivo CSV generado por Athena desde S3."""
    bucket_name = s3_path.replace('s3://', '').split('/')[0]
    key = '/'.join(s3_path.replace('s3://', '').split('/')[1:])
    
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    csv_data = response['Body'].read().decode('utf-8')
    
    return csv_data

def lambda_handler(event, context):
    # Definir los parámetros de la consulta
    query = """
    SELECT 
        date,
        time,
        humidity, 
        temperature, 
        windSpeed, 
        ROUND(humidity / 100 * 6.112 * (17.67 * temperature / (temperature + 243.5))) AS vapor_pressure,
        ROUND(13.12 + 0.6215 * temperature - 11.37 * (windSpeed * 0.16) + 0.3965 * temperature * (windSpeed * 0.16), 2) AS wind_chill, 
        ROUND(temperature + 0.33 * humidity - 0.7 - 4.00, 2) AS heat_index, 
        ROUND(temperature - ((100 - humidity) / 5), 2) AS dew_point 
    FROM datosclimaticostransformados.weather_data_calculations
    """
    database = "datosclimaticostransformados"  # Cambia al nombre de tu base de datos en Athena
    output_location = "s3://bucket-de-resultados-athena/"  # Cambia a la ubicación de tu bucket S3 de salida

    try:
        # Ejecutar la consulta en Athena
        query_execution_id = run_athena_query(query, database, output_location)
        
        # Obtener la ubicación del CSV en S3
        result_location = get_query_results(query_execution_id)
        
        # Descargar el CSV desde S3
        csv_data = download_csv_from_s3(result_location)
        
        # Depuración: Imprimir el contenido del CSV
        print(csv_data)

        # Guardar el CSV en otro bucket o retornar el resultado como desees
        s3_client.put_object(Bucket='bucket-destino', Key='nuevo_datos_calculados.csv', Body=csv_data)

        return {
            'statusCode': 200,
            'body': f"Consulta completada exitosamente. Los resultados están en: {result_location}"
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Error al procesar la consulta de Athena: {str(e)}"
        }
