
import json
import boto3
import urllib.request
 
def lambda_handler(event, context):
    # Clave API de Tomorrow.io y coordenadas de Lima, Perú
    api_key = 'HNw9xAcFzZrvK5n8MnC4atFoepCFCEk3'  # Esta es tu clave API
    lat = '-12.0464'
    lon = '-77.0428'
    # Parámetros para obtener datos actuales con Tomorrow.io
    url = f'https://api.tomorrow.io/v4/timelines?location={lat},{lon}&fields=temperature,humidity,windSpeed&timesteps=1h&units=metric&apikey={api_key}'
    try:
        # Solicitar datos de la API
        response = urllib.request.urlopen(url)
        data = response.read()
        weather_data = json.loads(data)
        # Inicializar cliente S3
        s3 = boto3.client('s3')
        bucket_name = 'bucket-1-landing'  # Reemplaza con el nombre de tu bucket de S3
        object_name = 'weather_data_lima_tomorrow_io.json'
        # Guardar datos en S3
        s3.put_object(Bucket=bucket_name, Key=object_name, Body=json.dumps(weather_data))
        return {
            'statusCode': 200,
            'body': json.dumps(f'Datos climáticos de Lima guardados exitosamente en S3.')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error al obtener o guardar datos climáticos: {str(e)}')
        }