# Import de Librerias usadas:
#import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from airflow.models import DAG, Variable
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

from datetime import datetime  # Importo datetime antes de su uso en pandas
import pandas as pd
from googleapiclient.discovery import build


# Definición de Variables:

# Variables de Conexión a Redshift:
url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
data_base="data-engineer-database"
user=Variable.get("user_redshift")                                 #esta variable fue creada en la interfaz de Airflow por cuestiones de Seguridad
pwd= Variable.get("secret_pass_redshift")                          #esta variable fue creada en la interfaz de Airflow por cuestiones de Seguridad
#user='christian_r_coderhouse'                                     #dejo escrito el usuario aca de backup
#pwd='3b4LjN1alG'                                                  #dejo escrito la password aca de backup

# Variable de Conexión a la API de Youtube:
client_API_KEY = Variable.get("client_API_KEY")                    #esta variable fue creada en la interfaz de Airflow por cuestiones de Seguridad
#client_API_KEY= 'AIzaSyBXPyx2L67WhXATIaaR8yl3FJZLsXvpDIE'         #dejo escrita la client_API_KEY de Youtube aca de backup


# Definición de las Tasks de mi Proceso:


# Task 1: "get_top_videos"
def get_top_videos():
    
    # Definir tu clave de API de YouTube
    API_KEY = client_API_KEY

    # Crear una instancia del servicio de la API de YouTube
    youtube = build('youtube', 'v3', developerKey=API_KEY)

    # Función para obtener el nombre de la categoría a partir del ID
    def get_category_name(youtube, category_id):
        categories_response = youtube.videoCategories().list(
            part='snippet',
            id=category_id
        ).execute()
        if 'items' in categories_response:
            return categories_response['items'][0]['snippet']['title']
        else:
            return 'Desconocida'

    # Función para convertir la duración en formato "PT11M13S" a segundos
    def convert_duration_to_seconds(duration):
        parts = duration[2:].split('T')[-1].split('H')
        hours = int(parts[0]) if len(parts) > 1 else 0
        minutes_parts = parts[-1].split('M')
        minutes = int(minutes_parts[0]) if len(minutes_parts) > 1 else 0
        seconds_parts = minutes_parts[-1].split('S')
        seconds = int(seconds_parts[0]) if len(seconds_parts) > 1 else 0
        total_seconds = hours * 3600 + minutes * 60 + seconds
        return total_seconds


    # Realizar la búsqueda de videos ordenados por vistas y limitar a 10 resultados
    search_response = youtube.search().list(
        part='id',
        maxResults=10,
        order='viewCount',
        type='video'
    ).execute()

    # Extraer los IDs de los videos obtenidos en la búsqueda
    video_ids = [item['id']['videoId'] for item in search_response['items']]

    # Obtener detalles de los videos
    videos_response = youtube.videos().list(
        part='snippet,statistics,contentDetails',
        id=','.join(video_ids)
    ).execute()

    # Crear una lista de diccionarios con la información de los videos
    video_data = []
    for video in videos_response['items']:
        video_id = video['id']
        video_info = {
            "ID_del_Video": video_id,
            "Título": video['snippet']['title'],
            "Descripción": video['snippet']['description'],
            "Canal_Propietario": video['snippet']['channelTitle'],
            "Fecha_de_Publicación": video['snippet']['publishedAt'],
            "Categoría_ID": video['snippet']['categoryId'],
            "Categoría": get_category_name(youtube, video['snippet']['categoryId']),
            "Duración_segundos": convert_duration_to_seconds(video['contentDetails']['duration']),
            "URL_del_Video": f"https://www.youtube.com/watch?v={video_id}",      
            "Vistas": video['statistics']['viewCount'],
            "Likes": video['statistics'].get('likeCount', 0),
            "Dislikes": video['statistics'].get('dislikeCount', 0),
            "Favorite_Count": video['statistics'].get('favoriteCount', 0),
            "Comment_Count": video['statistics'].get('commentCount', 0),
            #"Insert_Date": current_date
        }
        video_data.append(video_info)

    # Crear un DataFrame a partir de la lista de diccionarios
    df = pd.DataFrame(video_data)

    #Hago las siguientes transformaciones a las columnas del Dataframe df:

    # Recortar la columna "Descripción" y "Título" a 301 caracteres:
    df['Descripción'] = df['Descripción'].str[:301]
    df['Título'] = df['Título'].str[:301]
    
    
    # Convertir la columna "Fecha de Publicación" en objeto datetime y creo la columna "Insert Date" en objeto datetime:
    df['Fecha_de_Publicación'] = pd.to_datetime(df['Fecha_de_Publicación'])
    #df['Insert_Date'] = pd.to_datetime(df['Insert_Date'])
    df['Insert_Date'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%dT00:00:00Z'))
    
    # Formatear la columna "Fecha de Publicación" y "Insert Date" en el formato deseado:
    df['Fecha_de_Publicación'] = df['Fecha_de_Publicación'].dt.strftime('%Y-%m-%d')
    df['Insert_Date'] = df['Insert_Date'].dt.strftime('%Y-%m-%d')

    df=df.to_dict()
    return(df)




# Task 2: "conectar_Redshift"
def conectar_Redshift():
    import psycopg2
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    data_base="data-engineer-database"
    user="christian_r_coderhouse"

    try:
        conn = psycopg2.connect(
            host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            dbname=data_base,
            user=user,
            password= pwd,
            port='5439'
        )
        print("Conectado a Redshift con éxito!")

    except Exception as e:
        print("No es posible conectar a Redshift")
        print(e)
    
    #Crear la tabla si no existe:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS videos
            (
            Id_del_Video VARCHAR(50) primary key
            ,Título VARCHAR(350)
            ,Descripción VARCHAR(350)   
            ,Canal_Propietario VARCHAR(255)
            ,Fecha_de_Publicación date
            ,Categoría_ID VARCHAR(50)
            ,Categoría VARCHAR(100)
            ,Duración_segundos INTEGER
            ,URL_del_Video NVARCHAR(500)
            ,Vistas INTEGER
            ,Likes INTEGER
            ,Dislikes INTEGER
            ,Favorite_Count INTEGER
            ,Comment_Count INTEGER
            ,Insert_Date date

            )
        """)
        conn.commit()





# Task 3: "insert_data"
def insert_data():
    
    import psycopg2
    
    
    conn = psycopg2.connect(
            host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            dbname=data_base,
            user=user,
            password= pwd,
            port='5439'
        )
    
    
    
    data_dict = get_top_videos()
    df = pd.DataFrame(data_dict)
    #data = [(row['ID_del_Video'], row['Título'], row['Descripción'], row['Canal_Propietario'], row['Fecha_de_Publicación'], row['Categoría_ID'], row['Categoría'], row['Duración_segundos'], row['URL_del_Video'], row['Vistas'],  row['Likes'], row['Dislikes'], row['Favorite_Count'], row['Comment_Count'], row['Insert_Date'] ) for _, row in df.iterrows()]
    print(df)
    
    from psycopg2.extras import execute_values  # Añado esta línea para importar execute_values
    with conn.cursor() as cur:
        try:
            execute_values(
                cur,
                '''
                    INSERT INTO videos (ID_del_Video, Título, Descripción, Canal_Propietario, Fecha_de_Publicación, Categoría_ID, Categoría, Duración_segundos, URL_del_Video, Vistas, Likes, Dislikes, Favorite_Count, Comment_Count, Insert_Date)
                    VALUES %s
                    ''',
                    [tuple(row) for row in df.values],
                    #data,
                    page_size=len(df)
                )
            conn.commit()
            conn.close()
        except Exception as e:
            print("No es posible insertar datos")
            print(e)



# Task 4: 
Pass_Email= Variable.get("secret_pass_gmail")
smtp_server = 'smtp.gmail.com'
smtp_port = 587
sender_email = 'christian.jrivasn@gmail.com'
password = Pass_Email


def send_email():
        try:
            subject = 'Carga de datos de 10 videos mas vistos en Youtube a Redshift'
            body_text = 'Los datos de los 10 videos mas vistos en Youtube fueron cargados a la base de datos de Redshift exitosamente.'

            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['To'] = sender_email
            msg['Subject'] = subject
            msg.attach(MIMEText(body_text, 'plain'))
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender_email, password)
                server.send_message(msg)
            print('El email fue enviado correctamente.')

        except Exception as exception:
            print(exception)
            print('El email no se pudo enviar.')