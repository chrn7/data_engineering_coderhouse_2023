#!/usr/bin/env python
# coding: utf-8

# # ENTREGABLE 1 - EXTRAER DATOS DE API DE YOUTUBE E INSERTAR LA DATA EN UNA TABLA DE AWS REDSHIFT.

# #### Explicación del Proyecto:
# 
# El script siguiente obtiene los 10 videos más populares (con más vistas) de YouTube en el momento de la ejecución del script. La API realiza una búsqueda de videos, ordenándolos por la cantidad de vistas (ordenados en orden descendente). Luego, obtiene los detalles de esos videos y crea un DataFrame con la información relevante, incluida la cantidad de vistas, y lo imprime.
# 
# Además, en la tabla resultante de la API le incluyo la columna "Insert_Date", que contiene el día de ejecución del script, para identificar en qué día se obtuvieron los 10 registros con los 10 videos más vistos.
# 
# Por último, esos 10 registros son insertados en la tabla "videos" dentro de mi Base de Datos de Redshift, haciendo previamente la conexión a dicha Base de Datos (y creando la tabla en primera instancia).
# 

# In[1]:


# Guardo mi contraseña "api_key" de la API de Youtuber en un archivo .txt por cuestiones de seguridad y que no aparezca visible mi contraseña en el código:
with open("C:/Users/cnieto1/Desktop/Curso Data Engineering - Coderhouse/Clases/Entregable 1/api_key_youtube.txt",'r') as f:
    pwd= f.read()


# In[2]:


import pandas as pd
from googleapiclient.discovery import build
import datetime

# Definir tu clave de API de YouTube
API_KEY = pwd

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

# Obtener la fecha actual en el formato requerido por la API de YouTube
current_date = datetime.datetime.now().strftime('%Y-%m-%dT00:00:00Z')

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
        "Insert_Date": current_date
    }
    video_data.append(video_info)

# Crear un DataFrame a partir de la lista de diccionarios
df = pd.DataFrame(video_data)

# Mostrar el DataFrame
print(df)


# In[3]:


df.head()


# In[4]:


df


# In[5]:


#Obtenemos un Dataframe de 10 registros y 15 columnas:
df.shape


# In[6]:


#Hago las siguientes transformaciones a las columnas del Dataframe df:

# Recortar la columna "Descripción" y "Título" a 301 caracteres:
df['Descripción'] = df['Descripción'].str[:301]
df['Título'] = df['Título'].str[:301]

import pandas as pd
from datetime import datetime

# Convertir la columna "Fecha de Publicación" y "Insert Date" a objetos datetime:
df['Fecha_de_Publicación'] = pd.to_datetime(df['Fecha_de_Publicación'])
df['Insert_Date'] = pd.to_datetime(df['Insert_Date'])

# Formatear la columna "Fecha de Publicación" y "Insert Date" en el formato deseado:
df['Fecha_de_Publicación'] = df['Fecha_de_Publicación'].dt.strftime('%Y-%m-%d')
df['Insert_Date'] = df['Insert_Date'].dt.strftime('%Y-%m-%d')

df.head()


# In[7]:


# Guardo mi contraseña "pwd_redshift" de mi cuenta de Redshift en un archivo .txt por cuestiones de seguridad y que no aparezca visible mi contraseña en el código:
with open("C:/Users/cnieto1/Desktop/Curso Data Engineering - Coderhouse/Clases/Entregable 1/redshift_password.txt",'r') as f:
    pwd_redshift= f.read()


# In[8]:


# Creando la conexión a Redshift:
import psycopg2
url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
data_base="data-engineer-database"
user="christian_r_coderhouse"

try:
    conn = psycopg2.connect(
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        dbname=data_base,
        user=user,
        password= pwd_redshift,
        port='5439'
    )
    print("Conectado a Redshift con éxito!")
    
except Exception as e:
    print("No es posible conectar a Redshift")
    print(e)


# In[9]:


# Código para hacer Drop Table de la tabla "canciones". USARLO SOLO en caso de que la tabla ya exista y tenga que hacerle modificaciones:
# RECORDAR: Antes de correr este código, correr primero el código anterior (Creando la conexión a Redshift):

# Crear un cursor:
#cur = conn.cursor()

# Ejecutar la sentencia DROP TABLE:
#cur.execute("DROP TABLE IF EXISTS videos")

# Hacer commit para aplicar los cambios:
#conn.commit()


# In[10]:


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


# In[11]:


# Comento este paso, para que cada vez que corra el script de la API, que me vaya insertando los registros de cada día y se vayan acumulando:
#Vaciar la tabla para evitar duplicados o inconsistencias:
#with conn.cursor() as cur:
#  cur.execute("Truncate table canciones")
#  count = cur.rowcount
# count


# In[12]:


#consultando la tabla canciones:
cur = conn.cursor()
cur.execute("SELECT * FROM videos")
results = cur.fetchall()
#results


# In[13]:


#Insertando los datos en Redsfhift:
from psycopg2.extras import execute_values
with conn.cursor() as cur:
    execute_values(
        cur,
        '''
        INSERT INTO videos (ID_del_Video, Título, Descripción, Canal_Propietario, Fecha_de_Publicación, Categoría_ID, Categoría, Duración_segundos, URL_del_Video, Vistas, Likes, Dislikes, Favorite_Count, Comment_Count, Insert_Date)
        VALUES %s
        ''',
        [tuple(row) for row in df.values],
        page_size=len(df)
    )
    conn.commit()


# In[14]:


# Veo cómo quedó la tabla en Redshift luego de hacer los Insert:
#consultando la tabla
cur = conn.cursor()
cur.execute("SELECT * FROM videos")
results = cur.fetchall()


# In[15]:


# Veo cómo quedó la tabla "canciones" en Redshift. Convierto "results" al DataFrame "df_redshift":
column_names=['Video_ID', 'Title', 'Description', 'Channel', 'Publication_Date', 'Category_ID', 'Category','Duration_seconds','Video_URL', 'Views_Count','Likes', 'Dislikes','Favorite_Count','Comment_Count','Insert_date']
df_redshift= pd.DataFrame(results, columns=column_names)
df_redshift.head()


# In[16]:


# Cierro tanto el cursor como la conexión a la base de datos:
cur.close()
conn.close()

