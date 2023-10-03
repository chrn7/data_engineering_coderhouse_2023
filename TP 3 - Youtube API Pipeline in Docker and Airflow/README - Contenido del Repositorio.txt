README - Contenido del Repositorio:

El siguiente repositorio incluye los siguientes archivos:
- Youtube_ETL.py : archivo .py incluyendo el script de Python con las 4 funciones del proceso (extracción de datos de la API de Youtube, Conexión a Redshift, Insert de los datos extraídos en la tabla de Redshift y envío de email para confirmar que el proceso fue completado).
- DAG_Entregable.py: archivo creando y seteando los parametros del DAG, y asociando las 4 Tasks del proceso a las 4 funciones del Script "Youtube_ETL.py" de Python
- Dockerfile: archivo Dockerfile para instalar todas las dependencias (librerías de Python) que se necesitan para ejecutar el Pipeline.
- requirements.txt: archivo txt indicando las versiones de cada una de las librerías que necesitamos instalar para que se ejecute satisfactoriamente el proceso en cualquier otra máquina, computadora o dispositivo.
- docker-compose.yaml: El archivo .yaml orquesta distintos contenedores para que se comuniquen entre ellos.Se usa para definir la configuracion de un entorno de contenedor o para orquestar multiples contenedores.
		       El archivo .yaml en este caso orquesta estas 2 imágenes, por eso al correr el yaml en el command prompt tenemos estas 2 imágenes en Docker:
				- Imagen "postgres":Crea las condiciones necesarias para instalar Postgre SQL.
				- Imagen "apache/airflow": Crea las condiciones para instalar Airflow.
				- Imagen "airflow_docker-my-custom-service": Crea las condiciones necesarias para instalar las librerías y dependencias que necesitamos para nuestro proceso y que se indican en los files "Dockerfile" y "requirements.txt"
- Entregable 3 - Christian Javier Rivas Nieto - Extract data from Youtube API.ipynb: Este archivo ipynb es un archivo Jupyter Notebooks en el que fui probando el paso a paso del Script de Python que se encuentra en el file "Youtube_ETL.py".
										     Una vez que comprobé que el proceso funcionaba bien en el archivo Jupyter Notebooks, llevé ese Script al archivo "Youtube_ETL.py" en Visual Studio Code para llevarlo al formato .py (conviene utilizar este formato de archivo para realizar y automatizar Pipelines).
- Carpeta "Imagenes de evidencia de proceso corrido en Airflow satisfactoriamente": Contiene imagenes (archivos .png) de evidencia mostrando que el proceso funciona correctamente en Airflow, que la data se inserta satisfactoriamente a la tabla de Redshift y que se envía el email correctamente indicando que el proceso fue completado con exito.
- Consigna Entregable 3.pdf: Archivo PDF indicando la consigna del TP que se realiza con los archivos de este repositorio.
- Carpeta "airflow_docker": Carpeta que tengo en mi Desktop incluyendo los archivos anteriormente mencionados, y con la estructura para que sea ejecutado el proceso en mi computadora directamente con los siguientes Comandos en el "Command Prompt":
			    Comandos a ejecutar en el Command Prompt:
				C:\Users\cnieto1>cd Desktop

				C:\Users\cnieto1\Desktop>cd airflow_docker

				C:\Users\cnieto1\Desktop\airflow_docker>docker-compose up


