![](Screenshot_1.png)


# **Proyecto individual MLOps**

El objetivo principal de este proyecto es crear un sistema de recomendación de videojuegos para Steam, una plataforma masiva de distribución digital de videojuegos que cuenta con 120 millones de usuarios activos mensuales y más de 50.000 juegos en su catálogo. Es importante tener en cuenta que en este proyecto trabajaremos con los datos publicados por SteamSpy, los cuales son hasta el año 2017.

## **Descripción**

Para este proyecto, se solicita un Producto Mínimo Viable a través de un enfoque que involucra tareas de Data Engineering (ETL, EDA, API) hasta la implementación un modelo de recomendación item-item y que muestre una API deployada en un servicio en la nube. 

Hay un obstáculo importante: la madurez y calidad de los datos disponibles es poca (ok, es nula 😭 ). Los datos están anidados, en formato crudo y no hay procesos automatizados para la actualización de nuevos productos.

Sin duda alguna, este proyecto desafía a adquirir habilidades y conocimientos esenciales para abordar situaciones del mundo real en el campo de MLOps. Desde la preparación y análisis de datos hasta la implementación de un modelo predictivo para recomendaciones de juegos y sobretodo prepararnos para proporcionar un acceso eficaz a información valiosa.

## **Etapas y tareas desarrolladas**

1. **Limpieza y Transformación de Datos:** Para este proyecto se proporcionaron tres archivos en formato JSON, los cuales estan disponibles en esta carpeta de [Google Drive](https://drive.google.com/drive/folders/1HqBG2-sUkz_R3h1dZU5F2uAzpRn7BSpj). También contiene el [Diccionario](https://docs.google.com/spreadsheets/d/1-t9HLzLHIGXvliq56UE_gMaWBVTPfrlTf2D9uAtLGrk/edit#gid=0) de datos con algunas de las columnas disponibles en el dataset.

    - Se exploraron los datos disponibles para comprender su estructura y calidad. 
    
    - Se identificaron problemas y desafíos relacionados con la calidad de los datos. Se realizaron tareas de limpieza de datos exhaustivas para abordar problemas como datos anidados, valores nulos y otros problemas de calidad de datos. Sin embargo, uno de los desafíos clave fue la identificación y gestión de valores críticos que eran fundamentales para el proyecto.

    - Identificación de Valores Críticos: Se llevaron a cabo análisis detallados para identificar valores clave que eran críticos para el proyecto. Estos valores eran esenciales para la precisión del sistema de recomendación y su eliminación habría resultado en una pérdida significativa de información relevante.

    - Criterios de Conservación: Se establecieron criterios sólidos para determinar cuándo era apropiado conservar y transformar valores críticos en lugar de eliminarlos. Estos criterios se basaron en la importancia y relevancia de los datos para el objetivo del proyecto.

Esta etapa fue esencial para garantizar que los datos fueran adecuados para su procesamiento posterior y que se mantuviera la integridad de los valores críticos, preservando así información valiosa y relevante para el sistema de recomendación.
Puede revisar todo este proceso a detalle en el notebook [ETL](ETL.ipynb).

2. **Creación de funciones:** Se crearon [funciones](funciones.py) especificas para construir una API utilizando FastAPI. Esta API facilitará la interacción eficiente con nuestros datos y se alojará en el servidor web de Render para simplificar el acceso y la consulta de información valiosa.

3. **Feature engineering:** se creó una nueva columna llamada 'sentiment_analysis' que reemplaza a la columna que contiene los reviews donde clasifica los sentimientos de los comentarios con la siguiente escala: '0' si es malo, '1' si es neutral y '2' si es positivo.

4. **Análisis Exploratorio de Datos:** Utilizamos el Análisis Exploratorio de Datos [(EDA)](EDA.ipynb) como nuestra herramienta clave para comprender las relaciones entre variables y detectar posibles patrones e irregularidades.

5. **Desarrollo del Modelo de Recomendación:** Utilizando similitud del coseno y CountVectorizer, desarrollé un [modelo](modelo-recomendacion.ipynb) que recomienda juegos similares en base a un juego dado. 

    - Vectorización de descripciones: Con CountVectorizer, las descripciones de los juegos (columna 'specs') se convierten en vectores numéricos.
    
    - Similitud del coseno: Se calcula la similitud entre los vectores de dos juegos. Cuanto más cercano a 1, más similares son.
    
    - Recomendaciones: Se presentan juegos con vectores similares al juego dado como recomendaciones potenciales.

## **Desarrollo de API**

Proponemos disponibilizar los datos de la empresa utilizando el framework FastAPI. Creamos las siguientes funciones para los endpoints que se consumirán en la API:

- `def developer( desarrollador : str )`: Cantidad de items y porcentaje de contenido Free por año según empresa desarrolladora.
- `def userdata( User_id : str )`: Debe devolver cantidad de dinero gastado por el usuario, el porcentaje de recomendación en base a reviews.recommend y cantidad de items.
- `def UserForGenre( genero : str )`: Debe devolver el usuario que acumula más horas jugadas para el género dado y una lista de la acumulación de horas jugadas por año de lanzamiento.
- `def best_developer_year( año : int )`: Devuelve el top 3 de desarrolladores con juegos MÁS recomendados por usuarios para el año dado.
- `def developer_reviews_analysis( desarrolladora : str )`: Según el desarrollador, se devuelve un diccionario con el nombre del desarrollador como llave y una lista con la cantidad total de registros de reseñas de usuarios que se encuentren categorizados con un análisis de sentimiento como valor positivo o negativo.
- `def recomendacion_juego(item_id)`: Esta función se encarga de obtener una lista de juegos recomendados para un usuario a partir del ID de un juego que le gusta.
    La función utiliza un modelo de recomendación pre-entrenado basado en la similitud entre juegos.

## **Deployment**

Se desplegó la API con Render para poder ser consumida desde la web.

Puedes acceder al servicio en el siguiente link: https://proyectomlops-rafaelopz1.onrender.com/docs

## Video

En este [video](https://youtu.be/vjse1SPxShs)se explica brevemente este proyecto mostrando el funcionamiento de la API.

## Autor
Rafael Oropeza
[Linkedin](https://www.linkedin.com/in/rafael-oropeza-594853151/)
rafael415oropeza@gmail.com