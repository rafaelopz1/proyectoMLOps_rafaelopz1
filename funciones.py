import pandas as pd

# # # # # # # # #  funcion 1: developer # # # # # # # # #

def developer(dev_nombre : str):
    '''
    Devuelve información sobre actividad en el mercado de un desarrollador de videojuegos.
         
    Parámetros:
        desarrollador (str): El nombre del desarrollador de videojuegos.
    
    Retorna:
        dict: Un diccionario con las siguientes claves:
            - 'Año' (dict): Año de lanzamiento solicitado.
            - 'Cantidad de Items' (dict): Cantidad de juegos lanzados ese año según desarrollador.
            - 'Contenido Free' (dict): Porcentaje de juegos gratuitos ese año según desarrollador.
    '''
    df_developer = pd.read_parquet('./data/steam_games_developer.parquet')

    if dev_nombre not in df_developer['developer'].unique():
        return {'error': 'El Desarrollador especificado no existe.'}

    developer = df_developer[df_developer['developer'] == dev_nombre]

    cantidad_item = developer.groupby('año_lanzamiento')['item_id'].count()
    total_gratis = developer[developer['price'] == 0].groupby('año_lanzamiento')['price'].count()
    porc_gratis_año = round((total_gratis / cantidad_item) * 100, 2)

    # Se asegura que las Series tengan nombres para la operación de merge.
    if cantidad_item.name is None:
        cantidad_item.name = 'Cantidad de Items'
    if porc_gratis_año.name is None:
        porc_gratis_año.name = 'Contenido Free'
    cantidad_item.name = 'Cantidad de Items'

    tabla = pd.merge(cantidad_item, porc_gratis_año, on='año_lanzamiento').reset_index()
    tabla = tabla.fillna(0)
    tabla['Contenido Free'] = tabla['Contenido Free'].apply(lambda x: f'{x}%')

    return tabla.to_dict(orient='records')


# # # # # # # # # funcion 2: userdata # # # # # # # # #

def userdata(user_id):
    '''
    Obtiene información sobre un usuario a partir de su 'user_id'.
         
    Parámetros:
        user_id (str): Identificador único del usuario.
    
    Retorna:
        dict: Un diccionario que contiene información sobre el usuario.
            - 'Usuario' (str): Identificador único del usuario.
            - 'Dinero gastado' (float): Cantidad de dinero gastado por el usuario.
            - 'porcentaje_recomendacion' (float): Porcentaje de recomendaciones realizadas por el usuario.
            - 'Cantidad de items' (int): Cantidad de items que tiene el usuario.
    '''
    df_userdata = pd.read_parquet('./data/userdata.parquet')

    if user_id not in df_userdata['user_id'].unique():
        return {'error': 'El usuario especificado no existe.'}

    user_data = df_userdata[df_userdata['user_id'] == user_id]
    dinero_gastado = user_data['price'].sum()
    porcentaje_recomendacion = (user_data['recommend'].sum() / len(user_data)) * 100
    cantidad_de_items = user_data['item_id'].nunique()

    return {
        'Usuario': user_id,
        'Dinero gastado': dinero_gastado,
        'Porcentaje de recomendación': porcentaje_recomendacion,
        'Cantidad de items': cantidad_de_items
    }


# # # # # # # # # funcion 3: UserForGenre # # # # # # # # #

def UserForGenre(genero: str):
    '''
    Devuelve información sobre actividad en el mercado de un desarrollador de videojuegos.
         
    Parámetros:
        desarrollador (str): El nombre del desarrollador de videojuegos.
    
    Retorna:
        dict: Un diccionario con las siguientes claves:
            - 'Año' (dict): Año de lanzamiento solicitado.
            - 'Cantidad de Items' (dict): Cantidad de juegos lanzados ese año según desarrollador.
            - 'Contenido Free' (dict): Porcentaje de juegos gratuitos ese año según desarrollador.
    '''
    df_games_genres = pd.read_parquet('./data/df_games_genres.parquet')
    df_users_horas = pd.read_parquet('./data/df_users_horas.parquet')

    df_games_genres = df_games_genres.sample(frac=0.1, random_state=42)
    df_users_horas = df_users_horas.sample(frac=0.1, random_state=42)

    # Une ambos dataframes
    df_genres_horas = df_games_genres.merge(df_users_horas, on='item_id', how='right')

    # Filtra el DataFrame resultante para obtener solo las filas relacionadas con el género dado
    df_filtered = df_genres_horas[df_genres_horas['genres'] == genero]

    if df_filtered.empty:
        return {"message": "No data found for the given genre"}

    # Encontrar el usuario que acumula más horas jugadas para el género dado
    max_user = df_filtered.groupby('user_id')['playtime_forever'].sum().idxmax()

    # Filtrar el DataFrame para obtener solo las filas relacionadas con el usuario que acumula más horas
    df_user_max_hours = df_filtered[df_filtered['user_id'] == max_user]

    # Agrupar por año y sumar las horas jugadas
    horas_por_anio = df_user_max_hours.groupby('año_lanzamiento')['playtime_forever'].sum()

    # Construir el diccionario de resultados
    result_dict = {
        f"Usuario con más horas jugadas para Género {genero}": max_user,
        "Horas jugadas": [{"Año": int(year), "Horas": int(hours)} for year, hours in horas_por_anio.reset_index().to_dict(orient='split')['data']]
    }

    return result_dict


# # # # # # # # # funcion 4: best_developer_year # # # # # # # # #

def best_developer_year(año: int):
    '''
    Obtiene el top 3 de desarrolladores con juegos más recomendados por usuarios para un año específico.

    Parámetros:
        año (int): El año del que se quieren obtener los mejores desarrolladores.
    
    Retorna:
        dict: Un diccionario con el top 3 de desarrolladores:
            - 'Puesto 1' (str): El nombre del desarrollador en el primer puesto.
            - 'Puesto 2' (str): El nombre del desarrollador en el segundo puesto.
            - 'Puesto 3' (str): El nombre del desarrollador en el tercer puesto.
    '''
    merged_df = pd.read_parquet('./data/best_developer_year.parquet')
    
    # Validar el año
    if año not in merged_df['año'].unique():
        return {'error': 'El año especificado no es válido.'}

    # Filtrar los juegos por año y por recomendación positiva
    df_year = merged_df[(merged_df['año'] == año) & (merged_df['recommend'] == True) & (merged_df['sentiment_analysis'] == 2)]

    # Contar el número de juegos recomendados por desarrollador y devolver los tres primeros desarrolladores
    top_desarrolladores = df_year['developer'].value_counts().index.tolist()[:3]

     # Devolver el top 3 de desarrolladores
    return {'Puesto 1' : top_desarrolladores[0], 'Puesto 2' : top_desarrolladores[1], 'Puesto 3' : top_desarrolladores[2]}

# # # # # # # # # funcion 5: developer_reviews_analysis # # # # # # # # #

def developer_reviews_analysis(desarrolladora: str):
    '''
    Esta funcion retorna un diccionario con el análisis de las reseñas de los juegos de un desarrollador específico.

    Parámetros:

        desarrolladora (str): El nombre del desarrollador del que se quieren obtener las reseñas.

    Retorno:
        dict: Un diccionario con el análisis de las reseñas:
        - "Nombre del desarrollador" (str): El nombre del desarrollador.
        - "Resumen de reseñas" (list): Una lista con la cantidad de reseñas positivas y negativas.
    '''
    df_dev = pd.read_parquet('./data/developer_reviews_analysis.parquet')
    # Validar el desarrollador
    if desarrolladora not in df_dev['developer'].unique():
        return {'error': 'El Desarrollador especificado no existe.'}

    # Filtrar por el desarrollador
    df_merged = df_dev[df_dev['developer'] == desarrolladora]

    # Contar las reseñas positivas y negativas
    reviews_positivas = df_merged[df_merged['sentiment_analysis'] == 2].shape[0]
    reviews_negativas = df_merged[df_merged['sentiment_analysis'] == 0].shape[0]

    # Resumen de las reseñas
    resumen_reviews = f'[Negative = {reviews_negativas}, Positive = {reviews_positivas}]'

    # Diccionario con los resultados
    resultado = {desarrolladora: resumen_reviews}

    # Retornar el diccionario
    return resultado


# # # # # # # # # funcion 6: recomendacion_juego # # # # # # # # #

def recomendacion_juego(item_id):
    ''' 
    Esta función se encarga de obtener una lista de juegos recomendados para un usuario a partir del ID de un juego que le gusta.
    La función utiliza un modelo de recomendación pre-entrenado basado en la similitud entre juegos.

    Parámetros:
        item_id (int): El ID del juego que el usuario ha indicado que le gusta.
    Retorna:
        Una lista con 5 juegos recomendados similares al ingresado.
    '''
    df_modelo = pd.read_parquet('./data/modelo_recomendacion.parquet')
    # Filtrar el DataFrame por el año especificado
    df_filtro = df_modelo[df_modelo['item_id'] == item_id]
    
    resultado = df_filtro['Recomendaciones']
 
    return resultado