import pandas as pd

df_games = pd.read_parquet('./data/steam_games.parquet')
df_items = pd.read_parquet('./data/users_items.parquet')
df_reviews = pd.read_parquet('./data/users_reviews.parquet')
df_modelo = pd.read_parquet('./data/modelo_recomendacion.parquet')

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
    if dev_nombre not in df_games['developer'].unique():
        return {'error': 'El Desarrollador especificado no existe.'}
    #columnas necesarias
    df = df_games[['item_id', 'price','developer','año_lanzamiento']] 
    
    #llamo al desarrollador
    developer = df[df['developer'] == dev_nombre]
    
    #obtengo la cantidad por año 
    cantidad_item = developer.groupby('año_lanzamiento')['item_id'].count() 
    
    #juegos gratuitos del desarrollador
    juegos_gratis = developer[developer['price'] == 0] 
    
    #cantidad juegos gratis por año 
    total_gratis = juegos_gratis.groupby('año_lanzamiento')['price'].count() 

    #porcentaje gratis por año 
    porc_gratis_año = round((total_gratis/cantidad_item)*100,2) 

    #asigno nombre a las series
    cantidad_item.name = 'Cantidad de Items'

    porc_gratis_año.name = 'Contenido Free'
    #unimos las dos tablas para hacerla unica
    tabla = pd.merge(cantidad_item, porc_gratis_año,on='año_lanzamiento').reset_index() 

    #reemplazo los nan por 0
    tabla = tabla.fillna(0) 
    
    tabla['Contenido Free'] = tabla['Contenido Free'].apply(lambda x: f'{x}%')
    #convierto la tabla en diccionario
    resultado = tabla.to_dict(orient='records') 
    
    return resultado


# # # # # # # # # funcion 2: userdata # # # # # # # # #

#Unión de los DataFrames
merged_reviews_games = df_reviews.merge(df_games[['item_id', 'price']])
merged_reviews_games.drop(columns=['helpful','año','sentiment_analysis'], inplace=True)

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
    if user_id not in merged_reviews_games['user_id'].unique():
        return {'error': 'El usuario especificado no existe.'}
    # Filtrar los datos para el usuario especificado
    user_data = merged_reviews_games[merged_reviews_games['user_id'] == user_id]
    # Calcular la cantidad de dinero gastado por el usuario
    dinero_gastado = user_data['price'].sum()

    # Calcular el porcentaje de recomendación en base a reviews.recommend
    recomendacion = user_data['recommend'].sum()
    porcentaje_recomendacion = recomendacion / len(user_data) * 100

    # Calcular la cantidad de items
    cantidad_de_items = user_data['item_id'].nunique()

    # Crear un diccionario con los resultados
    resultado = {
        'Usuario': user_id,
        'Dinero gastado': dinero_gastado,
        'Porcentaje de recomendación': porcentaje_recomendacion,
        'Cantidad de items': cantidad_de_items
    }

    return resultado


# # # # # # # # # funcion 3: UserForGenre # # # # # # # # #

def UserForGenre( genero : str ):
    '''
    Esta función obtiene información del usuario que más horas ha dedicado a un género específico,
    junto con su historial de horas jugadas por año de lanzamiento.         
    
    Parámetros:
        genero (str): Género del videojuego.
    
    Retorna:
        dict: Un diccionario con la información del usuario y su historial:
            - 'Usuario' (str): ID del usuario.
            - 'Historial' (list): Una lista con el historial de horas jugadas por año de lanzamiento:
                'En el año X jugó Y horas': Una frase que describe la cantidad de horas jugadas por el usuario en un año específico.
    '''
    genero = genero.capitalize()

    # Hacemos un join de las tablas para tener las horas y el usuario agrupadas a los items
    df_join = df_games.merge(df_items, on='item_id', how='inner')

    # Filtro por genero solicitado
    df_join = df_join.loc[df_join['genres'].apply(lambda x: genero in x)]

    # Sumo por usuario (ya esta filtrado por el genero). Me crea un df
    suma_por_años = df_join.groupby(['user_id'])['playtime_forever'].sum().reset_index()
    
    # Guardo el nombre del usuario con más horas jugadas en una variable
    user_con_mas_horas = suma_por_años.max().iloc[0]

    # Creo un nuevo df en donde filtro por el usuario con más horas, para poder sacar sus horas por año
    df_join_user = df_join[df_join['user_id'] == user_con_mas_horas].drop(columns=['item_id','user_id','genres'], axis=1)

    # Ahora voy a sumar por año (ya esta filtrado por el usuario). Me crea un df
    suma_por_años = df_join_user.groupby('año_lanzamiento')['playtime_forever'].sum().reset_index()

    # Conveirto el DataFrame a un diccionario para poder iterarlo
    suma_por_años_dic = suma_por_años.to_dict(orient='records')

    # Crear una lista para poder darle una forma más clara a la respuesta
    año_y_horas = [(f" En el año {i['año_lanzamiento']} jugó {i['playtime_forever']} horas\n") for i in suma_por_años_dic]
    
    resultados = {
        'Usuario': user_con_mas_horas,
        'historial': año_y_horas}
    # Retorno la respuesta
    return resultados


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
    # Realizar la unión de los DataFrames
    merged_df = pd.merge(df_reviews, df_games, on='item_id')
    if año not in merged_df['año'].unique():
        return {'error': 'El año especificado no es válido.'}

    # Filtrar los juegos por año y por recomendación positiva
    df_year = merged_df[(merged_df['año'] == año) & (merged_df['recommend'] == True) & (merged_df['sentiment_analysis'] == 2)]

    # Contar el número de juegos recomendados por desarrollador y devolver los tres primeros desarrolladores
    top_desarrolladores = df_year['developer'].value_counts().head(3).index.tolist()

     # Devolver el top 3 de desarrolladores
    return {'Puesto 1' : top_desarrolladores[0], 'Puesto 2' : top_desarrolladores[1], 'Puesto 3' : top_desarrolladores[2]}


# # # # # # # # # funcion 5: developer_reviews_analysis # # # # # # # # #

merged = df_reviews.merge(df_games[['item_id', 'price','developer']], on='item_id')
def developer_reviews_analysis(desarrolladora:str):
    '''
    Esta funcion retorna un diccionario con el análisis de las reseñas de los juegos de un desarrollador específico.

    Parámetros:

        desarrolladora (str): El nombre del desarrollador del que se quieren obtener las reseñas.

    Retorno:
        dict: Un diccionario con el análisis de las reseñas:
        - "Nombre del desarrollador" (str): El nombre del desarrollador.
        - "Resumen de reseñas" (list): Una lista con la cantidad de reseñas positivas y negativas.
    '''
    if desarrolladora not in df_games['developer'].unique():
        return {'error': 'El Desarrollador especificado no existe.'}
    
    #filtrar las columnas a utilizar 
    df = merged[['user_id', 'item_id','developer','año','sentiment_analysis']] 
    #filtrar los datos por desarrolladora
    df_merged = df[df['developer'] == desarrolladora] 

    # Se obtienen la cantidad de reviews positivas y negativas
    reviews_positivas = df_merged[df_merged['sentiment_analysis'] == 2].shape[0] 
    reviews_negativas = df_merged[df_merged['sentiment_analysis'] == 0].shape[0]

    # Se crea un string con el resumen de las reviews
    resumen_reviews = f'[Negative = {reviews_negativas}, Positive = {reviews_positivas}]' 
    # Se crea un diccionario con los resultados obtenidos
    resultado = {desarrolladora : resumen_reviews} 

    # Se devuelve un diccionario con los resultados obtenidos
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
    # Filtrar el DataFrame por el año especificado
    df_filtro = df_modelo[df_modelo['item_id'] == item_id]
    
    resultado = df_filtro['Recomendaciones']
 
    return resultado