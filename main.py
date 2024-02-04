# Importaciones necesarias
from fastapi import FastAPI, HTTPException
import funciones

# Se instancia la aplicación
app = FastAPI()

@app.get('/')
async def root():
    return {'message': 'Proyecto individual MLOps - Rafael Oropeza. github @rafaelopz1'}

@app.get('/developer/{desarrollador}')
async def desarrollador(desarrollador: str):
    try:
        resultado = funciones.developer(desarrollador)
        return resultado
    
    except Exception as e:
        import traceback
        print(traceback.format_exc())  # Imprime el traceback en la consola para ver más detalles
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")  

    
@app.get('/userdata/{user_id}')
async def user(user_id: str):
    try:
        result = funciones.userdata(user_id)
        return result
    
    except Exception as e:
        import traceback
        print(traceback.format_exc())  # Imprime el traceback en la consola para ver más detalles
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")


@app.get('/userforgenre/{genero}')
async def genre(genero: str):
    try:
        resultado = funciones.UserForGenre(genero)
        return resultado
    
    except Exception as e:
        import traceback
        print(traceback.format_exc())  # Imprime el traceback en la consola para ver más detalles
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")   
    

@app.get('/best_developer_year/{año}')
async def Best_developer_year(year: str):
    try:
        year_int = int(year)  # Convertir el año a un entero
        result2 = funciones.best_developer_year(year_int)
        return result2
    
    except Exception as e:
        import traceback
        print(traceback.format_exc())  # Imprime el traceback en la consola para ver más detalles
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")                                     

@app.get('/developer_reviews_analysis/{desarrolladora}') 
async def get_developer(desarrolladora: str):
    try:
        resultado= funciones.developer_reviews_analysis(desarrolladora)
        return resultado
    
    except Exception as e:
        import traceback
        print(traceback.format_exc())  # Imprime el traceback en la consola para ver más detalles
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")


@app.get('/recomendacion_juego/{item_id}', tags=['sistema de recomendación item-item'])
def item(item_id: int):
    try:
        item_id = int(item_id) 
        resultado= funciones.recomendacion_juego(item_id)
        return {'Juegos recomendados': resultado}
    
    except Exception as e:
        import traceback
        print(traceback.format_exc())  # Imprime el traceback en la consola para ver más detalles
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")