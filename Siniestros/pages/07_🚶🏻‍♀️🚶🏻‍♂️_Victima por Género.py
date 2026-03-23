import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import folium
from streamlit_folium import folium_static
import numpy as np

# Cargar el archivo CSV en un DataFrame
data = pd.read_csv('Data/VICTIMAS.csv')

# Crear el DataFrame con los datos
data = {
    "Víctima": ["AUTO - CONDUCTOR", "PASAJERO_ACOMPAÑANTE", "BICICLETA - CICLISTA",
                "CARGAS - CONDUCTOR", "PASAJERO_ACOMPAÑANTE", "MOTO - CONDUCTOR",
                "PASAJERO_ACOMPAÑANTE", "SD", "MOVIL - CONDUCTOR", "PASAJERO_ACOMPAÑANTE",
                "PASAJEROS - PASAJERO_ACOMPAÑANTE", "PEATON - PEATON"],
    "ROL": ["CONDUCTOR", "CONDUCTOR", "CICLISTA", "CONDUCTOR", "PASAJERO_ACOMPAÑANTE",
            "CONDUCTOR", "PASAJERO_ACOMPAÑANTE", "1", "CONDUCTOR", "PASAJERO_ACOMPAÑANTE",
            "PASAJERO_ACOMPAÑANTE", "PEATON"],
    "2019": [5, 10, 8, 0, 0, 15, 20, 1, 0, 0, 3, 103],
    "2020": [60, 19, 21, 3, 4, 244, 19, 1, 2, 0, 2, 163],
    "2021": [0, 0, 0, 0, 0, 2, 0, 1, 0, 0, 0, 1]
}

df = pd.DataFrame(data)

# Configurar el índice para tener las columnas 'Víctima' y 'ROL'
df.set_index(['Víctima', 'ROL'], inplace=True)

# Mostrar los datos en Streamlit
st.write(df)

# Crear el gráfico de barras apiladas
fig, ax = plt.subplots(figsize=(12, 8))

# Transponer los datos para representarlos correctamente en el gráfico
df.T.plot(kind='bar', stacked=True, ax=ax)

# Configurar etiquetas y título
plt.xlabel("Año")
plt.ylabel("Cantidad de Víctimas")
plt.title("Relación entre Víctima, Rol y Año")

# Mostrar el gráfico en Streamlit
st.pyplot(fig)