import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import folium
from streamlit_folium import folium_static
import numpy as np

# Cargar el archivo CSV en un DataFrame
data = pd.read_csv('Data/VICTIMAS.csv')

# Se filtran los valores de edad quitando los que no tienen datos
df_edad = data[data["EDAD"] != "SD"]

# Se convierten los valores de la columna edad a numéricos por si hay algún error
df_edad["EDAD"] = pd.to_numeric(df_edad["EDAD"], errors="coerce")

# Configuración de la página de Streamlit
st.title("Histograma de Edades de las Víctimas")

# Crear un histograma para visualizar la distribución de edades al fallecimiento
fig, ax = plt.subplots(figsize=(10, 6))
ax.hist(df_edad["EDAD"], bins=40, color="blue", alpha=0.7)
ax.set_xlabel("Edad de las Víctimas")
ax.set_ylabel("Frecuencia")
ax.set_title("Histograma de Edades de las Víctimas")
ax.grid(True)

# Mostrar el gráfico en Streamlit
st.pyplot(fig)

