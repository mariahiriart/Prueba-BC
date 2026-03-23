import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import folium
from streamlit_folium import folium_static

# Cargar el archivo CSV en un DataFrame de Pandas
df_homicidios = pd.read_csv('Data\homicidios.csv')


# Creación de la tabla pivot para el gráfico
tabla_acusados = df_homicidios.pivot_table(index="ACUSADO", columns="VICTIMA", aggfunc="size", fill_value=0)

# Configuración del gráfico con estilo más atractivo
plt.figure(figsize=(12, 8))

# Tipo de gráfico
tabla_acusados.plot(kind="bar", stacked=True, colormap='viridis')

# Configuración del título y etiquetas
plt.title("Distribución de Víctimas por Tipo de Acusado")
plt.xlabel("Tipo de Acusado")
plt.ylabel("Número de Víctimas")

# Personalización adicional
plt.legend(title='Tipo de Víctima', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.xticks(rotation=45)
plt.tight_layout()

# Mostrar el gráfico en Streamlit
st.pyplot(plt)

# Creación de la tabla pivot para el gráfico
pivot_table_3 = df_homicidios.pivot_table(index="COMUNA", columns="TIPO_DE_CALLE", aggfunc="size", fill_value=0)

# Configuración del gráfico con estilo más atractivo
plt.figure(figsize=(12, 8))

# Tipo de gráfico
pivot_table_3.plot(kind="barh", stacked=True, colormap='magma')

# Configuración del título y etiquetas
plt.title("Distribución de Tipo de Calle donde ocurren más muertes por Comuna")
plt.xlabel("Número de Ocurrencias")
plt.ylabel("Comuna")

# Personalización adicional
plt.legend(title='Tipo de Calle', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()

# Mostrar el gráfico en Streamlit
st.pyplot(plt)