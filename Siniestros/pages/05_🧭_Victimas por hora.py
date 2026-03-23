import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import folium
from streamlit_folium import folium_static



# Cargar el archivo CSV en un DataFrame de Pandas
df_homicidios = pd.read_csv('Data\homicidios.csv')

# Agrupar por hora y calcular el número total de víctimas por hora
suma_victimas_por_hora = df_homicidios.groupby("HH")["N_VICTIMAS"].sum().reset_index()

# Configuración del estilo utilizando seaborn
plt.figure(figsize=(12, 6))
sns.set_style("whitegrid")  # Estilo de la cuadrícula blanca
sns.barplot(data=suma_victimas_por_hora, x="HH", y="N_VICTIMAS", palette="viridis")
plt.title("Víctimas por Hora del día")
plt.xlabel("Hora del Accidente")
plt.ylabel("Cantidad de Víctimas")
plt.xticks(rotation=45)
plt.ylim(0, 50)  # Establecer límites en el eje y para una mejor visualización

# Mostrar el gráfico en Streamlit
st.pyplot(plt)

# Se genera un dataframe para el filtro
agrupado_por_anio_mes = df_homicidios.groupby(["AAAA", "MM"])["N_VICTIMAS"].sum().reset_index()

# Se inicializa la figura y el eje
fig, ax = plt.subplots(figsize=(12, 8))

# Iteración a través de cada año para encontrar el mes con más siniestros
for year in agrupado_por_anio_mes["AAAA"].unique():
    df_year = agrupado_por_anio_mes[agrupado_por_anio_mes["AAAA"] == year]
    max_month = df_year.loc[df_year["N_VICTIMAS"].idxmax()]["MM"]
    max_victims = df_year["N_VICTIMAS"].max()
    ax.bar(str(year), max_victims, label=f"Año {year}: Mes {max_month}", color='skyblue')

# Configuración de etiquetas y título
ax.set_xlabel("Año")
ax.set_ylabel("Número de Víctimas")
ax.set_title("Mes con Más Siniestros por Año")
ax.legend()

# Personalización de la apariencia
ax.grid(axis='y', linestyle='--', alpha=0.7)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
plt.xticks(rotation=45)

# Mostrar el gráfico en Streamlit
st.pyplot(fig)