import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import folium
from streamlit_folium import folium_static

# Cargar el archivo CSV en un DataFrame de Pandas
df_homicidios = pd.read_csv('Data\homicidios.csv')
# Filtrar los datos por la calle "PAZ, GRAL. AV."
df_Gen_Paz = df_homicidios[df_homicidios["Calle"] == "PAZ, GRAL. AV."]

# Contar los siniestros por comuna
siniestros_por_comuna = df_Gen_Paz['COMUNA'].value_counts()

# Mostrar el gráfico en Streamlit
st.bar_chart(siniestros_por_comuna)

# Crear gráfico en Streamlit
st.pyplot(plt.figure(figsize=(10, 6)))
siniestros_por_comuna.plot(kind='bar', color='skyblue')
plt.title('Número de siniestros en la calle General Paz por comuna')
plt.xlabel('Comuna')
plt.ylabel('Cantidad de siniestros')
plt.xticks(rotation=45)
plt.grid(axis='y')
plt.tight_layout()


# DataFrame con los datos del número de víctimas por comuna
data = {
    'COMUNA': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    'N_VICTIMAS': [2, 93, 25, 46, 79, 22, 22, 62, 67, 75, 30, 33, 39, 40, 37, 45]
}
df_homicidios = pd.DataFrame(data)

# Mostrar el DataFrame en Streamlit
st.write(df_homicidios)

# Crear la gráfica de barras con Seaborn y Matplotlib
plt.figure(figsize=(12, 6))
sns.barplot(data=df_homicidios, x="COMUNA", y="N_VICTIMAS")
plt.title("Víctimas por Comuna")
plt.xlabel("Comuna")
plt.ylabel("Cantidad de Víctimas")
plt.xticks(rotation=45)

# Mostrar la gráfica en Streamlit
st.pyplot(plt)

datos = {
    'COMUNA': list(range(16)),  # Comunas del 0 al 15
    'N_VICTIMAS': [2, 93, 25, 46, 79, 22, 22, 62, 67, 75, 30, 33, 39, 40, 37, 45],  # Cantidad de víctimas por comuna
}

# Años del 2016 al 2021
años = list(range(2016, 2022))

# Crear un DataFrame con los datos proporcionados
df_homicidios = pd.DataFrame(datos)

# Inicializar la figura y el eje
fig, ax = plt.subplots(figsize=(10, 6))

# Graficar la cantidad de víctimas por comuna para cada año
for i, año in enumerate(años):
    ax.plot(df_homicidios['COMUNA'], [df_homicidios['N_VICTIMAS'][j] for j in range(len(df_homicidios))], label=str(año))

# Configuración del gráfico
ax.set_xlabel('Comuna')
ax.set_ylabel('Número de Víctimas')
ax.set_title('Cantidad de Víctimas por Comuna (2016-2021)')
ax.legend()
plt.xticks(df_homicidios['COMUNA'])
plt.grid(True)

# Mostrar el gráfico
plt.tight_layout()
plt.show()

# Datos de años y comunas
data1 = {
    'AAAA': [2016, 2016, 2017, 2017, 2018, 2018, 2019, 2019, 2020, 2020, 2021, 2021],
    'COMUNA': [1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2],
    'N_VICTIMAS': [65, 81, 69, 71, 70, 79, 57, 47, 31, 50, 55, 42]
}

# Crear DataFrame
df1 = pd.DataFrame(data1)

# Agrupar datos por año y comuna
agrupado_por_anio_comuna = df1.groupby(["AAAA", "COMUNA"])["N_VICTIMAS"].sum().reset_index()

# Inicializar la figura y el eje
fig, ax = plt.subplots(figsize=(12, 8))

# Diccionario de colores para cada comuna
colors = plt.cm.tab20b.colors[:len(agrupado_por_anio_comuna["AAAA"].unique())]

# Iterar a través de cada año para encontrar la comuna con más siniestros y graficarla
for i, year in enumerate(agrupado_por_anio_comuna["AAAA"].unique()):
    df_year_new = agrupado_por_anio_comuna[agrupado_por_anio_comuna["AAAA"] == year]
    max_comuna = df_year_new.loc[df_year_new["N_VICTIMAS"].idxmax()]
    ax.bar(str(year), max_comuna["N_VICTIMAS"], color=colors[i], label=f"Año {year}: Comuna {max_comuna['COMUNA']} - Víctimas: {max_comuna['N_VICTIMAS']}")

# Configurar etiquetas y título
ax.set_xlabel("Año")
ax.set_ylabel("Número de Víctimas")
ax.set_title("Comuna con Más Siniestros por Año")
ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')

# Mostrar el gráfico en Streamlit
st.pyplot(fig)