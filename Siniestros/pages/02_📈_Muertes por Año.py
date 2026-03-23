import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns

# Definir una paleta de colores personalizada
colores = ["#1f77b4", "#2ca02c", "#ff7f0e", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]

# Usar la paleta personalizada en Seaborn
sns.set_palette(colores)


# Cargar el archivo CSV en un DataFrame de Pandas
df_homicidios = pd.read_csv('Data\homicidios.csv')

# Realizar la agrupación y sumar las víctimas por año
suma_victimas_por_año = df_homicidios.groupby("AAAA")["N_VICTIMAS"].sum().reset_index()

# Crear la gráfica interactiva con Streamlit
st.title("Gráfico interactivo de Víctimas por Año")
st.write("Cantidad de víctimas por año")
# Usar la paleta de colores "Blues" de Seaborn
colores = sns.color_palette("pastel")
sns.set_palette(colores)
# Agregar un checkbox para seleccionar el estilo de la gráfica
if st.checkbox('Mostrar gráfico de barras'):
    # Mostrar la gráfica de barras utilizando Seaborn y Matplotlib
    plt.figure(figsize=(12, 6))
    sns.barplot(data=suma_victimas_por_año, x="AAAA", y="N_VICTIMAS")
    plt.title("Víctimas por Año")
    plt.xlabel("Año")
    plt.ylabel("Cantidad de Víctimas")
    plt.xticks(rotation=45)
    plt.ylim(0, 160)
    st.pyplot(plt)
elif st.checkbox('Mostrar gráfico de línea'):
    # Mostrar la gráfica de línea utilizando Seaborn y Matplotlib
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=suma_victimas_por_año, x="AAAA", y="N_VICTIMAS", marker='o')
    plt.title("Víctimas por Año")
    plt.xlabel("Año")
    plt.ylabel("Cantidad de Víctimas")
    plt.xticks(rotation=45)
    plt.ylim(0, 160)
    st.pyplot(plt)


