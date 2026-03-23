import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


st.checkbox('Mostrar gráficas')

#definimos las columnas que me interesan 
fields = ["AAAA","N_VICTIMAS"]



# Cargar el archivo CSV en un DataFrame de Pandas
df_homicidios = pd.read_csv('Data\homicidios.csv')

# Mostrar el DataFrame utilizando st.dataframe()
st.dataframe(df_homicidios)

# Crear un checkbox para mostrar u ocultar los datos
show_data = st.checkbox('Mostrar datos de homicidios')

# Mostrar los datos si el checkbox está marcado como True
if show_data:
    st.dataframe(df_homicidios)

    # Crear un botón para mostrar el head() del DataFrame al presionarlo
if st.button('Mostrar Head'):
    # Mostrar el head() del DataFrame cuando se presione el botón
    st.dataframe(df_homicidios.head())

    # Crear un botón para mostrar el tail() del DataFrame al presionarlo
if st.button('Mostrar Tail'):
    # Mostrar el tail() del DataFrame cuando se presione el botón
    st.dataframe(df_homicidios.tail())

dim =st.radio('Dimension a mostrar:',('Filas','Columnas'), horizontal =True)

if dim == 'Filas':
    st.write('Cantidad de Filas:', df_homicidios.shape[0])


# Realizar la agrupación y sumar las víctimas por año
resumen = df_homicidios.groupby("AAAA")["N_VICTIMAS"].sum().reset_index()

# Mostrar el resultado en Streamlit
st.write('Resumen de víctimas por año:')
st.write(resumen)


# Cargar el archivo CSV en un DataFrame de Pandas
data = pd.read_csv('Data/VICTIMAS.csv')

# Mostrar el DataFrame utilizando st.dataframe()
st.write('Mostrar datos de Víctimas:')
st.dataframe(data)  # Mostrar el DataFrame completo

# Crear un checkbox para mostrar u ocultar los datos
show_data = st.checkbox('Mostrar datos de Víctimas')

# Mostrar los datos si el checkbox está marcado como True
if show_data:
    st.write('Datos de Víctimas:')
    st.dataframe(data)  # Mostrar el DataFrame completo

# Crear un botón para mostrar el head() del DataFrame al presionarlo
if st.button('Mostrar Head', key='head_button'):
    # Mostrar el head() del DataFrame cuando se presione el botón
    st.write('Head del DataFrame:')
    st.dataframe(data.head())

# Crear un botón para mostrar el tail() del DataFrame al presionarlo
if st.button('Mostrar Tail', key='tail_button'):
    # Mostrar el tail() del DataFrame cuando se presione el botón
    st.write('Tail del DataFrame:')
    st.dataframe(data.tail())

dim = st.radio('Dimensión a mostrar:', ('Filas', 'Columnas'), index=0)

if dim == 'Filas':
    st.write('Cantidad de Filas:', data.shape[0])

    # Cargar el archivo CSV en un DataFrame de Pandas
comuna = pd.read_csv('Data\informacion_comunas.csv')

# Mostrar el DataFrame utilizando st.dataframe()
st.write('Mostrar datos de las Comunas:')
st.dataframe(comuna)  # Mostrar el DataFrame completo

# Crear un checkbox para mostrar u ocultar los datos
show_data = st.checkbox('Mostrar de la población')

# Mostrar los datos si el checkbox está marcado como True
if show_data:
    st.write('Datos extensión territorial:')
    st.dataframe(comuna)  # Mostrar el DataFrame completo

# Crear un botón para mostrar el head() del DataFrame al presionarlo
if st.button('Mostrar Head 1', key='head_button_1'):
    # Acciones al presionar el botón 'Mostrar Head 1'
    pass
    # Mostrar el head() del DataFrame cuando se presione el botón
    st.write('Head del DataFrame:')
    st.dataframe(comuna.head())

if st.button('Mostrar Head 2', key='head_button_2'):
    # Acciones al presionar el botón 'Mostrar Head 2'
    pass
    # Mostrar el tail() del DataFrame cuando se presione el botón
    st.write('Tail del DataFrame:')
    st.dataframe(comuna.tail())





