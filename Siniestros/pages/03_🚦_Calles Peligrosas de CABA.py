import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import folium
from streamlit_folium import folium_static

# Cargar el archivo CSV en un DataFrame de Pandas
df_homicidios = pd.read_csv('Data\homicidios.csv')


# Definir los datos
data = {
    'Lugar': ['PAZ, GRAL. AV.', 'RIVADAVIA AV.', 'DEL LIBERTADOR AV.', 'AUTOPISTA 1 SUR PRESIDENTE ARTURO FRONDIZI', 'ALBERDI, JUAN BAUTISTA AV.'],
    'Cantidad': [57, 19, 18, 14, 13],
    'Latitud': [-34.6295, -34.6051, -34.5626, -34.6406, -34.6484],
    'Longitud': [-58.5034, -58.4459, -58.4491, -58.3773, -58.3987]
}
df = pd.DataFrame(data)

# Crear un mapa centrado en Buenos Aires
mapa = folium.Map(location=[-34.6037, -58.3816], zoom_start=12)

# Añadir los puntos al mapa
for i, row in df.iterrows():
    folium.Marker(
        location=[row['Latitud'], row['Longitud']],
        popup=f"{row['Lugar']} - Cantidad: {row['Cantidad']}",
        icon=folium.Icon(color='red', icon='info-sign')
    ).add_to(mapa)

# Mostrar el mapa interactivo en Streamlit
st.write("Mapa de ubicaciones")
folium_static(mapa)

calles = ['PAZ, GRAL. AV.', 'RIVADAVIA AV.', 'DEL LIBERTADOR AV.', 'AUTOPISTA 1 SUR PRESIDENTE ARTURO FRONDIZI', 'ALBERDI, JUAN BAUTISTA AV.']
cantidad = [57, 19, 18, 14, 13]

# Crear gráfico de barras con Matplotlib
fig, ax = plt.subplots(figsize=(10, 6))
bars = ax.barh(calles, cantidad, color='blue')
ax.set_xlabel('Cantidad de Incidentes')
ax.set_title('Calles más peligrosas en CABA')
ax.invert_yaxis()  # Invierte el eje y para mostrar la calle más peligrosa arriba

# Mostrar la gráfica utilizando Streamlit
st.pyplot(fig)

# Botón para ocultar/mostrar los datos
if st.button('Mostrar/Ocultar Datos'):
    st.write(pd.DataFrame({'Calle': calles, 'Cantidad de Incidentes': cantidad}))
else:
    st.write('Presiona el botón para ver los datos')